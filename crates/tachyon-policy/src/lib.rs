//! Policy: capability matching, project trust, approvals, containment.
//!
//! Spec §33–§35. The policy unit is an explicit capability/resource scope
//! (`fs.read:workspace/**`), never a command deny-list. Decisions are
//! [`PolicyDecision::Allow`], [`PolicyDecision::Deny`], or
//! [`PolicyDecision::Ask`]. Approvals bind to the BLAKE3 hash of the exact
//! operation: any material change invalidates them.
//!
//! Path containment ([`contain`]) resolves workspace-relative components,
//! rejects traversal, canonicalizes symlink components, and verifies the
//! target stays inside granted roots — never raw string-prefix comparison.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use tachyon_types::{ApprovalId, CapabilityId};
use thiserror::Error;

/// A policy evaluation outcome (spec §33).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    Deny { reason: String },
    Ask { request: ApprovalRequest },
}

/// An approval ask bound to an exact operation hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: ApprovalId,
    pub capability: CapabilityId,
    pub scope: String,
    /// BLAKE3 hash (hex) of the canonical operation JSON.
    pub operation_hash: String,
    /// Human-readable summary shown at approval time.
    pub summary: String,
}

/// A granted approval: the request plus the recorded decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Approval {
    pub request: ApprovalRequest,
    pub approved: bool,
}

/// Internal lookup key for a one-shot authorization grant. Operation data
/// alone is not a complete authorization identity: the capability and
/// policy scope must match too.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ApprovalKey {
    capability: String,
    scope: String,
    operation_hash: String,
}

fn approval_key(request: &ApprovalRequest) -> ApprovalKey {
    ApprovalKey {
        capability: request.capability.0.clone(),
        scope: request.scope.clone(),
        operation_hash: request.operation_hash.clone(),
    }
}

/// A capability scope pattern: `capability` plus a `/`-separated scope
/// glob where `**` matches any suffix and `*` matches within a segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub capability: CapabilityId,
    pub scope: String,
}

impl Grant {
    #[must_use]
    pub fn new(capability: &str, scope: &str) -> Self {
        Self {
            capability: CapabilityId(capability.to_owned()),
            scope: scope.to_owned(),
        }
    }

    /// Matches when both the capability and the scope pattern match.
    /// `scope` here is a resource path like `workspace/src/main.rs`.
    #[must_use]
    pub fn matches(&self, capability: &CapabilityId, scope: &str) -> bool {
        self.capability == *capability && scope_matches(&self.scope, scope)
    }
}

/// Matches a scope glob against a resource path. `**` as a full segment
/// matches any (possibly empty) suffix; `*` matches any run within one
/// segment; anything else matches literally.
#[must_use]
pub fn scope_matches(pattern: &str, scope: &str) -> bool {
    let pattern: Vec<&str> = pattern.split('/').collect();
    let scope: Vec<&str> = scope.split('/').collect();
    match_segments(&pattern, &scope)
}

fn match_segments(pattern: &[&str], scope: &[&str]) -> bool {
    if pattern.is_empty() {
        return scope.is_empty();
    }
    if pattern[0] == "**" {
        return (0..=scope.len()).any(|skip| match_segments(&pattern[1..], &scope[skip..]));
    }
    if scope.is_empty() {
        return false;
    }
    if !match_segment(pattern[0], scope[0]) {
        return false;
    }
    match_segments(&pattern[1..], &scope[1..])
}

fn match_segment(pattern: &str, segment: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == segment;
    }
    // Within-segment wildcard: anchor literal runs in order.
    let mut rest = segment;
    let mut parts = pattern.split('*');
    let Some(first) = parts.next() else {
        return true;
    };
    if !rest.starts_with(first) {
        return false;
    }
    rest = &rest[first.len()..];
    let mut parts: Vec<&str> = parts.collect();
    let last = parts.pop();
    for part in parts {
        let Some(index) = rest.find(part) else {
            return false;
        };
        rest = &rest[index + part.len()..];
    }
    if let Some(last) = last {
        return rest.ends_with(last);
    }
    true
}

/// Default posture for capabilities with no matching grant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DefaultPosture {
    /// Deny unless granted.
    #[default]
    Deny,
    /// Ask unless granted or denied.
    Ask,
}

/// A policy: ordered grants plus per-capability default postures.
#[derive(Clone, Debug, Default)]
pub struct Policy {
    grants: Vec<Grant>,
    /// Explicit deny rules (checked before grants).
    denials: Vec<Grant>,
    default_posture: DefaultPosture,
}

impl Policy {
    #[must_use]
    pub fn new(default_posture: DefaultPosture) -> Self {
        Self {
            grants: Vec::new(),
            denials: Vec::new(),
            default_posture,
        }
    }

    /// Project-trusted defaults: normal reads/writes/search/build/test and
    /// read-only git inside the workspace run automatically.
    #[must_use]
    pub fn trusted_workspace() -> Self {
        let mut policy = Self::new(DefaultPosture::Ask);
        for (capability, scope) in [
            ("fs.read", "workspace/**"),
            ("fs.list", "workspace/**"),
            ("fs.metadata", "workspace/**"),
            ("fs.write", "workspace/**"),
            ("process.spawn", "cargo"),
            ("process.spawn", "git"),
            ("process.spawn", "rustc"),
            ("git.read", "workspace/**"),
            ("search.lexical", "workspace/**"),
        ] {
            policy.allow(capability, scope);
        }
        // Dangerous git writes always ask, even in trusted workspaces.
        for scope in [
            "workspace/**:push",
            "workspace/**:force",
            "workspace/**:hard-reset",
        ] {
            policy.deny("git.write", scope);
        }
        policy
    }

    pub fn allow(&mut self, capability: &str, scope: &str) {
        self.grants.push(Grant::new(capability, scope));
    }

    pub fn deny(&mut self, capability: &str, scope: &str) {
        self.denials.push(Grant::new(capability, scope));
    }

    /// Evaluates a capability use. Denials win over grants; with no match
    /// the default posture applies (`Ask` synthesizes a request).
    #[must_use]
    pub fn decide(
        &self,
        capability: &CapabilityId,
        scope: &str,
        operation: &serde_json::Value,
        summary: &str,
    ) -> PolicyDecision {
        if self
            .denials
            .iter()
            .any(|grant| grant.matches(capability, scope))
        {
            return PolicyDecision::Deny {
                reason: format!("explicit denial for {0} on {scope}", capability.0),
            };
        }
        if self
            .grants
            .iter()
            .any(|grant| grant.matches(capability, scope))
        {
            return PolicyDecision::Allow;
        }
        match self.default_posture {
            DefaultPosture::Deny => PolicyDecision::Deny {
                reason: format!("no grant for {0} on {scope}", capability.0),
            },
            DefaultPosture::Ask => PolicyDecision::Ask {
                request: ApprovalRequest {
                    id: ApprovalId::generate(),
                    capability: capability.clone(),
                    scope: scope.to_owned(),
                    operation_hash: operation_hash(operation),
                    summary: summary.to_owned(),
                },
            },
        }
    }
}

/// Canonical operation hash: BLAKE3 over the JSON value with object keys
/// sorted, so semantically identical operations hash identically and any
/// material change invalidates bound approvals.
#[must_use]
pub fn operation_hash(operation: &serde_json::Value) -> String {
    blake3::hash(&canonical_json(operation))
        .to_hex()
        .to_string()
}

fn canonical_json(value: &serde_json::Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &serde_json::Value, out: &mut Vec<u8>) {
    match value {
        serde_json::Value::Null => out.extend_from_slice(b"null"),
        serde_json::Value::Bool(true) => out.extend_from_slice(b"true"),
        serde_json::Value::Bool(false) => out.extend_from_slice(b"false"),
        serde_json::Value::Number(number) => out.extend_from_slice(number.to_string().as_bytes()),
        serde_json::Value::String(text) => {
            out.extend_from_slice(serde_json::to_string(text).unwrap_or_default().as_bytes());
        }
        serde_json::Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        serde_json::Value::Object(map) => {
            out.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(serde_json::to_string(key).unwrap_or_default().as_bytes());
                out.push(b':');
                write_canonical(&map[*key], out);
            }
            out.push(b'}');
        }
    }
}

/// In-memory approval registry: approvals bind to a capability, scope and
/// operation hash.
/// Durable persistence rides the store's approval records (later milestone).
///
/// Consumed on first use (M11 item 8): a grant authorizes exactly one
/// successful [`Approvals::resolve`], after which the entry is removed and
/// the operation must Ask again under a fresh approval id. The map sits
/// behind a `Mutex` so `resolve` stays `&self` for the shared `ToolsContext`
/// while still consuming the entry.
#[derive(Debug, Default)]
pub struct Approvals {
    granted: std::sync::Mutex<HashMap<ApprovalKey, Approval>>,
}

impl Clone for Approvals {
    fn clone(&self) -> Self {
        Self {
            granted: std::sync::Mutex::new(self.lock().clone()),
        }
    }
}

impl Approvals {
    /// Records the decision for `request`. Returns the stored approval.
    /// A fresh grant for an already-consumed authorization re-arms exactly
    /// one use. Capability, scope and operation hash all identify the grant.
    /// Takes `&self` (the registry is behind its mutex) so a supervisor
    /// holding a shared `Arc<ToolsContext>` can arm a granted ask.
    pub fn decide(&self, request: ApprovalRequest, approved: bool) -> Approval {
        let stored = Approval { request, approved };
        self.lock()
            .insert(approval_key(&stored.request), stored.clone());
        stored
    }

    /// Resolves a pending ask: granted only when a matching approval exists
    /// for the exact capability, scope and current operation hash — and the
    /// grant is consumed on that success, so it authorizes exactly one
    /// execution (M11 item 8). Denied entries persist (a denial is not a grant).
    #[must_use]
    pub fn resolve(
        &self,
        request: &ApprovalRequest,
        operation: &serde_json::Value,
    ) -> PolicyDecision {
        if operation_hash(operation) != request.operation_hash {
            return PolicyDecision::Deny {
                reason: "operation changed since approval was requested".to_owned(),
            };
        }
        let key = approval_key(request);
        let mut granted = self.lock();
        match granted.get(&key) {
            Some(approval) if approval.approved => {
                // Consume on first use: remove before allowing, so the
                // success and the consumption are one atomic step.
                granted.remove(&key);
                PolicyDecision::Allow
            }
            Some(_) => PolicyDecision::Deny {
                reason: "approval denied".to_owned(),
            },
            None => PolicyDecision::Ask {
                request: request.clone(),
            },
        }
    }

    /// Resolves a pending ask WITHOUT consuming the grant (M11): a
    /// pre-effect gate (mutation preparation, the commit boundary) checks
    /// that a grant exists but must not burn it — the grant's single use
    /// belongs to the per-effect recheck that actually mutates. The same
    /// capability/scope/operation binding applies; only consumption is skipped.
    #[must_use]
    pub fn resolve_peek(
        &self,
        request: &ApprovalRequest,
        operation: &serde_json::Value,
    ) -> PolicyDecision {
        if operation_hash(operation) != request.operation_hash {
            return PolicyDecision::Deny {
                reason: "operation changed since approval was requested".to_owned(),
            };
        }
        let key = approval_key(request);
        let granted = self.lock();
        match granted.get(&key) {
            Some(approval) if approval.approved => PolicyDecision::Allow,
            Some(_) => PolicyDecision::Deny {
                reason: "approval denied".to_owned(),
            },
            None => PolicyDecision::Ask {
                request: request.clone(),
            },
        }
    }

    /// Locks the registry; a poisoned lock still yields the map (the data
    /// itself cannot be corrupted by a panic mid-insert).
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ApprovalKey, Approval>> {
        self.granted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Containment failures (spec §34).
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContainmentError {
    #[error("absolute path escapes the workspace request: {0}")]
    AbsoluteOutsideRequest(String),
    #[error("path traversal escapes the workspace: {0}")]
    Traversal(String),
    #[error("symlink component escapes the workspace: {0}")]
    SymlinkEscape(String),
    #[error("resolved target escapes granted roots: {0}")]
    OutsideRoots(String),
    #[error("missing path component: {0}")]
    MissingComponent(String),
}

/// Lexical check: resolving `requested` against `workspace_root` must never
/// climb above the workspace root. Platform-independent and disk-free: the
/// floor is the root's own depth, not zero, so deep temp dirs on Windows
/// and macOS runners are handled exactly like `/tmp`.
#[must_use]
pub fn lexical_contained(workspace_root: &Path, requested: &Path) -> bool {
    if requested.is_absolute() {
        let mut depth: i32 = 0;
        for component in requested.components() {
            match component {
                Component::ParentDir => depth -= 1,
                Component::Normal(_) => depth += 1,
                Component::RootDir | Component::Prefix(_) => depth = 0,
                Component::CurDir => {}
            }
            if depth < 0 {
                return false;
            }
        }
        // Merely absolute is not traversal; the canonical check decides
        // inside vs outside roots.
        return true;
    }
    let mut depth: i32 = 0;
    for component in workspace_root.components() {
        match component {
            Component::ParentDir => depth -= 1,
            Component::Normal(_) => depth += 1,
            Component::RootDir | Component::Prefix(_) | Component::CurDir => {}
        }
    }
    let floor = depth;
    for component in requested.components() {
        match component {
            Component::ParentDir => depth -= 1,
            Component::Normal(_) => depth += 1,
            // An absolute smuggled into a relative join, or a drive-qualified
            // fragment: refuse lexically rather than reason about it.
            Component::RootDir | Component::Prefix(_) => return false,
            Component::CurDir => {}
        }
        if depth < floor {
            return false;
        }
    }
    true
}

/// Resolves `requested` (workspace-relative or absolute) against
/// `workspace_root`, rejecting traversal and symlink escapes, and verifies
/// the result sits inside `workspace_root`. Returns the resolved path.
///
/// Existing prefixes are canonicalized (resolving symlinks); missing
/// trailing components are appended lexically after `..` rejection, so
/// writes to not-yet-existing files are still contained.
pub fn contain(workspace_root: &Path, requested: &Path) -> Result<PathBuf, ContainmentError> {
    let joined = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        workspace_root.join(requested)
    };
    // Lexical pass: reject `..` above the workspace floor before touching disk.
    if !lexical_contained(workspace_root, requested) {
        return Err(ContainmentError::Traversal(requested.display().to_string()));
    }
    // Canonicalize the longest existing prefix to resolve symlinks.
    let mut existing = joined.clone();
    let mut missing: Vec<std::ffi::OsString> = Vec::new();
    loop {
        if existing.exists() {
            break;
        }
        let Some(file_name) = existing.file_name() else {
            return Err(ContainmentError::MissingComponent(
                requested.display().to_string(),
            ));
        };
        missing.push(file_name.to_owned());
        if !existing.pop() {
            return Err(ContainmentError::MissingComponent(
                requested.display().to_string(),
            ));
        }
    }
    let canonical = std::fs::canonicalize(&existing)
        .map_err(|_| ContainmentError::MissingComponent(requested.display().to_string()))?;
    let root = std::fs::canonicalize(workspace_root)
        .map_err(|_| ContainmentError::MissingComponent(workspace_root.display().to_string()))?;
    if canonical != root && !canonical.starts_with(&root) {
        // An absolute request (or a symlinked prefix) landed outside.
        return Err(if requested.is_absolute() {
            ContainmentError::AbsoluteOutsideRequest(requested.display().to_string())
        } else {
            ContainmentError::SymlinkEscape(requested.display().to_string())
        });
    }
    let mut resolved = canonical;
    for component in missing.iter().rev() {
        if component == ".." || component == "." {
            return Err(ContainmentError::Traversal(requested.display().to_string()));
        }
        resolved.push(component);
    }
    if resolved != root && !resolved.starts_with(&root) {
        return Err(ContainmentError::OutsideRoots(
            requested.display().to_string(),
        ));
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trusted_workspace_allows_local_reads() {
        let policy = Policy::trusted_workspace();
        let op = json!({"path": "workspace/src/main.rs"});
        assert_eq!(
            policy.decide(
                &CapabilityId("fs.read".to_owned()),
                "workspace/src/main.rs",
                &op,
                "read main.rs"
            ),
            PolicyDecision::Allow
        );
    }

    #[test]
    fn unknown_capability_asks_with_hash() {
        let policy = Policy::trusted_workspace();
        let op = json!({"host": "api.example.com"});
        let decision = policy.decide(
            &CapabilityId("network.connect".to_owned()),
            "api.example.com",
            &op,
            "connect",
        );
        let PolicyDecision::Ask { request } = decision else {
            panic!("expected Ask");
        };
        assert_eq!(request.operation_hash, operation_hash(&op));
    }

    #[test]
    fn denial_wins_over_grant() {
        let policy = Policy::trusted_workspace();
        let op = json!({"op": "push"});
        assert!(matches!(
            policy.decide(
                &CapabilityId("git.write".to_owned()),
                "workspace/**:push",
                &op,
                "push"
            ),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn grant_for_another_operation_cannot_satisfy_this_ask() {
        // A model-minted approval (granted for the model's own planned
        // operation) presented for the real operation is denied: approval
        // binds the exact operation hash, so no output can self-approve.
        let approvals = Approvals::default();
        let model_op = json!({"batch": "model-minted", "path": "evil.rs"});
        let model_request = ApprovalRequest {
            id: ApprovalId::generate(),
            capability: CapabilityId("mutation.patch".to_owned()),
            scope: "workspace/evil.rs".to_owned(),
            operation_hash: operation_hash(&model_op),
            summary: String::new(),
        };
        approvals.decide(model_request.clone(), true);
        let real_op = json!({"batch": "real-batch-id", "path": "src/a.rs"});
        assert!(matches!(
            approvals.resolve(&model_request, &real_op),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn one_shot_grant_is_bound_to_capability_and_scope() {
        let approvals = Approvals::default();
        let policy = Policy::new(DefaultPosture::Ask);
        let op = json!({"request": "same-json"});
        let PolicyDecision::Ask { request: original } = policy.decide(
            &CapabilityId("capability.one".to_owned()),
            "workspace/a",
            &op,
            "original operation",
        ) else {
            panic!("expected original operation to ask");
        };
        approvals.decide(original.clone(), true);

        let PolicyDecision::Ask {
            request: other_capability,
        } = policy.decide(
            &CapabilityId("capability.two".to_owned()),
            &original.scope,
            &op,
            "same JSON under another capability",
        )
        else {
            panic!("expected other capability to ask");
        };
        assert_eq!(other_capability.operation_hash, original.operation_hash);
        assert!(matches!(
            approvals.resolve(&other_capability, &op),
            PolicyDecision::Ask { .. }
        ));
        assert!(matches!(
            approvals.resolve_peek(&other_capability, &op),
            PolicyDecision::Ask { .. }
        ));

        let PolicyDecision::Ask {
            request: other_scope,
        } = policy.decide(
            &original.capability,
            "workspace/b",
            &op,
            "same JSON under another scope",
        )
        else {
            panic!("expected other scope to ask");
        };
        assert_eq!(other_scope.operation_hash, original.operation_hash);
        assert!(matches!(
            approvals.resolve(&other_scope, &op),
            PolicyDecision::Ask { .. }
        ));
        assert_eq!(
            approvals.resolve(&original, &op),
            PolicyDecision::Allow,
            "mismatched requests must not consume the original one-shot grant"
        );
    }

    #[test]
    fn material_change_invalidates_approval() {
        let approvals = Approvals::default();
        let op = json!({"path": "a", "content": "x"});
        let request = ApprovalRequest {
            id: ApprovalId::generate(),
            capability: CapabilityId("fs.write".to_owned()),
            scope: "workspace/a".to_owned(),
            operation_hash: operation_hash(&op),
            summary: String::new(),
        };
        approvals.decide(request.clone(), true);
        assert_eq!(approvals.resolve(&request, &op), PolicyDecision::Allow);
        let changed = json!({"path": "a", "content": "y"});
        assert!(matches!(
            approvals.resolve(&request, &changed),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn granted_operation_authorizes_exactly_once_then_asks_again_fresh() {
        // Plan item 8: a granted operation hash authorizes exactly ONE
        // successful resolve; afterwards the grant is consumed and the same
        // operation must Ask again under a fresh approval id — a model can
        // never mint or replay its own grant.
        let policy = Policy::new(DefaultPosture::Ask);
        let approvals = Approvals::default();
        let capability = CapabilityId("mutation.patch".to_owned());
        let scope = "workspace/a.rs".to_owned();
        let op = json!({"path": "a.rs", "content": "x"});

        // First authorize() equivalent: Ask, human grants, resolve succeeds.
        let PolicyDecision::Ask { request: first } =
            policy.decide(&capability, &scope, &op, "patch a.rs")
        else {
            panic!("expected Ask before any grant");
        };
        approvals.decide(first.clone(), true);
        assert_eq!(approvals.resolve(&first, &op), PolicyDecision::Allow);

        // Second attempt on the SAME operation: the policy synthesizes a
        // fresh ask, and the consumed grant must not satisfy it.
        let PolicyDecision::Ask { request: second } =
            policy.decide(&capability, &scope, &op, "patch a.rs")
        else {
            panic!("expected Ask after the grant was consumed");
        };
        assert_ne!(second.id, first.id, "policy mints a fresh approval id");
        let decision = approvals.resolve(&second, &op);
        assert!(
            matches!(&decision, PolicyDecision::Ask { request } if request.id == second.id),
            "consumed grant must re-ask under the fresh id, got {decision:?}"
        );

        // A fresh human re-grant authorizes exactly one more use, then
        // consumption bites again.
        approvals.decide(second.clone(), true);
        assert_eq!(approvals.resolve(&second, &op), PolicyDecision::Allow);
        assert!(
            matches!(approvals.resolve(&second, &op), PolicyDecision::Ask { .. }),
            "the re-grant must also be one-shot"
        );
    }

    #[test]
    fn glob_matching() {
        assert!(scope_matches("workspace/**", "workspace/src/main.rs"));
        assert!(!scope_matches("workspace/src/**", "workspace/other/x"));
        assert!(scope_matches("cargo", "cargo"));
        assert!(!scope_matches("cargo", "cargo-extra"));
        assert!(scope_matches("workspace/*.rs", "workspace/main.rs"));
        assert!(!scope_matches("workspace/*.rs", "workspace/sub/main.rs"));
    }

    #[test]
    fn traversal_rejected() {
        let root = std::env::temp_dir();
        assert!(matches!(
            contain(&root, Path::new("../../etc/passwd")),
            Err(ContainmentError::Traversal(_))
        ));
    }

    #[test]
    fn traversal_rejected_under_deep_root() {
        // Simulates deep temp dirs (Windows/macOS runners): `..` chains that
        // stay non-negative from the filesystem root must still be rejected
        // when they climb above the workspace floor.
        let base = std::env::temp_dir().join(format!("tachyon-deep-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let root = base.join("a").join("b").join("workspace");
        std::fs::create_dir_all(&root).unwrap();
        assert!(!lexical_contained(&root, Path::new("../../outside")));
        assert!(matches!(
            contain(&root, Path::new("../../outside")),
            Err(ContainmentError::Traversal(_))
        ));
        assert!(lexical_contained(&root, Path::new("sub/../inside")));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_escape_rejected() {
        use std::os::unix::fs::symlink;
        let base = std::env::temp_dir().join(format!("tachyon-contain-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("root/sub")).unwrap();
        symlink("/etc", base.join("root/sub/evil")).unwrap();
        let result = contain(&base.join("root"), Path::new("sub/evil/passwd"));
        assert!(
            matches!(
                result,
                Err(ContainmentError::SymlinkEscape(_)
                    | ContainmentError::AbsoluteOutsideRequest(_)
                    | ContainmentError::OutsideRoots(_))
            ),
            "unexpected: {result:?}"
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}
