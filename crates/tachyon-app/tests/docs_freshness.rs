//! Docs-freshness tripwires: structural invariants between code and docs.
//!
//! These tests fail when documentation drifts from the workspace instead of
//! letting it rot silently. Fuzzy prose consistency (does the README describe
//! what the code actually does?) is reserved for the `tachyon-judgment`
//! milestone, where a bounded Jev judge fits; everything checked here is
//! exact, so it is checked deterministically.

use std::collections::BTreeSet;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn read(name: &str) -> String {
    let path = workspace_root().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{name} unreadable: {error}"))
}

/// `Milestone N` markers in `text`, in order of appearance.
fn milestones(text: &str) -> Vec<u64> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(index) = rest.find("Milestone ") {
        rest = &rest[index + "Milestone ".len()..];
        let digits: String = rest.chars().take_while(|c| c.is_numeric()).collect();
        if let Ok(number) = digits.parse::<u64>() {
            found.push(number);
        }
    }
    found
}

fn workspace_members() -> Vec<String> {
    let manifest = read("Cargo.toml");
    let mut members = Vec::new();
    let mut in_members = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with("members") {
            in_members = true;
            continue;
        }
        if in_members {
            if line.starts_with(']') {
                break;
            }
            let name = line.trim_matches(|c| c == '"' || c == ',' || c == ' ');
            let name = name.strip_prefix("crates/").unwrap_or(name);
            if !name.is_empty() {
                members.push(name.to_owned());
            }
        }
    }
    members
}

/// Workspace members as a sorted set, the shape every membership
/// comparison wants.
fn member_set() -> BTreeSet<String> {
    workspace_members().into_iter().collect()
}

/// Crates whose milestone is complete MUST be implemented: the scaffold
/// marker must be gone from their `lib.rs`. Move a crate into this list
/// with the milestone that implements it — the test forces the docs move
/// to happen alongside the code.
const IMPLEMENTED: &[&str] = &[
    "tachyon-types",
    "tachyon-protocol",
    "tachyon-ir",
    "tachyon-store",
    "tachyon-core",
    "tachyon-gateway",
    "tachyon-app",
    "tachyon-scheduler",
    "tachyon-policy",
    "tachyon-tools",
    "tachyon-repo",
    "tachyon-router",
    "tachyon-telemetry",
    "tachyon-retrieval",
    "tachyon-models",
    "tachyon-judgment",
    "tachyon-mutation",
    "tachyon-verify",
    "tachyon-tui",
];

#[test]
fn readme_status_matches_progress_gates() {
    let readme = read("README.md");
    let progress = read("PROGRESS.md");
    let gates_section = progress
        .split("## Completed gates")
        .nth(1)
        .expect("PROGRESS.md needs a Completed gates section");
    let completed: BTreeSet<u64> = milestones(gates_section).into_iter().collect();
    assert!(!completed.is_empty(), "no completed gates in PROGRESS.md");
    let latest = completed.iter().max().copied().unwrap_or(0);
    let status = readme
        .lines()
        .find(|line| line.contains("Status ("))
        .expect("README.md needs a Status line");
    let claimed: BTreeSet<u64> = milestones(status).into_iter().collect();
    assert!(
        claimed.contains(&latest),
        "README status ({status}) lags PROGRESS.md gates (latest Milestone {latest})"
    );
}

#[test]
fn current_milestone_follows_gates() {
    let progress = read("PROGRESS.md");
    let current = progress
        .split("## Current milestone")
        .nth(1)
        .expect("PROGRESS.md needs a Current milestone section");
    let current: BTreeSet<u64> = milestones(current).into_iter().collect();
    let gates = progress.split("## Completed gates").nth(1).unwrap_or("");
    let completed: BTreeSet<u64> = milestones(gates).into_iter().collect();
    let latest = completed.iter().max().copied().unwrap_or(0);
    assert!(
        current.iter().any(|milestone| *milestone == latest + 1),
        "current milestone {current:?} should be Milestone {} after gates {completed:?}",
        latest + 1
    );
}

#[test]
fn changelog_covers_completed_milestones() {
    let changelog = read("CHANGELOG.md");
    let progress = read("PROGRESS.md");
    let gates = progress.split("## Completed gates").nth(1).unwrap_or("");
    let completed: BTreeSet<u64> = milestones(gates).into_iter().collect();
    for milestone in completed {
        assert!(
            changelog.contains(&format!("Milestone {milestone}")),
            "CHANGELOG.md has no entry for completed Milestone {milestone}"
        );
    }
}

/// Spec §1 enumerates every workspace crate with its responsibility; it is
/// the implementation contract. Cargo.toml is the workspace truth. Neither
/// may drift from the other in either direction.
#[test]
fn spec_crate_list_matches_workspace_members() {
    let spec = spec_crates();
    let members = member_set();
    assert!(!members.is_empty(), "no workspace members parsed");
    let missing_from_spec: Vec<&String> = members.difference(&spec).collect();
    let phantom_in_spec: Vec<&String> = spec.difference(&members).collect();
    assert!(
        missing_from_spec.is_empty() && phantom_in_spec.is_empty(),
        "spec §1 crate list drifts from workspace members; \
         missing from spec: {missing_from_spec:?}; \
         in spec but not a workspace member: {phantom_in_spec:?}"
    );
}

/// All markdown files that are project documentation, excluding build
/// output, VCS data, agent config, and the gitignored third-party skill
/// packs (`.agents/`, `agent/`, `.claude/` — their example ADR numbers are
/// not references to this repo's ADRs). A new artifact directory that is
/// not listed fails loudly; add it deliberately.
fn markdown_files() -> Vec<PathBuf> {
    const SKIP: &[&str] = &[
        ".git",
        ".cargo",
        ".claude",
        ".agents",
        ".mimocode",
        ".unlazy",
        "agent",
        "target",
        "node_modules",
    ];
    fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
        let entries = std::fs::read_dir(dir)
            .unwrap_or_else(|error| panic!("{} unreadable: {error}", dir.display()));
        for entry in entries {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if SKIP.contains(&name.as_str()) {
                continue;
            }
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|extension| extension == "md") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(&workspace_root(), &mut files);
    files.sort();
    files
}

/// Sorted `NNNN-*.md` filenames in docs/adr, excluding README.
fn adr_filenames() -> Vec<String> {
    let mut names = Vec::new();
    let entries = std::fs::read_dir(workspace_root().join("docs").join("adr"))
        .unwrap_or_else(|error| panic!("docs/adr unreadable: {error}"));
    for entry in entries {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        if name != "README.md" {
            names.push(name);
        }
    }
    names.sort();
    names
}

/// ADR numbers present as `docs/adr/NNNN-*.md` filenames, in order.
fn adr_numbers() -> Vec<u64> {
    adr_filenames()
        .iter()
        .map(|name| {
            name.split('-')
                .next()
                .and_then(|prefix| prefix.parse().ok())
                .unwrap_or_else(|| panic!("ADR filename not NNNN-title.md: {name}"))
        })
        .collect()
}

/// ADR numbering must be contiguous from 0001 with no gaps or duplicates,
/// and every `ADR-NNNN` / `ADR NNNN` / `adr/NNNN-…` mention in any markdown
/// file must point at an ADR that exists — an `adr/NNNN-title.md` mention
/// must match a real filename, not merely a valid number.
#[test]
fn adr_numbering_contiguous_and_references_resolve() {
    let numbers = adr_numbers();
    assert!(!numbers.is_empty(), "no ADR files found in docs/adr");
    let expected: Vec<u64> = (1..=numbers.len() as u64).collect();
    assert_eq!(
        numbers, expected,
        "docs/adr numbering must be contiguous 0001..N"
    );
    let known: BTreeSet<u64> = numbers.into_iter().collect();
    let filenames: BTreeSet<String> = adr_filenames().into_iter().collect();
    let mut dangling = Vec::new();
    for file in markdown_files() {
        let text = std::fs::read_to_string(&file).unwrap();
        let bytes = text.as_bytes();
        let display = file
            .strip_prefix(workspace_root())
            .unwrap_or(&file)
            .display()
            .to_string();
        let mut scan = |needle: &str| {
            let mut from = 0;
            while let Some(found) = text[from..].find(needle) {
                let start = from + found + needle.len();
                from = start;
                let digits: String = bytes[start..]
                    .iter()
                    .take_while(|byte| byte.is_ascii_digit())
                    .map(|byte| (*byte as char).to_string())
                    .collect();
                if digits.len() == 4 {
                    if needle == "adr/" {
                        let token: String = bytes[start..]
                            .iter()
                            .take_while(|byte| {
                                byte.is_ascii_alphanumeric()
                                    || **byte == b'-'
                                    || **byte == b'_'
                                    || **byte == b'.'
                            })
                            .map(|byte| (*byte as char).to_string())
                            .collect();
                        let is_markdown = std::path::Path::new(&token)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("md"));
                        if is_markdown && !filenames.contains(&token) {
                            dangling.push(format!("{display}: adr/{token}"));
                            continue;
                        }
                    }
                    let number: u64 = digits.parse().unwrap();
                    if !known.contains(&number) {
                        dangling.push(format!("{display}: {needle}{digits}"));
                    }
                }
            }
        };
        scan("ADR-");
        scan("ADR ");
        scan("adr/");
    }
    dangling.sort();
    dangling.dedup();
    assert!(
        dangling.is_empty(),
        "references to ADRs that do not exist: {dangling:?}"
    );
}

/// The path half of a raw link target — first token, fragment stripped —
/// when the target is a relative path; None for empty, anchor-only,
/// mailto, and http(s) targets.
fn relative_link_path(raw: &str) -> Option<&str> {
    let target = raw.split_whitespace().next()?;
    if target.starts_with('#') || target.starts_with("mailto:") || target.contains("://") {
        return None;
    }
    let path = target.split('#').next()?;
    (!path.is_empty()).then_some(path)
}

/// Every relative link in project markdown — inline `[text](target)` and
/// reference-style `[label]: target` definitions alike — must resolve to
/// something that exists, judged from the linking file's directory
/// (fragment stripped). A path with an optional link title
/// (`](path "title")`) resolves against `path`.
#[test]
fn relative_markdown_links_resolve() {
    let mut broken = Vec::new();
    for file in markdown_files() {
        let text = std::fs::read_to_string(&file).unwrap();
        let display = file
            .strip_prefix(workspace_root())
            .unwrap_or(&file)
            .display()
            .to_string();
        let mut check = |raw: &str| {
            if let Some(path) = relative_link_path(raw)
                && !file.parent().unwrap().join(path).exists()
            {
                broken.push(format!("{display}: {path}"));
            }
        };
        // Inline links: `[text](target)`.
        let mut from = 0;
        while let Some(found) = text[from..].find("](") {
            let start = from + found + 2;
            let Some(end) = text[start..].find(')') else {
                break;
            };
            from = start + end + 1;
            check(&text[start..start + end]);
        }
        // Reference-style definitions: `[label]: target`.
        for line in text.lines() {
            let line = line.trim_start();
            if !line.starts_with('[') {
                continue;
            }
            let Some(close) = line.find("]:") else {
                continue;
            };
            check(&line[close + 2..]);
        }
    }
    broken.sort();
    broken.dedup();
    assert!(
        broken.is_empty(),
        "relative markdown links point at nothing: {broken:#?}"
    );
}

/// Every ADR must carry the sections docs/adr/README.md requires of it.
/// ADRs 0001–0003 predate the README format (grandfathered to the core
/// trio + Status, per README); 0004 onward must satisfy the full list.
#[test]
fn adrs_contain_required_sections() {
    const GRANDFATHERED_CORE: &[&str] = &["Status", "Context", "Decision", "Consequences"];
    const FIRST_FULL_FORMAT: u64 = 4;
    const REQUIRED_SECTIONS: &[&str] = &[
        "Status",
        "Context",
        "Decision",
        "Alternatives considered",
        "Evidence",
        "Consequences",
        "Migration/rollback plan",
    ];

    let requirements = read("docs/adr/README.md").replace('\r', "");
    let requirements = requirements
        .split("Each ADR should contain:")
        .nth(1)
        .expect("docs/adr/README.md needs an \"Each ADR should contain:\" list");
    let mut full_format = Vec::new();
    for line in requirements.lines() {
        let line = line.trim();
        if let Some(item) = line.strip_prefix("- ") {
            full_format.push(item.trim().to_owned());
        } else if !line.is_empty() && !full_format.is_empty() {
            break;
        }
    }
    assert!(
        !full_format.is_empty(),
        "parsed no required sections from docs/adr/README.md"
    );
    for core in GRANDFATHERED_CORE {
        assert!(
            full_format.iter().any(|item| item == core),
            "grandfathered core section {core:?} missing from docs/adr/README.md's list"
        );
    }
    assert_eq!(
        full_format,
        REQUIRED_SECTIONS.to_vec(),
        "docs/adr/README.md's required-sections list drifted from the pinned \
         seven-section format; if the ADR format genuinely changed, update \
         REQUIRED_SECTIONS deliberately"
    );

    let mut violations = Vec::new();
    for number in adr_numbers() {
        let required: Vec<&str> = if number >= FIRST_FULL_FORMAT {
            full_format.iter().map(String::as_str).collect()
        } else {
            GRANDFATHERED_CORE.to_vec()
        };
        let prefix = format!("{number:04}-");
        let entry = std::fs::read_dir(workspace_root().join("docs").join("adr"))
            .unwrap()
            .find(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&prefix)
            })
            .unwrap_or_else(|| panic!("no ADR file numbered {number:04}"));
        let path = entry.unwrap().path();
        let text = std::fs::read_to_string(&path).unwrap();
        let headings: BTreeSet<&str> = text
            .lines()
            .filter_map(|line| line.strip_prefix("## "))
            .map(str::trim)
            .collect();
        for section in &required {
            if !headings.contains(section) {
                violations.push(format!("{number:04}: missing \"## {section}\""));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "ADRs missing sections required by docs/adr/README.md: {violations:?}"
    );
}

/// Every crate named in CONTEXT.md's "Repo / crates (shorthand)" section
/// must be a real workspace member. The list is documented shorthand, not
/// exhaustive, so this checks subset — never equality.
#[test]
fn context_crate_shorthand_exists_in_workspace() {
    let context = read("CONTEXT.md");
    let rest = context
        .split("## Repo / crates (shorthand)")
        .nth(1)
        .expect("CONTEXT.md needs a \"Repo / crates (shorthand)\" section");
    let section = rest.split("\n## ").next().unwrap_or(rest);
    let mut named = BTreeSet::new();
    for token in section.split('`') {
        if let Some(name) = token.strip_prefix("tachyon-")
            && !name.is_empty()
            && !name.contains(char::is_whitespace)
        {
            named.insert(format!("tachyon-{name}"));
        }
    }
    assert!(
        !named.is_empty(),
        "parsed no crates from CONTEXT.md shorthand"
    );
    let members = member_set();
    let phantom: Vec<&String> = named.difference(&members).collect();
    assert!(
        phantom.is_empty(),
        "CONTEXT.md shorthand names crates that are not workspace members: {phantom:?}"
    );
}

/// Every directory under `crates/` must be a workspace member and every
/// member must have a directory — a crate neither side forgot.
#[test]
fn crate_directories_match_workspace_members() {
    let members = member_set();
    assert!(!members.is_empty(), "no workspace members parsed");
    let mut directories = BTreeSet::new();
    let entries = std::fs::read_dir(workspace_root().join("crates"))
        .unwrap_or_else(|error| panic!("crates/ unreadable: {error}"));
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            directories.insert(path.file_name().unwrap().to_string_lossy().into_owned());
        }
    }
    let unmembered: Vec<&String> = directories.difference(&members).collect();
    let undirected: Vec<&String> = members.difference(&directories).collect();
    assert!(
        unmembered.is_empty() && undirected.is_empty(),
        "crates/ directories drift from workspace members; \
         directory without a member: {unmembered:?}; \
         member without a directory: {undirected:?}"
    );
}

/// Parses the responsibility bullets (one `tachyon-…` list item per crate)
/// under §1's "Crate responsibilities:" lead-in.
fn spec_crates() -> BTreeSet<String> {
    let spec = read("docs/02_IMPLEMENTATION_SPEC.md");
    let rest = spec
        .split("Crate responsibilities:")
        .nth(1)
        .expect("spec §1 needs a \"Crate responsibilities:\" list");
    let mut crates = BTreeSet::new();
    for line in rest.lines() {
        let line = line.trim();
        if let Some(after_marker) = line.strip_prefix("- `") {
            let name = after_marker.split('`').next().unwrap_or("");
            if let Some(crate_name) = name.strip_prefix("tachyon-") {
                crates.insert(format!("tachyon-{crate_name}"));
            }
        } else if !line.is_empty() && !crates.is_empty() {
            break;
        }
    }
    assert!(!crates.is_empty(), "parsed no crate bullets from spec §1");
    crates
}

#[test]
fn implemented_crates_left_the_scaffold() {
    let members = workspace_members();
    assert!(!members.is_empty(), "no workspace members parsed");
    for name in members {
        let dir = workspace_root().join("crates").join(&name).join("src");
        let lib = dir.join("lib.rs");
        let main = dir.join("main.rs");
        assert!(
            lib.exists() || main.exists(),
            "missing src/lib.rs or src/main.rs for {name}"
        );
        if !lib.exists() {
            continue;
        }
        let source = std::fs::read_to_string(&lib).unwrap();
        if IMPLEMENTED.contains(&name.as_str()) {
            assert!(
                !source.contains("Scaffold only"),
                "{name} is listed IMPLEMENTED but still carries the scaffold marker"
            );
        }
    }
}
