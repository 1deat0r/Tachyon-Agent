//! Env-gated fault points (ADR 0001, M12).
//!
//! Named holds compiled into production code. No-op unless
//! `TACHYON_FAULT_POINT` matches the call name (cached once). When armed,
//! the calling task parks until the process is killed — the deterministic
//! seam for recovery kill tests. Portable: no libc, works in the shipped
//! binary and in child-reexec test processes.

use std::sync::OnceLock;

/// Returns the armed fault-point name, if any.
fn armed() -> Option<&'static str> {
    static ARMED: OnceLock<Option<String>> = OnceLock::new();
    ARMED
        .get_or_init(|| std::env::var("TACHYON_FAULT_POINT").ok())
        .as_deref()
}

/// True when `TACHYON_FAULT_POINT` equals `name`.
#[must_use]
pub fn is_armed(name: &str) -> bool {
    armed() == Some(name)
}

/// Parks forever when this fault point is armed; returns immediately otherwise.
///
/// Call from async contexts at a commit seam. The parent test arms the
/// env var, waits until the child hits the seam, then `Child::kill()`s.
pub async fn reach(name: &str) {
    if is_armed(name) {
        if let Some(path) = std::env::var_os("TACHYON_FAULT_REACHED_FILE") {
            // A subprocess recovery test can wait for this marker before it
            // kills the child, avoiding timing guesses around the seam.
            let _ = std::fs::write(path, name);
        }
        std::future::pending::<()>().await;
    }
}

/// Blocking variant for synchronous seams (same semantics as [`reach`]).
///
/// # Panics
/// Never panics; parks the OS thread when armed.
pub fn reach_blocking(name: &str) {
    if is_armed(name) {
        loop {
            std::thread::park();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_point_unarmed_is_noop() {
        // Default test env has no TACHYON_FAULT_POINT (OnceLock caches first read).
        assert!(!is_armed("unit_test_seam_never_armed"));
    }

    #[tokio::test]
    async fn fault_point_reach_returns_immediately_when_unarmed() {
        tokio::time::timeout(std::time::Duration::from_millis(50), reach("never"))
            .await
            .expect("unarmed reach must not park");
    }

    #[test]
    fn fault_point_reach_blocking_returns_immediately_when_unarmed() {
        reach_blocking("never");
    }
}
