//! Executable acceptance contracts and verification-gated completion (M9).

#![warn(unsafe_code)]

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod test_support;

mod compile;
mod contract;
mod plan;
mod project;
mod runner;
mod snapshot;
pub use plan::{HardRequirement, VerificationPlan, VerificationRisk};
pub use project::{ProjectDetector, RustProjectDetector};
pub use runner::{CheckEvidence, VerificationReport, run, run_with_lifetime};
pub use snapshot::WorkspaceSnapshot;

pub use compile::{compile_criteria, compile_criterion, compile_spec};
pub use contract::{AcceptanceContract, Clause, CommandCheck, VerifyError};
