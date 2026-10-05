//! Bounded pre-mutation model attempts. The driver owns this policy;
//! adapters perform one call and never retry internally.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tachyon_models::{ModelCallRecord, ModelError, ModelProvider, ModelRequest, ModelResult};
use tokio_util::sync::CancellationToken;

use super::{DriveError, Proposer, RunRecord};

/// One initial call plus one retry of a malformed response. No other error,
/// valid decision, or rejected patch is retried by this policy.
const MAX_MODEL_ATTEMPTS: u32 = 2;

pub(super) async fn invoke(
    proposer: &mut Proposer,
    provider: &Arc<dyn ModelProvider>,
    request: ModelRequest,
    cancel: &CancellationToken,
    deadline_ms: u64,
) -> Result<(ModelResult, Vec<ModelCallRecord>), DriveError> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    let mut calls = Vec::new();
    for attempt in 1..=MAX_MODEL_ATTEMPTS {
        if cancel.is_cancelled() {
            return Err(DriveError::RunCancelled);
        }
        // Supervisor acknowledgment rejects stale revision/run identity before
        // each call. No model output is accepted without another fenced ack.
        proposer
            .propose(RunRecord::Stage {
                stage: "model".into(),
                detail: format!("requesting proposal (attempt {attempt}/{MAX_MODEL_ATTEMPTS})"),
            })
            .await?;
        if tokio::time::Instant::now() >= deadline {
            return Err(DriveError::Provider(ModelError::Timeout {
                timeout_ms: deadline_ms,
            }));
        }
        let (sink, events) = tokio::sync::mpsc::unbounded_channel();
        // The patch driver does not display unvalidated streaming fragments.
        // Dropping this receiver also prevents a failed call accumulating text.
        drop(events);
        let started = Instant::now();
        let invocation = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(DriveError::RunCancelled),
            result = tokio::time::timeout_at(deadline, provider.invoke_observed(request.clone(), sink)) => {
                result.unwrap_or_else(|_| tachyon_models::ModelInvocation {
                    result: Err(ModelError::Timeout { timeout_ms: deadline_ms }),
                    usage: tachyon_models::ModelUsage::default(),
                })
            },
        };
        if cancel.is_cancelled() {
            return Err(DriveError::RunCancelled);
        }
        let call = ModelCallRecord {
            attempt,
            latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
            usage: invocation.usage,
            output_failure: invocation
                .result
                .as_ref()
                .err()
                .and_then(|e| e.output_failure().map(str::to_owned)),
            error: invocation
                .result
                .as_ref()
                .err()
                .map(|e| e.code().to_owned()),
        };
        proposer
            .propose(RunRecord::Stage {
                stage: "model_attempt".into(),
                detail: serde_json::to_string(&call)?,
            })
            .await?;
        calls.push(call);
        match invocation.result {
            Ok(result) => return Ok((result, calls)),
            Err(ModelError::MalformedOutput(_)) if attempt < MAX_MODEL_ATTEMPTS => {}
            Err(error) => return Err(DriveError::Provider(error)),
        }
    }
    unreachable!("each final attempt returns its result")
}
