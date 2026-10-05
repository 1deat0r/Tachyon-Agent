//! Usage availability must not be inferred from legacy counters or provider IDs.

use async_trait::async_trait;
use serde_json::{Value, json};
use tachyon_models::{
    AgentDecision, FakeModelProvider, FakeResponse, HttpTransport, ModelError, ModelProvider,
    ModelRequest, ModelResult, ModelUsage, OpenAiCompatConfig, OpenAiCompatProvider, Role,
    UsageProvenance,
};
use tachyon_types::ProviderId;

fn request() -> ModelRequest {
    ModelRequest {
        role: Role::Primary,
        model: "usage-test".to_owned(),
        context: vec![],
        max_output_tokens: 64,
        require_structured_output: true,
    }
}

struct StubTransport(String);

#[async_trait]
impl HttpTransport for StubTransport {
    async fn post_json(
        &self,
        _url: &str,
        _api_key: Option<&str>,
        _body: &str,
        _timeout_ms: u64,
    ) -> Result<String, ModelError> {
        Ok(self.0.clone())
    }
}

async fn http_result(usage: Option<Value>) -> ModelResult {
    let mut response = json!({
        "choices": [{"message": {"content": "{\"decision\":\"respond\",\"message\":\"usage\"}"}}]
    });
    if let Some(usage) = usage {
        response["usage"] = usage;
    }
    let provider = OpenAiCompatProvider::new(
        ProviderId("same-id".to_owned()),
        OpenAiCompatConfig::default(),
        StubTransport(response.to_string()),
    );
    let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
    provider.invoke(request(), sink).await.expect("HTTP result")
}

#[tokio::test]
async fn http_usage_provenance_requires_an_object_not_a_provider_name() {
    for usage in [
        None,
        Some(Value::Null),
        Some(json!(false)),
        Some(json!(0)),
        Some(json!("malformed")),
        Some(json!([])),
        Some(json!([{"prompt_tokens": 7}])),
    ] {
        let result = http_result(usage.clone()).await;
        assert_eq!(result.usage, ModelUsage::default(), "usage: {usage:?}");
        assert_eq!((result.input_tokens, result.output_tokens), (0, 0));
    }
    let result = http_result(Some(json!({}))).await;
    assert_eq!(
        result.usage,
        ModelUsage {
            input_tokens: None,
            output_tokens: None,
            provenance: UsageProvenance::ProviderReported,
        }
    );
    assert_eq!((result.input_tokens, result.output_tokens), (0, 0));
}

#[tokio::test]
async fn http_usage_missing_or_malformed_fields_are_independently_unavailable() {
    for (usage, input_tokens, output_tokens) in [
        (json!({"prompt_tokens": 0}), Some(0), None),
        (json!({"completion_tokens": 11}), None, Some(11)),
        (json!({"input_tokens": 7, "output_tokens": 8}), None, None),
    ] {
        let result = http_result(Some(usage.clone())).await;
        assert_eq!(
            result.usage,
            ModelUsage {
                input_tokens,
                output_tokens,
                provenance: UsageProvenance::ProviderReported,
            },
            "usage: {usage}"
        );
        assert_eq!(result.input_tokens, input_tokens.unwrap_or(0));
        assert_eq!(result.output_tokens, output_tokens.unwrap_or(0));
    }

    for malformed in [
        Value::Null,
        json!("12"),
        json!(true),
        json!([]),
        json!({}),
        json!(-1),
        json!(1.5),
        json!(0.0),
        json!(1.0),
        json!(1.0e100),
        serde_json::from_str("18446744073709551616").expect("JSON integer exceeding u64"),
    ] {
        for (usage, input_tokens, output_tokens) in [
            (
                json!({"prompt_tokens": malformed, "completion_tokens": 11}),
                None,
                Some(11),
            ),
            (
                json!({"prompt_tokens": 0, "completion_tokens": malformed}),
                Some(0),
                None,
            ),
            (
                json!({"prompt_tokens": malformed, "completion_tokens": malformed}),
                None,
                None,
            ),
        ] {
            let result = http_result(Some(usage.clone())).await;
            assert_eq!(
                result.usage,
                ModelUsage {
                    input_tokens,
                    output_tokens,
                    provenance: UsageProvenance::ProviderReported,
                },
                "usage: {usage}"
            );
            assert_eq!(result.input_tokens, input_tokens.unwrap_or(0));
            assert_eq!(result.output_tokens, output_tokens.unwrap_or(0));
        }
    }
}

#[tokio::test]
async fn http_usage_preserves_reported_counts_including_explicit_zero() {
    for (input_tokens, output_tokens) in [(17, 9), (0, 0), (u32::MAX, u32::MAX)] {
        let result = http_result(Some(json!({
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
        })))
        .await;
        assert_eq!(
            (result.input_tokens, result.output_tokens),
            (input_tokens, output_tokens)
        );
        assert_eq!(
            result.usage,
            ModelUsage {
                input_tokens: Some(input_tokens),
                output_tokens: Some(output_tokens),
                provenance: UsageProvenance::ProviderReported,
            }
        );
    }
}

#[tokio::test]
async fn http_usage_overflow_is_unavailable_without_changing_legacy_saturation() {
    for overflow in [u64::from(u32::MAX) + 1, u64::MAX] {
        for (usage, input_tokens, output_tokens, legacy_input, legacy_output) in [
            (
                json!({"prompt_tokens": overflow, "completion_tokens": 0}),
                None,
                Some(0),
                u32::MAX,
                0,
            ),
            (
                json!({"prompt_tokens": 11, "completion_tokens": overflow}),
                Some(11),
                None,
                11,
                u32::MAX,
            ),
            (
                json!({"prompt_tokens": overflow, "completion_tokens": overflow}),
                None,
                None,
                u32::MAX,
                u32::MAX,
            ),
        ] {
            let result = http_result(Some(usage.clone())).await;
            assert_eq!(result.input_tokens, legacy_input);
            assert_eq!(result.output_tokens, legacy_output);
            assert_eq!(
                result.usage,
                ModelUsage {
                    input_tokens,
                    output_tokens,
                    provenance: UsageProvenance::ProviderReported,
                },
                "usage: {usage}"
            );
        }
    }
}

#[test]
fn legacy_result_without_usage_preserves_counters_but_not_availability() {
    let legacy = json!({
        "decision": {"decision": "respond", "message": "legacy"},
        "input_tokens": 17,
        "output_tokens": 9,
        "latency_ms": 1.0,
        "provider": "scripted-sounding-name",
        "model": "legacy-model"
    });
    let result: ModelResult = serde_json::from_value(legacy).expect("legacy result");
    assert_eq!(result.input_tokens, 17);
    assert_eq!(result.output_tokens, 9);
    assert_eq!(result.usage, ModelUsage::default());
    assert_eq!(UsageProvenance::default(), UsageProvenance::Unknown);
    let serialized = serde_json::to_value(result).expect("serialize result");
    assert_eq!(
        serialized["usage"],
        json!({"input_tokens": null, "output_tokens": null, "provenance": "unknown"})
    );
}

#[test]
fn usage_round_trips_without_reinterpreting_legacy_counters() {
    for (usage, provenance) in [
        (ModelUsage::default(), "unknown"),
        (
            ModelUsage {
                input_tokens: None,
                output_tokens: Some(0),
                provenance: UsageProvenance::ProviderReported,
            },
            "provider_reported",
        ),
        (
            ModelUsage {
                input_tokens: Some(u32::MAX),
                output_tokens: Some(0),
                provenance: UsageProvenance::Scripted,
            },
            "scripted",
        ),
    ] {
        let result = ModelResult {
            decision: AgentDecision::Respond {
                message: "round trip".to_owned(),
            },
            input_tokens: 17,
            output_tokens: 9,
            usage,
            latency_ms: 1.0,
            provider: ProviderId("same-id".to_owned()),
            model: "usage-test".to_owned(),
        };
        let serialized = serde_json::to_value(&result).expect("serialize result");
        assert_eq!(
            serialized["usage"],
            json!({
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "provenance": provenance,
            })
        );
        let decoded: ModelResult = serde_json::from_value(serialized).expect("deserialize result");
        assert_eq!(decoded, result);
    }
}

#[tokio::test]
async fn fake_usage_is_scripted_even_for_zero_and_arbitrary_provider_ids() {
    let provider = FakeModelProvider::new(ProviderId("same-id".to_owned()));
    for (input_tokens, output_tokens) in [(0, 0), (17, 9), (u32::MAX, u32::MAX)] {
        provider.push_response(FakeResponse {
            text: "scripted".to_owned(),
            decision: AgentDecision::Respond {
                message: "scripted".to_owned(),
            },
            input_tokens,
            output_tokens,
        });
        let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
        let result = provider.invoke(request(), sink).await.expect("fake result");
        assert_eq!(result.input_tokens, input_tokens);
        assert_eq!(result.output_tokens, output_tokens);
        assert_eq!(
            result.usage,
            ModelUsage {
                input_tokens: Some(input_tokens),
                output_tokens: Some(output_tokens),
                provenance: UsageProvenance::Scripted,
            }
        );
    }
}

#[tokio::test]
async fn malformed_response_retains_reported_usage_without_accepting_text() {
    for content in [serde_json::Value::Null, json!(""), json!("not JSON")] {
        let response = json!({
            "choices": [{"message": {"content": content}}],
            "usage": {"prompt_tokens": 17, "completion_tokens": 9}
        });
        let provider = OpenAiCompatProvider::new(
            ProviderId("malformed-usage".into()),
            OpenAiCompatConfig::default(),
            StubTransport(response.to_string()),
        );
        let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
        let attempt = provider.invoke_observed(request(), sink).await;
        assert!(matches!(
            attempt.result,
            Err(ModelError::MalformedOutput(_))
        ));
        assert_eq!(attempt.usage.input_tokens, Some(17));
        assert_eq!(attempt.usage.output_tokens, Some(9));
        assert_eq!(attempt.usage.provenance, UsageProvenance::ProviderReported);
    }
}

#[test]
fn attempt_totals_do_not_turn_missing_usage_into_zero() {
    use tachyon_models::ModelCallRecord;
    let mut calls = vec![ModelCallRecord {
        attempt: 1,
        latency_ms: 1.0,
        error: Some("malformed_output".into()),
        output_failure: Some("invalid_json".into()),
        usage: ModelUsage {
            input_tokens: Some(17),
            output_tokens: Some(9),
            provenance: UsageProvenance::ProviderReported,
        },
    }];
    calls.push(ModelCallRecord {
        attempt: 2,
        error: None,
        ..calls[0].clone()
    });
    assert_eq!(ModelUsage::total(&calls).input_tokens, Some(34));
    calls[0].usage.output_tokens = None;
    assert_eq!(ModelUsage::total(&calls).output_tokens, None);
    calls[0].usage.input_tokens = Some(u32::MAX);
    assert_eq!(ModelUsage::total(&calls).input_tokens, None);
    assert_eq!(ModelUsage::total(&[]), ModelUsage::default());
}
