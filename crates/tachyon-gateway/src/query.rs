//! Deterministic repository lookup for a user question.
//!
//! This is the handoff's "connect repository intelligence and
//! deterministic routing to actual user requests" slice: a question such
//! as *"Where is `complete_refresh` defined and used?"* is routed by the
//! classifier, answered from a freshly built `tachyon-repo` index, and
//! returned as source locations — with **zero model calls by
//! construction**. The function below never sees a provider, a task, or a
//! journal, so no route can quietly escalate into inference: a plan that
//! would need one is refused with `requires_model` instead of being
//! degraded into a lookup.
//!
//! Freshness follows the repository's own contract — *"watchers are hints
//! — content hashes are authoritative truth"*: every request rescans the
//! workspace (BLAKE3 per file) and rebuilds the index from that
//! inventory, so locations are never older than the request. The scan is
//! the dominant cost (~6 ms over 441 files measured in M13, ~10 ms for the
//! warm lookup, against a 250 ms budget); a verified index cache is the
//! obvious follow-up once a cache-invalidation rule is worth its weight.

use std::path::Path;

use serde_json::{Value, json};
use tachyon_repo::{
    Inventory, SearchOptions, SymbolIndex, language::HeuristicBackend, lexical_search,
};
use tachyon_router::{RouteClass, Router, requested_symbol};

/// Largest workspace this command will index in one request. Matches the
/// Class A leg's scan budget so a huge tree fails closed instead of
/// blocking the gateway.
const SCAN_LIMIT: usize = 10_000;

/// Largest lexical-hit list returned alongside the symbol answer.
const SEARCH_LIMIT: usize = 200;

/// Longest question this command accepts; anything longer is not a
/// lookup.
const MAX_QUESTION_BYTES: usize = 4_096;

/// A refused query: a stable machine-readable code plus a human message.
pub(crate) struct Refusal {
    /// Code the protocol reports for this refusal.
    pub(crate) code: &'static str,
    /// Human-readable explanation.
    pub(crate) message: String,
}

impl Refusal {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Answers `question` over `workspace_root`, or refuses with a typed code.
pub(crate) fn answer(workspace_root: &Path, question: &str) -> Result<Value, Refusal> {
    let question = question.trim();
    if question.is_empty() {
        return Err(Refusal::new(
            "invalid_question",
            "the question is empty; ask e.g. \"Where is <symbol> defined and used?\"",
        ));
    }
    if question.len() > MAX_QUESTION_BYTES {
        return Err(Refusal::new(
            "invalid_question",
            format!(
                "question is {} bytes; the lookup bound is {MAX_QUESTION_BYTES}",
                question.len()
            ),
        ));
    }

    // Deterministic routing first: anything that would need a model (or a
    // judge) is refused here, before any index work, so "zero model
    // calls" is a structural property of this command rather than a
    // property of the answer we happened to get.
    let mut router = Router::new();
    let plan = router.route(question);
    match plan.class {
        RouteClass::DirectNative | RouteClass::EvidenceFirst => {}
        other => {
            return Err(Refusal::new(
                "requires_model",
                format!(
                    "\"{question}\" routes to {}, which is not a deterministic lookup; \
                     use `tachyon run` for reasoning work",
                    other.name()
                ),
            ));
        }
    }
    if plan.requires_model() {
        return Err(Refusal::new(
            "requires_model",
            format!("\"{question}\" plans a model call; refusing to answer it here"),
        ));
    }

    // Bind the symbol the question asks about. No candidate and no cue
    // means we have nothing to look up, and we say so instead of
    // returning an unrelated empty answer.
    let symbol = requested_symbol(question).ok_or_else(|| {
        Refusal::new(
            "no_symbol",
            "no symbol could be extracted from the question; \
             ask e.g. \"Where is <symbol> defined and used?\"",
        )
    })?;

    // Fresh inventory, fresh index: content hashes decide what changed.
    let inventory = Inventory::scan(workspace_root, SCAN_LIMIT)
        .map_err(|error| Refusal::new("index_failed", error.to_string()))?;
    let mut index = SymbolIndex::new(workspace_root, HeuristicBackend);
    index.build(&inventory);

    let answer = index.definition_use(&symbol);
    let lexical_hits = lexical_search(
        workspace_root,
        &inventory,
        index.projection(),
        &symbol,
        &SearchOptions {
            limit: SEARCH_LIMIT,
            ..SearchOptions::default()
        },
    );
    let found = !answer.definitions.is_empty() || !answer.references.is_empty();
    if !found && lexical_hits.is_empty() {
        return Err(Refusal::new(
            "not_found",
            format!(
                "no definitions, references or matches for `{symbol}` in {}",
                workspace_root.display()
            ),
        ));
    }

    Ok(json!({
        "question": question,
        "symbol": answer.name,
        "route_class": plan.class.name(),
        "route_confidence": plan.confidence,
        // The contract this command exists to keep: no provider was
        // constructed, so no call could have been made.
        "model_calls": 0,
        "found": found,
        "definitions": answer.definitions,
        "references": answer.references,
        "lexical_hits": lexical_hits.len(),
        "files_indexed": inventory.files.len(),
        "index_generation": index.generation,
        "workspace_root": workspace_root.display().to_string(),
    }))
}
