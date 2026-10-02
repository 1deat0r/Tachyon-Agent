//! Configuration loading and precedence.
//!
//! Precedence, weakest to strongest:
//!
//! 1. built-in defaults;
//! 2. JSON config file (`--config`, then `TACHYON_CONFIG`, then the platform
//!    default path);
//! 3. environment variables (`TACHYON_LOG_LEVEL`, `TACHYON_DATA_DIR`,
//!    `TACHYON_EVIDENCE_GRACE_MS`);
//! 4. CLI flags.
//!
//! A missing config file is fine (defaults apply). A present-but-malformed
//! file, or a malformed environment value, is an error.

use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use tachyon_gateway::{FAKE_PROVIDER_LABEL, GatewayRuntime};
use tachyon_models::fake::FakeModelProvider;
use tachyon_models::{
    HttpTransport, ModelProvider, OpenAiCompatConfig, OpenAiCompatProvider, TcpHttpTransport,
};
use tachyon_tools::credential::CredentialBroker;
use tachyon_types::ProviderId;

use crate::logging::DEFAULT_LOG_LEVEL;

/// Initial evidence grace window in milliseconds (spec §23 candidate).
pub const DEFAULT_EVIDENCE_GRACE_MS: u64 = 75;

/// Prefix for configuration environment variables.
pub const ENV_PREFIX: &str = "TACHYON_";

/// Errors produced while loading configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Config file could not be read.
    #[error("cannot read config file {path}: {source}", path = .path.display())]
    UnreadableFile {
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
        /// File that could not be read.
        path: PathBuf,
    },
    /// Config file is not valid JSON.
    #[error("invalid JSON in config file {path}: {source}", path = .path.display())]
    InvalidFile {
        /// Underlying JSON failure.
        #[source]
        source: serde_json::Error,
        /// File that could not be parsed.
        path: PathBuf,
    },
    /// An environment variable holds a value of a wrong shape.
    #[error("environment variable {var} holds invalid value {value:?}: {reason}")]
    InvalidEnv {
        /// Variable name.
        var: String,
        /// Offending value.
        value: String,
        /// Why it was rejected.
        reason: String,
    },
    /// The provider section fails validation (unknown kind, missing
    /// required fields, or rejected extras).
    #[error("invalid provider configuration: {reason}")]
    InvalidProvider {
        /// Why the section was rejected.
        reason: String,
    },
    /// `provider.api_key_env` names an environment variable that is not
    /// set. The error carries the variable NAME only, never a value.
    #[error("provider api_key_env names unset environment variable {var}")]
    MissingProviderKey {
        /// The unset environment variable's name.
        var: String,
    },
}

/// Partial configuration as read from a JSON file. Every field is optional;
/// unset fields fall through to the next weaker layer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileConfig {
    /// Log filter, e.g. `"info"` or `"tachyon=debug"`.
    pub log_level: Option<String>,
    /// Directory for runtime state (`state.db`, sockets, artifacts).
    pub data_dir: Option<PathBuf>,
    /// Evidence grace window in milliseconds.
    pub evidence_grace_ms: Option<u64>,
    /// Model provider section (M11 item 5). Env variable NAMES only.
    pub provider: Option<ProviderConfig>,
}

/// Provider section of the config file: how the gateway's model provider
/// is built. The file carries environment variable NAMES only; the key
/// VALUE is resolved at load time into [`Config::provider_key`], which no
/// serialized or debug rendering can show (spec §35).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// Provider kind: `"openai_compat"` or `"fake"` (scripted test/replay).
    pub kind: String,
    /// Base URL. Required for `openai_compat`; ignored for `fake`.
    pub base_url: Option<String>,
    /// Model name. Required for `openai_compat`; defaults for `fake`.
    pub model: Option<String>,
    /// Environment variable NAME holding the API key (optional).
    pub api_key_env: Option<String>,
    /// Permit plaintext `http://` to non-loopback hosts (default false;
    /// SECURITY.md §2.3). Refused at load when absent or false.
    pub allow_insecure_remote: Option<bool>,
    /// Take the completion as server-sent events (default true), so text
    /// reaches the sink as it arrives. Set `false` for a server that
    /// rejects `stream_options`.
    pub stream: Option<bool>,
}

/// A secret resolved from the environment. `Debug` always prints
/// `[REDACTED]`, so no formatting path can leak the value (spec §35).
#[derive(Clone, PartialEq, Eq, Default)]
pub struct SecretValue(String);

impl SecretValue {
    /// Wraps a resolved value.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw value: only for registering with a redaction broker or
    /// handing to a provider transport.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// Strongest layer: explicit CLI flag values. `None` means "flag not given".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CliOverrides {
    /// `--log-level` value, if given.
    pub log_level: Option<String>,
    /// `--data-dir` value, if given.
    pub data_dir: Option<PathBuf>,
    /// `--evidence-grace-ms` value, if given.
    pub evidence_grace_ms: Option<u64>,
}

/// Fully resolved runtime configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// Tracing filter directive.
    pub log_level: String,
    /// Directory for runtime state.
    pub data_dir: PathBuf,
    /// Evidence grace window in milliseconds.
    pub evidence_grace_ms: u64,
    /// Config file actually loaded, if any.
    pub source_file: Option<PathBuf>,
    /// Model provider section resolved from the file layer.
    pub provider: Option<ProviderConfig>,
    /// Key value resolved at load from `provider.api_key_env`.
    /// `serde(skip)` keeps it out of every serialized rendering and
    /// `SecretValue`'s `Debug` prints `[REDACTED]` (spec §35).
    #[serde(skip)]
    pub provider_key: Option<SecretValue>,
}

impl Config {
    /// Loads configuration honoring the documented precedence.
    ///
    /// `explicit_path` is the `--config` value. Environment supplies
    /// `TACHYON_CONFIG` (file path) plus per-field overrides; `cli` wins.
    pub fn load(explicit_path: Option<PathBuf>, cli: CliOverrides) -> Result<Self, ConfigError> {
        let required = explicit_path.is_some() || env::var_os("TACHYON_CONFIG").is_some();
        let path = resolve_config_path(explicit_path);
        let (file, loaded_from) = read_file_config(path.as_deref(), required)?;
        let mut env_map: HashMap<String, String> = env::vars()
            .filter(|(key, _)| key.starts_with(ENV_PREFIX))
            .collect();
        // Provider key resolution happens at load: the VALUE joins the
        // lookup map (so `resolve` stays pure), the NAME stays in the
        // serialized section (spec §35).
        if let Some(name) = file
            .provider
            .as_ref()
            .and_then(|provider| provider.api_key_env.as_deref())
            && let Ok(value) = env::var(name)
        {
            env_map.insert(name.to_owned(), value);
        }
        let mut config = Self::resolve(file, &env_map, cli, loaded_from);
        config.validate_provider()?;
        Ok(config)
    }

    /// Fail-closed validation of the provider section: supported kinds
    /// only, required fields present, and the declared key variable set.
    fn validate_provider(&mut self) -> Result<(), ConfigError> {
        let Some(provider) = self.provider.clone() else {
            return Ok(());
        };
        match provider.kind.as_str() {
            "fake" => Ok(()),
            "openai_compat" => {
                if provider.base_url.as_deref().is_none_or(str::is_empty) {
                    return Err(ConfigError::InvalidProvider {
                        reason: "openai_compat requires base_url".to_owned(),
                    });
                }
                // §2.3: refuse plaintext remote at load, not first request.
                if let Err(error) = OpenAiCompatConfig::validate_base_url(
                    provider.base_url.as_deref().unwrap_or_default(),
                    provider.allow_insecure_remote.unwrap_or(false),
                ) {
                    return Err(ConfigError::InvalidProvider {
                        reason: error.to_string(),
                    });
                }
                if provider.model.as_deref().is_none_or(str::is_empty) {
                    return Err(ConfigError::InvalidProvider {
                        reason: "openai_compat requires model".to_owned(),
                    });
                }
                if let Some(var) = provider.api_key_env.as_ref()
                    && self.provider_key.is_none()
                {
                    return Err(ConfigError::MissingProviderKey { var: var.clone() });
                }
                Ok(())
            }
            other => Err(ConfigError::InvalidProvider {
                reason: format!(
                    "unknown provider kind {other:?}; expected \"openai_compat\" or \"fake\""
                ),
            }),
        }
    }

    /// Builds the gateway runtime (provider + label + model + redaction
    /// registry) for the `tachyon gateway` process from this config —
    /// wiring only, no agent decision logic. No provider section yields
    /// `provider: None`, which the gateway refuses with
    /// `provider_not_configured` before any run starts.
    #[must_use]
    pub fn gateway_runtime(&self) -> GatewayRuntime {
        let mut redactor = CredentialBroker::default();
        if let Some(key) = self.provider_key.as_ref() {
            redactor.register(key.expose().as_bytes(), "provider-api-key");
        }
        let (provider, label, model) = match self.provider.as_ref() {
            None => (None, String::new(), String::new()),
            Some(section) if section.kind == "fake" => (
                Some(std::sync::Arc::new(FakeModelProvider::new(ProviderId(
                    "scripted-replay".into(),
                ))) as std::sync::Arc<dyn ModelProvider>),
                FAKE_PROVIDER_LABEL.to_owned(),
                section
                    .model
                    .clone()
                    .unwrap_or_else(|| "scripted-replay-1".to_owned()),
            ),
            Some(section) => {
                let transport =
                    TcpHttpTransport::new(section.allow_insecure_remote.unwrap_or(false));
                let provider = self.build_openai_compat(section, transport);
                (
                    Some(std::sync::Arc::new(provider) as std::sync::Arc<dyn ModelProvider>),
                    "openai_compat".to_owned(),
                    section.model.clone().unwrap_or_default(),
                )
            }
        };
        GatewayRuntime {
            provider,
            label,
            model,
            redactor,
        }
    }

    /// Builds the `openai_compat` provider for `section` over `transport`,
    /// injecting the key this config resolved at load. This is the ONE
    /// construction site: `gateway_runtime` and the wire-key test share
    /// it, and `gateway_runtime` registers the same
    /// [`Config::provider_key`] bytes with the redaction broker — so the
    /// registered bytes and the transport's `Authorization` header are
    /// the same by construction, never by convention (acp-env-secrets
    /// ticket 03). Generic over the transport so tests can substitute a
    /// recording one; `api_key_env` stays as the fallback for a config
    /// with no resolved key.
    fn build_openai_compat<T: HttpTransport>(
        &self,
        section: &ProviderConfig,
        transport: T,
    ) -> OpenAiCompatProvider<T> {
        let provider = OpenAiCompatProvider::new(
            ProviderId("openai-compat".into()),
            OpenAiCompatConfig {
                base_url: section.base_url.clone().unwrap_or_default(),
                model: section.model.clone().unwrap_or_default(),
                api_key_env: section.api_key_env.clone(),
                request_timeout_ms: 60_000,
                context_window_tokens: 128_000,
                allow_insecure_remote: section.allow_insecure_remote.unwrap_or(false),
                stream: section.stream.unwrap_or(true),
            },
            transport,
        );
        match self.provider_key.as_ref() {
            Some(key) => provider.with_resolved_api_key(key.expose()),
            None => provider,
        }
    }

    /// Pure merge of the layers; `loaded_from` records the file actually read.
    /// Layer order: defaults, then `file`, then `env`, then `cli`.
    #[must_use]
    pub fn resolve(
        file: FileConfig,
        env: &HashMap<String, String>,
        cli: CliOverrides,
        loaded_from: Option<PathBuf>,
    ) -> Self {
        let log_level = cli
            .log_level
            .or_else(|| env.get("TACHYON_LOG_LEVEL").cloned())
            .or(file.log_level)
            .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_owned());
        let data_dir = cli
            .data_dir
            .or_else(|| env.get("TACHYON_DATA_DIR").map(PathBuf::from))
            .or(file.data_dir)
            .unwrap_or_else(default_data_dir);
        // Lenient parse: invalid env values are ignored here; `doctor`
        // surfaces them strictly via `strict_env_grace`.
        let env_grace = env
            .get("TACHYON_EVIDENCE_GRACE_MS")
            .and_then(|raw| raw.parse::<u64>().ok());
        let evidence_grace_ms = cli
            .evidence_grace_ms
            .or(env_grace)
            .or(file.evidence_grace_ms)
            .unwrap_or(DEFAULT_EVIDENCE_GRACE_MS);
        let provider = file.provider;
        let provider_key = provider
            .as_ref()
            .and_then(|section| section.api_key_env.as_deref())
            .and_then(|name| env.get(name))
            .map(SecretValue::new);
        Self {
            log_level,
            data_dir,
            evidence_grace_ms,
            source_file: loaded_from,
            provider,
            provider_key,
        }
    }

    /// Strict variant used when callers want malformed env values to fail.
    /// [`Self::load`] is lenient by design; `doctor` surfaces strict errors.
    pub fn strict_env_grace(env: &HashMap<String, String>) -> Result<Option<u64>, ConfigError> {
        env.get("TACHYON_EVIDENCE_GRACE_MS")
            .map(|raw| {
                raw.parse::<u64>().map_err(|_| ConfigError::InvalidEnv {
                    var: "TACHYON_EVIDENCE_GRACE_MS".to_owned(),
                    value: raw.clone(),
                    reason: "expected a non-negative integer of milliseconds".to_owned(),
                })
            })
            .transpose()
    }

    /// Strictly validates the process environment's `TACHYON_*` values.
    /// Invalid values are errors here even though [`Self::load`] stays lenient.
    pub fn check_process_env() -> Result<(), ConfigError> {
        let env_map: HashMap<String, String> = env::vars()
            .filter(|(key, _)| key.starts_with(ENV_PREFIX))
            .collect();
        Self::strict_env_grace(&env_map).map(|_| ())
    }
}

/// Picks the config file path: explicit flag, then `TACHYON_CONFIG` env,
/// then the platform default. Returns `None` only when unreachable.
fn resolve_config_path(explicit: Option<PathBuf>) -> Option<PathBuf> {
    if let Some(path) = explicit {
        return Some(path);
    }
    if let Some(path) = env::var_os("TACHYON_CONFIG").map(PathBuf::from) {
        return Some(path);
    }
    default_config_path()
}

/// Reads and parses the file at `path`. Missing files yield defaults unless
/// `required` (explicit `--config`/`TACHYON_CONFIG`), in which case absence
/// is an error. Returns the file config and the path actually loaded, if any.
fn read_file_config(
    path: Option<&Path>,
    required: bool,
) -> Result<(FileConfig, Option<PathBuf>), ConfigError> {
    let Some(path) = path else {
        return Ok((FileConfig::default(), None));
    };
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && !required => {
            return Ok((FileConfig::default(), None));
        }
        Err(err) => {
            return Err(ConfigError::UnreadableFile {
                source: err,
                path: path.to_owned(),
            });
        }
    };
    serde_json::from_slice(&bytes)
        .map(|file| (file, Some(path.to_owned())))
        .map_err(|err| ConfigError::InvalidFile {
            source: err,
            path: path.to_owned(),
        })
}

/// Platform config directory: `$XDG_CONFIG_HOME`, else `~/.config`
/// (Windows: `%APPDATA%`).
#[must_use]
pub fn default_config_path() -> Option<PathBuf> {
    config_base_dir().map(|base| base.join("tachyon").join("config.json"))
}

/// Platform data directory: `$XDG_DATA_HOME`, else `~/.local/share/tachyon`
/// (Windows: `%LOCALAPPDATA%/tachyon`).
#[must_use]
pub fn default_data_dir() -> PathBuf {
    data_base_dir().map_or_else(
        || PathBuf::from(".tachyon-data"),
        |base| base.join("tachyon"),
    )
}

fn config_base_dir() -> Option<PathBuf> {
    if let Some(dir) = env::var_os("XDG_CONFIG_HOME").map(PathBuf::from) {
        return Some(dir);
    }
    home_dir().map(|home| home.join(".config"))
}

fn data_base_dir() -> Option<PathBuf> {
    if let Some(dir) = env::var_os("XDG_DATA_HOME").map(PathBuf::from) {
        return Some(dir);
    }
    home_dir().map(|home| home.join(".local").join("share"))
}

#[cfg(unix)]
fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME").map(PathBuf::from)
}

#[cfg(windows)]
fn home_dir() -> Option<PathBuf> {
    env::var_os("USERPROFILE").map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::{CliOverrides, Config, ConfigError, FileConfig};
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn precedence_is_cli_over_env_over_file_over_defaults() {
        let file = FileConfig {
            log_level: Some("warn".to_owned()),
            data_dir: Some(PathBuf::from("/file")),
            evidence_grace_ms: Some(10),
            provider: None,
        };
        let env_map = env(&[
            ("TACHYON_LOG_LEVEL", "debug"),
            ("TACHYON_DATA_DIR", "/env"),
            ("TACHYON_EVIDENCE_GRACE_MS", "20"),
        ]);
        let cli = CliOverrides {
            log_level: Some("error".to_owned()),
            data_dir: None,
            evidence_grace_ms: None,
        };
        let config = Config::resolve(file, &env_map, cli, None);
        assert_eq!(config.log_level, "error");
        assert_eq!(config.data_dir, PathBuf::from("/env"));
        assert_eq!(config.evidence_grace_ms, 20);
    }

    #[test]
    fn file_values_apply_when_env_and_cli_are_silent() {
        let file = FileConfig {
            log_level: Some("warn".to_owned()),
            data_dir: Some(PathBuf::from("/file")),
            evidence_grace_ms: Some(10),
            provider: None,
        };
        let config = Config::resolve(file, &env(&[]), CliOverrides::default(), None);
        assert_eq!(config.log_level, "warn");
        assert_eq!(config.data_dir, PathBuf::from("/file"));
        assert_eq!(config.evidence_grace_ms, 10);
    }

    #[test]
    fn invalid_env_grace_is_reported_strictly() {
        let env_map = env(&[("TACHYON_EVIDENCE_GRACE_MS", "soon")]);
        let err = Config::strict_env_grace(&env_map).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidEnv { .. }));
        assert!(Config::strict_env_grace(&env(&[])).unwrap().is_none());
    }

    #[test]
    fn missing_file_config_yields_defaults() {
        let missing = PathBuf::from("/definitely/not/here/tachyon-config.json");
        let (file, loaded) = super::read_file_config(Some(missing.as_path()), false).unwrap();
        assert_eq!(file, FileConfig::default());
        assert_eq!(loaded, None);
    }

    #[test]
    fn missing_required_file_config_is_an_error() {
        let missing = PathBuf::from("/definitely/not/here/tachyon-config.json");
        let err = super::read_file_config(Some(missing.as_path()), true).unwrap_err();
        assert!(matches!(err, super::ConfigError::UnreadableFile { .. }));
    }
}

// M11 plan item 5 / spec §35 (CLI/config part): the `FileConfig`
// provider section — env NAMES only, fail-closed kind validation,
// resolve-at-load + redaction-registry registration, and the
// gateway-runtime factory the `tachyon gateway` process consumes.
//
// Spec 35: the resolved key value must never appear in any serialized
// or debug rendering of configuration, and a registered key must be
// scrubbed by the existing [`CredentialBroker`] registry before any
// provider error body can reach a client.

// Unit tests for the provider section (integration tests cannot import
// a bin-only crate, so these live beside the config code).
#[cfg(test)]
mod provider_tests {
    use super::{CliOverrides, Config, ConfigError, FileConfig};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tachyon_gateway::FAKE_PROVIDER_LABEL;
    use tachyon_models::{HttpTransport, ModelError, ModelProvider, ModelRequest, Role};

    /// Canned completion a recording transport answers with, so
    /// `invoke` completes instead of erroring after the header is sent.
    const WIRE_COMPLETION: &str = r#"{"choices":[{"message":{"content":"{\"decision\":\"respond\",\"message\":\"wire\"}"}}]}"#;

    /// Ticket 03: captures the key `invoke` puts on the wire — the
    /// app-side twin of the spy in tachyon-models' `openai_compat` tests.
    struct RecordingTransport {
        seen: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        response: String,
    }

    #[async_trait::async_trait]
    impl HttpTransport for RecordingTransport {
        async fn post_json(
            &self,
            _url: &str,
            api_key: Option<&str>,
            _body: &str,
            _timeout_ms: u64,
        ) -> Result<String, ModelError> {
            *self.seen.lock().expect("recording lock") = api_key.map(str::to_owned);
            Ok(self.response.clone())
        }
    }

    fn write_config(json: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tachyon-m11-provider-config-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        std::fs::write(&path, json).unwrap();
        path
    }

    /// A process env var that exists on every CI host (unix and
    /// Windows); stands in for a real API key so this test never
    /// mutates the process environment. (`HOME` is unset on Windows.)
    const PRESENT_KEY_ENV: &str = "PATH";
    /// A unique name guaranteed to be unset: used for the missing-variable
    /// refusal without ever touching the environment.
    const ABSENT_KEY_ENV: &str = "TACHYON_TEST_ABSENT_PROVIDER_KEY_M11_C2";

    /// Pure-resolution harness: the map stands in for the environment,
    /// so the key value is unique and collides with nothing else the
    /// config legitimately renders (paths, levels).
    fn resolve_with_key(value: &str) -> Config {
        let file = FileConfig {
            provider: Some(super::ProviderConfig {
                kind: "openai_compat".to_owned(),
                stream: None,
                base_url: Some("http://127.0.0.1:11434".to_owned()),
                model: Some("llama-3".to_owned()),
                api_key_env: Some("TEST_KEY_ENV_NAME".to_owned()),
                allow_insecure_remote: None,
            }),
            ..FileConfig::default()
        };
        let mut map = HashMap::new();
        map.insert("TEST_KEY_ENV_NAME".to_owned(), value.to_owned());
        Config::resolve(file, &map, CliOverrides::default(), None)
    }

    #[test]
    fn provider_section_survives_and_never_leaks_a_key_value() {
        let secret = "sk-unique-m11-c2-value-9471";
        let config = resolve_with_key(secret);

        let provider = config.provider.as_ref().expect("provider section kept");
        assert_eq!(provider.kind, "openai_compat");
        assert_eq!(provider.base_url.as_deref(), Some("http://127.0.0.1:11434"));
        assert_eq!(provider.model.as_deref(), Some("llama-3"));
        assert_eq!(provider.api_key_env.as_deref(), Some("TEST_KEY_ENV_NAME"));

        // Spec §35: the value resolved, yet no rendering of the config
        // can show it — neither serialized nor debug-formatted.
        assert!(
            config.provider_key.is_some(),
            "the key resolves into the transient slot"
        );
        let serialized = serde_json::to_string(&config).unwrap();
        assert!(
            serialized.contains("TEST_KEY_ENV_NAME"),
            "env NAME is shown"
        );
        assert!(!serialized.contains(secret), "serialized leaked the value");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains(secret), "Debug leaked the value");
        assert!(rendered.contains("[REDACTED]"), "got {rendered}");
    }

    #[test]
    fn load_resolves_a_declared_key_from_the_process_environment() {
        // Presence-only assertion: leak assertions live on the pure
        // path above (PATH legitimately appears in some Debug renderings).
        let json = format!(
            r#"{{"provider":{{"kind":"openai_compat","base_url":"http://127.0.0.1:11434","model":"llama-3","api_key_env":"{PRESENT_KEY_ENV}"}}}}"#
        );
        let path = write_config(&json);
        let config = Config::load(Some(path), CliOverrides::default()).expect("config loads");
        assert!(config.provider_key.is_some(), "PATH resolves at load");
        assert!(format!("{config:?}").contains("[REDACTED]"));
    }

    #[test]
    fn missing_api_key_variable_is_an_error_naming_only_the_variable() {
        let json = format!(
            r#"{{"provider":{{"kind":"openai_compat","base_url":"http://127.0.0.1:11434","model":"llama-3","api_key_env":"{ABSENT_KEY_ENV}"}}}}"#
        );
        let path = write_config(&json);
        let err = Config::load(Some(path), CliOverrides::default()).unwrap_err();
        match &err {
            ConfigError::MissingProviderKey { var } => assert_eq!(var, ABSENT_KEY_ENV),
            other => panic!("expected MissingProviderKey, got {other}"),
        }
        assert!(
            err.to_string().contains(ABSENT_KEY_ENV),
            "error must name the variable"
        );
    }

    #[test]
    fn plaintext_remote_base_url_fails_at_startup() {
        let json = r#"{"provider":{"kind":"openai_compat","base_url":"http://api.example.com:8080","model":"llama-3"}}"#;
        let path = write_config(json);
        let err = Config::load(Some(path), CliOverrides::default())
            .expect_err("non-loopback plaintext http:// must be refused at startup");
        assert!(
            matches!(err, ConfigError::InvalidProvider { .. }),
            "expected InvalidProvider, got {err}"
        );
        assert!(
            err.to_string()
                .contains("refusing plaintext http:// to a non-loopback host"),
            "loopback guard message expected, got {err}"
        );
    }

    #[test]
    fn loopback_plaintext_base_url_is_accepted() {
        let json = r#"{"provider":{"kind":"openai_compat","base_url":"http://localhost:11434","model":"llama-3"}}"#;
        let path = write_config(json);
        let config = Config::load(Some(path), CliOverrides::default())
            .expect("loopback plaintext http:// must stay accepted");
        let provider = config.provider.as_ref().expect("provider section");
        assert_eq!(provider.base_url.as_deref(), Some("http://localhost:11434"));
    }

    #[test]
    fn unknown_provider_kind_fails_closed() {
        let path = write_config(r#"{"provider":{"kind":"anthropic"}}"#);
        let err = Config::load(Some(path), CliOverrides::default()).unwrap_err();
        assert!(
            matches!(err, ConfigError::InvalidProvider { .. }),
            "got {err}"
        );
        assert!(err.to_string().contains("openai_compat"));
    }

    #[test]
    fn openai_compat_without_base_url_and_model_fails_closed() {
        let path = write_config(r#"{"provider":{"kind":"openai_compat"}}"#);
        let err = Config::load(Some(path), CliOverrides::default()).unwrap_err();
        assert!(
            matches!(err, ConfigError::InvalidProvider { .. }),
            "got {err}"
        );
    }

    #[test]
    fn fake_kind_builds_a_scripted_runtime_with_the_plan_label() {
        let path = write_config(r#"{"provider":{"kind":"fake","model":"scripted-replay-1"}}"#);
        let config = Config::load(Some(path), CliOverrides::default()).unwrap();
        let runtime = config.gateway_runtime();
        assert_eq!(runtime.label, FAKE_PROVIDER_LABEL);
        assert!(runtime.provider.is_some(), "fake provider is built");
        assert_eq!(runtime.model, "scripted-replay-1");
    }

    #[test]
    fn none_configured_provider_yields_a_runtime_that_refuses() {
        let path = write_config("{}");
        let config = Config::load(Some(path), CliOverrides::default()).unwrap();
        let runtime = config.gateway_runtime();
        assert!(
            runtime.provider.is_none(),
            "no section: gateway must refuse StartRun with provider_not_configured"
        );
        assert!(config.provider_key.is_none());
    }

    #[test]
    fn resolved_key_is_registered_with_the_redaction_registry() {
        let secret = "sk-unique-m11-c2-value-9471";
        let config = resolve_with_key(secret);
        let runtime = config.gateway_runtime();
        let probe = format!("upstream 401: bearer token {secret} rejected");
        let scrubbed = runtime.redactor.redact(&probe);
        assert!(
            !scrubbed.contains(secret),
            "registered key value still present: {scrubbed}"
        );
        assert!(
            scrubbed.contains("[redacted:provider-api-key"),
            "got {scrubbed}"
        );
    }

    /// Ticket 03 (acp-env-secrets): the app-resolved key is the single
    /// source of truth. One narrative pins the whole chain — resolve at
    /// load, register those exact bytes, build the provider through the
    /// SAME construction site `gateway_runtime` uses, rotate the
    /// process env, invoke — and the transport header still carries the
    /// registered bytes. Rotation is inert until restart; there is no
    /// re-registration machinery (the ticket's contract).
    ///
    /// The chain runs in a self-spawned child that executes exactly
    /// this one test (the `effect_recovery_tests` pattern), because the
    /// unit-test binary is shared with tests that read the process env
    /// concurrently — `set_var` is only sound when this process has no
    /// other env reader, so the mutation never happens in-process here.
    const ROTATION_KEY_ENV: &str = "TACHYON_TEST_PROVIDER_KEY_ROTATION_03";
    const ROTATION_CHILD_ENV: &str = "TACHYON_TEST_PROVIDER_KEY_ROTATION_CHILD_03";
    const REGISTERED_KEY: &str = "sk-registered-at-config-load-03-9471";
    const ROTATED_KEY: &str = "sk-rotated-after-config-load-03-9471";

    #[tokio::test]
    async fn registered_key_is_the_transport_header_after_process_env_rotation() {
        if std::env::var(ROTATION_CHILD_ENV).is_ok() {
            rotation_chain().await;
            std::process::exit(0);
        }
        let mut child = std::process::Command::new(
            std::env::current_exe().expect("test binary path"),
        )
        .args([
            "--exact",
            "config::provider_tests::registered_key_is_the_transport_header_after_process_env_rotation",
            "--nocapture",
        ])
        .env(ROTATION_CHILD_ENV, "1")
        .env(ROTATION_KEY_ENV, REGISTERED_KEY)
        .spawn()
        .expect("rotation child spawns");
        // Bounded wait (the effect-recovery precedent): a wedged child
        // must fail the test, never hang the suite.
        let started = std::time::Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll rotation child") {
                break status;
            }
            if started.elapsed() > std::time::Duration::from_secs(30) {
                let _ = child.kill();
                panic!("rotation chain child timed out");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        assert!(
            status.success(),
            "rotation chain child failed ({status}); its assertions print above"
        );
    }

    /// The chain itself. Runs ONLY in the self-spawned child (env
    /// marker above): that process executes exactly this one test
    /// (`--exact`), so the `set_var` rotation below has no concurrent
    /// environment reader — the SAFETY shape `effect_recovery_tests`,
    /// `mcp_env_isolation`, and `secret_env_allowlist` use.
    #[allow(unsafe_code)]
    async fn rotation_chain() {
        // 1. Resolve at load: the child process starts with the key in
        //    its environment (the parent passed it with `.env`).
        let json = format!(
            r#"{{"provider":{{"kind":"openai_compat","base_url":"http://127.0.0.1:11434","model":"llama-3","api_key_env":"{ROTATION_KEY_ENV}"}}}}"#
        );
        let path = write_config(&json);
        let config =
            Config::load(Some(path), CliOverrides::default()).expect("load resolves the key");
        assert_eq!(
            config.provider_key.as_ref().expect("resolved").expose(),
            REGISTERED_KEY,
            "Config::load must resolve the declared key at load"
        );

        // 2. Register at load: gateway_runtime registers exactly those
        //    bytes with the redaction broker.
        let runtime = config.gateway_runtime();
        let probe = format!("upstream 401: bearer token {REGISTERED_KEY} rejected");
        let scrubbed = runtime.redactor.redact(&probe);
        assert!(
            !scrubbed.contains(REGISTERED_KEY),
            "registered bytes not redacted: {scrubbed}"
        );
        assert!(
            scrubbed.contains("[redacted:provider-api-key"),
            "got {scrubbed}"
        );

        // 3. The provider goes through the SAME construction site
        //    gateway_runtime used, over a recording transport so the
        //    header bytes are observable.
        let section = config.provider.as_ref().expect("provider section").clone();
        let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
        let provider = config.build_openai_compat(
            &section,
            RecordingTransport {
                seen: seen.clone(),
                response: WIRE_COMPLETION.to_owned(),
            },
        );

        // 4. Rotate the process env AFTER load+register — the exact
        //    divergence this ticket closes.
        // SAFETY: sole test in this child process (spawned `--exact`),
        // so nothing else reads the environment concurrently.
        unsafe { std::env::set_var(ROTATION_KEY_ENV, ROTATED_KEY) };

        // 5. The wire header must still be the registered bytes.
        let request = ModelRequest {
            role: Role::Primary,
            model: section.model.clone().unwrap_or_default(),
            context: Vec::new(),
            max_output_tokens: 64,
            require_structured_output: false,
        };
        let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
        provider.invoke(request, sink).await.expect("invoke");
        assert_eq!(
            seen.lock().expect("recording lock").as_deref(),
            Some(REGISTERED_KEY),
            "the rotated env value reached the transport header"
        );
    }
}
