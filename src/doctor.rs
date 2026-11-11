//! `search doctor` — test-fires every configured provider with a minimal
//! real request and reports health, latency, and failure cause. This is the
//! preflight an agent fleet runs before trusting the tool: `config check`
//! says a key exists; doctor says it works. Each check is one billed
//! (minimal) request per key, so a spent fallback key shows; scrape-only
//! providers are probed against example.com.
//! Providers you have no key for are listed but not probed.

use crate::context::AppContext;
use crate::errors::redact_secrets;
use crate::providers::{self, Call};
use crate::types::{Mode, SearchOpts};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time::timeout;

const PROBE_QUERY: &str = "health";
const PROBE_URL: &str = "https://example.com/";

#[derive(Debug, Serialize)]
pub struct Check {
    pub provider: String,
    pub configured: bool,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// Which of the provider's keys this row probed (1 = the first), when
    /// it has more than one: each key is probed, so a spent one shows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<usize>,
    /// direct | local | none: how a search would reach it.
    pub transport: String,
}

/// The cheapest mode that exercises the provider: general search where it
/// has one, else its first mode (extract-only providers fetch example.com).
fn probe_mode(modes: &[Mode]) -> Mode {
    if modes.contains(&Mode::General) {
        Mode::General
    } else {
        modes.first().copied().unwrap_or(Mode::General)
    }
}

pub async fn run(ctx: Arc<AppContext>) -> Vec<Check> {
    let mut set: JoinSet<Check> = JoinSet::new();
    let mut out = Vec::new();

    for provider in providers::build_providers(&ctx) {
        let name = provider.name();
        if !provider.is_configured() {
            // Not probed: without a key it does not run.
            out.push(Check {
                provider: name.to_string(),
                configured: false,
                ok: true,
                latency_ms: None,
                results: None,
                error: None,
                category: None,
                key: None,
                transport: "none".to_string(),
            });
            continue;
        }
        let transport = provider.transport();
        let keys = provider.keys();
        let provider: Arc<dyn providers::Adapter> = Arc::from(provider);
        // A keyless provider is probed once, with no key.
        let slots: Vec<(Option<usize>, String)> = match keys.len() {
            0 => vec![(None, String::new())],
            1 => vec![(None, keys.into_iter().next().unwrap_or_default())],
            _ => keys
                .into_iter()
                .enumerate()
                .map(|(i, k)| (Some(i + 1), k))
                .collect(),
        };
        for (slot, key) in slots {
            let provider = provider.clone();
            set.spawn(async move { probe(provider.as_ref(), name, slot, &key, transport).await });
        }
    }

    // A panicked probe is skipped, not the end of the report.
    while let Some(joined) = set.join_next().await {
        if let Ok(check) = joined {
            out.push(check);
        }
    }
    out.sort_by(|a, b| a.provider.cmp(&b.provider).then(a.key.cmp(&b.key)));
    out
}

/// One minimal real request with one key.
async fn probe(
    provider: &dyn providers::Adapter,
    name: &'static str,
    slot: Option<usize>,
    key: &str,
    transport: crate::types::Transport,
) -> Check {
    let spec = provider.spec();
    let mode = probe_mode(spec.modes);
    let query = if registry_input_is_url(mode) {
        PROBE_URL
    } else {
        PROBE_QUERY
    };
    let opts = SearchOpts::default();
    let call = Call {
        mode,
        query,
        count: 1,
        opts: &opts,
        timeout: spec.timeout,
        key,
    };
    let start = Instant::now();
    let outcome = timeout(spec.timeout + Duration::from_secs(1), provider.call(&call)).await;
    let latency = start.elapsed().as_millis();
    let row =
        |ok: bool, results: Option<usize>, category: Option<String>, error: Option<String>| Check {
            provider: name.to_string(),
            configured: true,
            ok,
            latency_ms: Some(latency),
            results,
            error,
            category,
            key: slot,
            transport: transport.as_str().to_string(),
        };
    match outcome {
        Ok(Ok(output)) => row(true, Some(output.results.len()), None, None),
        Ok(Err(e)) => row(
            false,
            None,
            Some(e.category().as_str().to_string()),
            Some(truncate(&redact_secrets(&e.to_string()), 160)),
        ),
        Err(_) => row(
            false,
            None,
            Some("timeout".to_string()),
            Some("timed out".to_string()),
        ),
    }
}

fn registry_input_is_url(mode: Mode) -> bool {
    crate::registry::spec(mode).input == crate::registry::InputKind::Url
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect::<String>() + "…"
    }
}

pub fn render_human(checks: &[Check]) {
    use crate::output::style::Paint;
    eprintln!("\n{}  provider health\n", "doctor".bold().cyan());
    for c in checks {
        let label = match c.key {
            Some(k) => format!("{} key {k}", c.provider),
            None => c.provider.clone(),
        };
        if !c.configured {
            println!("  {:<12} {}", c.provider, "not configured".dimmed());
        } else if c.ok {
            println!(
                "  {:<12} {} {:>5}ms  {} result(s)",
                label.bold(),
                "ok".green(),
                c.latency_ms.unwrap_or(0),
                c.results.unwrap_or(0)
            );
        } else {
            println!(
                "  {:<12} {} [{}] {}",
                label.bold(),
                "FAIL".red(),
                c.category.as_deref().unwrap_or("?"),
                c.error.as_deref().unwrap_or("")
            );
        }
    }
    println!();
}
