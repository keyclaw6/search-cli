//! Parallel Search v1: `POST /v1/search`. `mode: "advanced"` ($5 per 1,000)
//! for general and deep, `"fast"` ($1 per 1,000) for news, plus $1 per 1,000
//! results above ten. Advanced takes about 1.5–3 s a call, well inside the
//! mode deadlines (25 s general, 50 s deep). v1 rejects unknown fields (`additionalProperties:
//! false`), so the body carries only documented ones; `v1beta` is legacy.
//! https://docs.parallel.ai/api-reference/search/search
//!
//! Extract v1: `POST /v1/extract` ($1 per 1,000 URLs), a rung of the extract
//! chain after the local fetch: it reads pages that need a browser and PDFs.
//! It always asks for the full content; with a goal it also asks for the
//! passages that serve it (`objective`), which a focused read shows.
//! https://docs.parallel.ai/api-reference/extract/extract

use super::perplexity::host_in;
use super::spec::{FilterSupport, Price};
use super::{Adapter, Call, Output, ProviderSpec};
use crate::context::AppContext;
use crate::errors::SearchError;
use crate::types::{FailureCategory, Mode, SearchResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const ENDPOINT: &str = "https://api.parallel.ai/v1/search";
const EXTRACT_ENDPOINT: &str = "https://api.parallel.ai/v1/extract";
const EXCERPT_CHARS: u32 = 1_500;
/// Parallel cuts each `search_queries` entry at this many characters.
const QUERY_MAX: usize = 200;

pub static SPEC: ProviderSpec = ProviderSpec {
    name: "parallel",
    env_keys: &["PARALLEL_API_KEY"],
    modes: &[Mode::General, Mode::News, Mode::Deep, Mode::Extract],
    filters: FilterSupport {
        freshness: true,
        domains: true,
        country: true,
        lang: false,
    },
    max_count: 20,
    // Advanced searches take longer than fast ones; the mode deadline still
    // bounds every call.
    timeout: Duration::from_secs(30),
    billed_on_send: true,
    price: ADVANCED,
};

/// `mode: "fast"`: news.
const FAST: Price = Price {
    per_call: 1_000,
    per_extra_result: 1_000,
    included_results: 10,
};

/// `mode: "advanced"`: general and deep (the owner's choice: it found more
/// original sources than fast in the round-8 test).
const ADVANCED: Price = Price {
    per_call: 5_000,
    per_extra_result: 1_000,
    included_results: 10,
};

/// Extract: one URL, excerpts and full content alike.
const EXTRACT: Price = Price {
    per_call: 1_000,
    per_extra_result: 0,
    included_results: 1,
};

fn price(mode: Mode) -> Price {
    match mode {
        Mode::News => FAST,
        Mode::Extract | Mode::Scrape => EXTRACT,
        _ => ADVANCED,
    }
}

pub struct Parallel {
    ctx: Arc<AppContext>,
}

impl Parallel {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }
}

/// The v1 request: `objective`, `search_queries`, `mode` and
/// `advanced_settings`, nothing else.
fn body(call: &Call<'_>) -> Value {
    let opts = call.opts;
    // include wins upstream (v1 ignores exclude when include is set); the
    // exclude list is then applied to the results in `parse`.
    let mut source_policy = serde_json::Map::new();
    if !opts.include_domains.is_empty() {
        source_policy.insert("include_domains".into(), json!(opts.include_domains));
    } else if !opts.exclude_domains.is_empty() {
        source_policy.insert("exclude_domains".into(), json!(opts.exclude_domains));
    }
    if let Some(days) = opts.freshness.as_deref().and_then(super::freshness_days) {
        source_policy.insert("after_date".into(), json!(super::date_days_ago(days)));
    }
    let mut advanced = json!({
        "max_results": call.count,
        "excerpt_settings": { "max_chars_per_result": EXCERPT_CHARS },
    });
    if !source_policy.is_empty() {
        advanced["source_policy"] = Value::Object(source_policy);
    }
    if let Some(country) = &opts.country {
        advanced["location"] = json!(country.to_ascii_lowercase());
    }
    // With several keyword queries Parallel gets them all in one call, and
    // the goal (when given) as the objective: its own recommended shape.
    let mut queries: Vec<String> = Vec::new();
    for q in std::iter::once(call.query).chain(opts.more_queries.iter().map(String::as_str)) {
        let q = keyword_query(q);
        if !queries.contains(&q) {
            queries.push(q);
        }
    }
    json!({
        "objective": opts.goal.as_deref().unwrap_or(call.query),
        "search_queries": queries,
        "mode": if call.mode == Mode::News { "fast" } else { "advanced" },
        "advanced_settings": advanced,
    })
}

/// The query as the keyword query. Parallel cuts a longer entry at
/// [`QUERY_MAX`] characters (it counts characters, not bytes), with a warning
/// and sometimes mid-word; the whole text still goes in `objective`, which
/// has no such limit, and the keyword query is its first words up to the
/// limit (a single word longer than the limit, such as a pasted URL, is
/// skipped). A short query is sent as is.
fn keyword_query(query: &str) -> String {
    if query.chars().count() <= QUERY_MAX {
        return query.to_string();
    }
    let mut out = String::new();
    for word in query.split_whitespace() {
        let n = word.chars().count();
        if n > QUERY_MAX {
            continue;
        }
        if out.chars().count() + usize::from(!out.is_empty()) + n > QUERY_MAX {
            break;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    if out.is_empty() {
        // One word longer than the limit.
        out = query.chars().take(QUERY_MAX).collect();
    }
    out
}

#[derive(Deserialize)]
struct ParallelResponse {
    #[serde(default)]
    results: Vec<ParallelResult>,
    #[serde(default)]
    warnings: serde_json::Value,
}

#[derive(Deserialize)]
struct ParallelResult {
    url: Option<String>,
    title: Option<String>,
    publish_date: Option<String>,
    excerpts: Option<Vec<String>>,
}

/// Results with an http(s) URL, minus any host on the exclude list when the
/// include list took the source policy.
fn parse(data: ParallelResponse, call: &Call<'_>) -> Output {
    let opts = call.opts;
    let drop: &[String] = if opts.include_domains.is_empty() {
        &[]
    } else {
        &opts.exclude_domains
    };
    let results: Vec<SearchResult> = data
        .results
        .into_iter()
        .filter_map(|r| {
            let url = r.url.filter(|u| u.starts_with("http"))?;
            if host_in(&url, drop) {
                return None;
            }
            Some(SearchResult {
                title: r.title.unwrap_or_default(),
                url,
                snippet: r
                    .excerpts
                    .unwrap_or_default()
                    .join("\n\n")
                    .trim()
                    .to_string(),
                source: "parallel".to_string(),
                published: r.publish_date.filter(|d| !d.is_empty()),
                image_url: None,
                extra: None,
            })
        })
        .collect();
    // Billed on what was asked for: max_results sets the extra-result fee.
    let cost_micros = Some(price(call.mode).estimate(call.count));
    Output {
        results,
        answers: Vec::new(),
        cost_micros,
        notes: super::note_messages(&data.warnings),
    }
}

/// The extract request: the page's full content, and with a goal the
/// passages that serve it, at most [`crate::focus::BUDGET`] characters.
fn extract_body(call: &Call<'_>) -> Value {
    let mut b = json!({
        "urls": [call.query],
        "advanced_settings": { "full_content": true },
    });
    if let Some(goal) = call.opts.goal.as_deref() {
        b["objective"] = json!(goal);
        b["max_chars_total"] = json!(crate::focus::BUDGET);
    }
    b
}

#[derive(Deserialize)]
struct ExtractResponse {
    #[serde(default)]
    results: Vec<ExtractResult>,
    #[serde(default)]
    errors: Vec<ExtractError>,
    #[serde(default)]
    warnings: serde_json::Value,
}

#[derive(Deserialize)]
struct ExtractResult {
    url: Option<String>,
    title: Option<String>,
    publish_date: Option<String>,
    #[serde(default)]
    excerpts: Vec<String>,
    full_content: Option<String>,
}

#[derive(Deserialize)]
struct ExtractError {
    #[serde(default)]
    error_type: String,
    http_status_code: Option<u16>,
    content: Option<String>,
}

/// The page as one result: its full content as the snippet, and with a goal
/// the chosen passages in `extra.excerpts`. A page Parallel could not read
/// (a fetch error, or no content) is a failure, so the chain goes on.
fn parse_extract(data: ExtractResponse, call: &Call<'_>) -> Result<Output, SearchError> {
    let notes = super::note_messages(&data.warnings);
    let page = data.results.into_iter().find(|r| {
        r.full_content
            .as_deref()
            .is_some_and(|c| !c.trim().is_empty())
    });
    let Some(r) = page else {
        let (status, why) = match data.errors.first() {
            Some(e) => (
                e.http_status_code.unwrap_or(0),
                format!(
                    "{}{}",
                    e.error_type,
                    e.content
                        .as_deref()
                        .map(|c| format!(": {}", c.chars().take(200).collect::<String>()))
                        .unwrap_or_default()
                ),
            ),
            None => (200, "no content for this page".to_string()),
        };
        return Err(SearchError::Upstream {
            provider: "parallel",
            status,
            category: FailureCategory::Rejected,
            message: why,
        });
    };
    let mut extra = json!({ "content_type": "text/markdown" });
    if call.opts.goal.is_some() {
        extra["excerpts"] = json!(r.excerpts);
    }
    Ok(Output {
        results: vec![SearchResult {
            title: r.title.unwrap_or_default(),
            url: r
                .url
                .filter(|u| u.starts_with("http"))
                .unwrap_or_else(|| call.query.to_string()),
            snippet: r.full_content.unwrap_or_default(),
            source: "parallel".to_string(),
            published: r.publish_date.filter(|d| !d.is_empty()),
            image_url: None,
            extra: Some(extra),
        }],
        answers: Vec::new(),
        cost_micros: Some(EXTRACT.estimate(1)),
        notes,
    })
}

#[async_trait]
impl Adapter for Parallel {
    fn spec(&self) -> &'static ProviderSpec {
        &SPEC
    }

    fn keys(&self) -> Vec<String> {
        super::keys_for(&self.ctx.config, &SPEC)
    }

    fn is_configured(&self) -> bool {
        !self.keys().is_empty()
    }

    /// Parallel's error reference: 402 is "insufficient credit in account"
    /// (it carries no code). Its 429 means "rate limited or quota exceeded",
    /// which cannot be told apart, so it never marks a key.
    fn spent(&self, status: u16, _body: &str) -> bool {
        status == 402
    }

    fn sent_cost(&self, mode: Mode) -> u64 {
        price(mode).estimate(0)
    }

    fn takes_several_queries(&self) -> bool {
        true
    }

    async fn call(&self, call: &Call<'_>) -> Result<Output, SearchError> {
        if !self.is_configured() {
            return Err(SearchError::AuthMissing {
                provider: "parallel",
            });
        }
        let client = &self.ctx.client;
        let api_key = call.key;
        if matches!(call.mode, Mode::Extract | Mode::Scrape) {
            crate::registry::rung_gate("parallel", call.query)?;
            let body = extract_body(call);
            let resp = super::send(self, call.timeout, || {
                client
                    .post(EXTRACT_ENDPOINT)
                    .header("x-api-key", api_key)
                    .json(&body)
            })
            .await?;
            return parse_extract(resp.json().await?, call);
        }
        let body = body(call);
        let resp = super::send(self, call.timeout, || {
            client
                .post(ENDPOINT)
                .header("x-api-key", api_key)
                .json(&body)
        })
        .await?;
        let data: ParallelResponse = resp.json().await?;
        Ok(parse(data, call))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::types::SearchOpts;

    fn call<'a>(mode: Mode, count: u16, opts: &'a SearchOpts) -> Call<'a> {
        Call {
            mode,
            query: "rust async runtime",
            count,
            opts,
            timeout: Duration::from_secs(5),
            key: "",
        }
    }

    fn fixture() -> ParallelResponse {
        serde_json::from_str(include_str!("../../tests/fixtures/parallel/search_v1.json")).unwrap()
    }

    #[test]
    fn request_uses_the_v1_field_layout_only() {
        let opts = SearchOpts {
            exclude_domains: vec!["pinterest.com".into()],
            freshness: Some("week".into()),
            country: Some("GB".into()),
            ..SearchOpts::default()
        };
        let b = body(&call(Mode::General, 15, &opts));
        let mut top: Vec<&str> = b.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        top.sort();
        assert_eq!(
            top,
            ["advanced_settings", "mode", "objective", "search_queries"]
        );
        assert_eq!(b["mode"], "advanced");
        assert_eq!(body(&call(Mode::News, 10, &opts))["mode"], "fast");
        assert_eq!(b["search_queries"], json!(["rust async runtime"]));
        let a = &b["advanced_settings"];
        let mut keys: Vec<&str> = a.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "excerpt_settings",
                "location",
                "max_results",
                "source_policy"
            ]
        );
        assert_eq!(a["max_results"], 15);
        assert_eq!(a["excerpt_settings"]["max_chars_per_result"], 1_500);
        assert_eq!(a["location"], "gb");
        assert_eq!(
            a["source_policy"]["exclude_domains"],
            json!(["pinterest.com"])
        );
        assert_eq!(
            a["source_policy"]["after_date"],
            json!(super::super::date_days_ago(7))
        );
    }

    #[test]
    fn deep_sends_advanced_and_include_takes_the_source_policy() {
        let opts = SearchOpts {
            include_domains: vec!["docs.rs".into()],
            exclude_domains: vec!["github.com".into()],
            ..SearchOpts::default()
        };
        let b = body(&call(Mode::Deep, 10, &opts));
        assert_eq!(b["mode"], "advanced");
        let sp = &b["advanced_settings"]["source_policy"];
        assert_eq!(sp["include_domains"], json!(["docs.rs"]));
        assert!(sp.get("exclude_domains").is_none());
        assert!(b["advanced_settings"].get("location").is_none());
    }

    /// Parallel's warnings are kept for the search log, never as results.
    #[test]
    fn warnings_become_notes() {
        let opts = SearchOpts::default();
        let data: ParallelResponse = serde_json::from_str(
            r#"{"results":[],"warnings":[{"type":"input_validation_warning","message":"1 of 1 search_queries were truncated to the maximum allowed length of 200 characters.","detail":null}]}"#,
        )
        .unwrap();
        let out = parse(data, &call(Mode::General, 10, &opts));
        assert_eq!(
            out.notes,
            ["1 of 1 search_queries were truncated to the maximum allowed length of 200 characters."]
        );
        assert!(parse(fixture(), &call(Mode::General, 10, &opts))
            .notes
            .is_empty());
        // An odd warnings value never fails the results.
        for odd in [
            r#"null"#,
            r#""plain text""#,
            r#"{"x":1}"#,
            r#"[1, {"message": 2}]"#,
        ] {
            let data: ParallelResponse =
                serde_json::from_str(&format!(r#"{{"results":[],"warnings":{odd}}}"#)).unwrap();
            let _ = parse(data, &call(Mode::General, 10, &opts));
        }
    }

    /// Several keyword queries go in one call, each at most 200 characters
    /// and none twice, with the goal as the objective.
    #[test]
    fn several_queries_go_in_one_call_with_the_goal() {
        let opts = SearchOpts {
            goal: Some("Which Gemini model topped Artificial Analysis in November 2026?".into()),
            more_queries: vec!["Gemini 4 Carbon index".into(), "rust async runtime".into()],
            ..SearchOpts::default()
        };
        let b = body(&call(Mode::General, 10, &opts));
        assert_eq!(
            b["objective"],
            "Which Gemini model topped Artificial Analysis in November 2026?"
        );
        assert_eq!(
            b["search_queries"],
            json!(["rust async runtime", "Gemini 4 Carbon index"])
        );
        let p = Parallel::new(Arc::new(AppContext::new(AppConfig::default())));
        assert!(p.takes_several_queries());
    }

    /// A query over 200 characters goes whole into `objective`, and its
    /// first words up to 200 characters are the keyword query, so Parallel
    /// cuts nothing; a short query is sent unchanged.
    #[test]
    fn a_long_query_is_never_cut() {
        let opts = SearchOpts::default();
        let long = "Google announced a new Gemini model on 28 October 2026. What is it called, \
            and what score and rank did Artificial Analysis give it on its Intelligence Index v5, \
            and how does that compare with the previous number one?";
        assert!(long.chars().count() > QUERY_MAX);
        let mut c = call(Mode::General, 10, &opts);
        c.query = long;
        let b = body(&c);
        assert_eq!(b["objective"], long);
        let kw = b["search_queries"][0].as_str().unwrap();
        assert!(kw.chars().count() <= QUERY_MAX, "{kw}");
        assert!(
            long.starts_with(kw) && long[kw.len()..].starts_with(' '),
            "cut at a word: {kw}"
        );
        let short = "rust async runtime";
        c.query = short;
        assert_eq!(body(&c)["search_queries"], json!([short]));
        let one_word = "x".repeat(250);
        assert_eq!(keyword_query(&one_word).len(), QUERY_MAX);
        // Characters, not bytes; an over-long word is skipped, not the rest.
        let danish = format!("{} ord", "æ".repeat(250));
        assert_eq!(keyword_query(&danish), "ord");
        let url = format!("compare https://example.com/{} with that", "a".repeat(260));
        assert_eq!(keyword_query(&url), "compare with that");
        let wide = "smørrebrød på Nørrebro ".repeat(12);
        let kw = keyword_query(&wide);
        assert!(
            kw.chars().count() <= QUERY_MAX && kw.len() > QUERY_MAX,
            "{kw}"
        );
    }

    #[test]
    fn parses_v1_results_with_dates_and_drops_bad_urls() {
        let opts = SearchOpts::default();
        let out = parse(fixture(), &call(Mode::General, 10, &opts));
        assert_eq!(out.results.len(), 2, "the non-http row is dropped");
        let r = &out.results[0];
        assert_eq!(r.url, "https://tokio.rs/blog/2026-10-tokio-1-48");
        assert_eq!(r.published.as_deref(), Some("2026-10-21"));
        assert!(r.snippet.contains("\n\n"), "excerpts are joined");
        assert_eq!(out.results[1].published, None);
    }

    #[test]
    fn exclude_list_is_applied_locally_when_include_wins_upstream() {
        let opts = SearchOpts {
            include_domains: vec!["tokio.rs".into(), "github.com".into()],
            exclude_domains: vec!["github.com".into()],
            ..SearchOpts::default()
        };
        let out = parse(fixture(), &call(Mode::General, 10, &opts));
        assert!(out.results.iter().all(|r| !r.url.contains("github.com")));
        assert_eq!(out.results.len(), 1);
    }

    #[test]
    fn cost_follows_mode_and_requested_count() {
        let opts = SearchOpts::default();
        let p = Parallel::new(Arc::new(AppContext::new(AppConfig::default())));
        assert_eq!(
            parse(fixture(), &call(Mode::General, 10, &opts)).cost_micros,
            Some(5_000)
        );
        assert_eq!(
            parse(fixture(), &call(Mode::News, 20, &opts)).cost_micros,
            Some(11_000)
        );
        assert_eq!(
            parse(fixture(), &call(Mode::Deep, 10, &opts)).cost_micros,
            Some(5_000)
        );
        assert_eq!(
            parse(fixture(), &call(Mode::Deep, 15, &opts)).cost_micros,
            Some(10_000)
        );
        assert_eq!(p.sent_cost(Mode::General), 5_000);
        assert_eq!(p.sent_cost(Mode::News), 1_000);
        assert_eq!(p.sent_cost(Mode::Deep), 5_000);
    }

    /// Extract asks for the full content always, and with a goal for the
    /// passages that serve it; the full content is the page.
    #[test]
    fn extract_reads_the_whole_page_and_with_a_goal_its_passages() {
        let url = "https://www.energinet.dk/media/x/udbud.pdf";
        let plain = SearchOpts::default();
        let mut c = call(Mode::Extract, 1, &plain);
        c.query = url;
        let b = extract_body(&c);
        assert_eq!(b["urls"], json!([url]));
        assert_eq!(b["advanced_settings"]["full_content"], true);
        assert!(b.get("objective").is_none());
        let focused = SearchOpts {
            goal: Some("FCR delivery duration".into()),
            ..SearchOpts::default()
        };
        let mut c = call(Mode::Extract, 1, &focused);
        c.query = url;
        let b = extract_body(&c);
        assert_eq!(b["objective"], "FCR delivery duration");
        assert_eq!(b["max_chars_total"], crate::focus::BUDGET);

        let data: ExtractResponse = serde_json::from_value(json!({
            "extract_id": "e", "session_id": "s", "errors": [],
            "results": [{"url": url, "title": "Udbud", "publish_date": "2026-11-02",
                         "excerpts": ["part one\n...\npart two"], "full_content": "# Udbud\n\nWhole text."}]
        }))
        .unwrap();
        let out = parse_extract(data, &c).unwrap();
        let r = &out.results[0];
        assert_eq!(r.snippet, "# Udbud\n\nWhole text.");
        assert_eq!(
            r.extra.as_ref().unwrap()["excerpts"],
            json!(["part one\n...\npart two"])
        );
        assert_eq!(out.cost_micros, Some(1_000));
        let p = Parallel::new(Arc::new(AppContext::new(AppConfig::default())));
        assert_eq!(p.sent_cost(Mode::Extract), 1_000);
    }

    /// A page Parallel could not read is a failure, so the chain goes on.
    #[test]
    fn an_unread_page_is_a_failure() {
        let opts = SearchOpts::default();
        let c = call(Mode::Extract, 1, &opts);
        let data: ExtractResponse = serde_json::from_value(json!({
            "results": [],
            "errors": [{"url": "u", "error_type": "fetch_error", "http_status_code": 500, "content": "boom"}]
        }))
        .unwrap();
        let err = parse_extract(data, &c).unwrap_err();
        assert_eq!(err.category(), FailureCategory::Rejected);
        assert!(err.to_string().contains("fetch_error: boom"), "{err}");
        let empty: ExtractResponse = serde_json::from_value(
            json!({"results": [{"url": "u", "excerpts": [], "full_content": " "}]}),
        )
        .unwrap();
        assert!(parse_extract(empty, &c).is_err());
    }
}
