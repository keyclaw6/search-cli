//! One executor for every mode. The registry names a mode's providers, its
//! merge kind and its deadline; the engine runs them:
//! - **fused**: every provider at once, all collected until they finish or
//!   the deadline passes, then reciprocal-rank fusion;
//! - **chain**: in order until one returns content the mode accepts;
//! - **single**: one call.
//!
//! A call that has been sent is paid for, so nothing is cancelled before the
//! mode deadline; whatever is still running then is reported in
//! `metadata.providers_timed_out`.

use crate::context::AppContext;
use crate::errors::{redact_secrets, SearchError};
use crate::providers::{self, Adapter, Call, FilterSupport, Output};
use crate::registry::{self, MergeKind};
use crate::spent;
use crate::types::{
    Answer, CostSummary, FailureCategory, Mode, ProviderFailure, ResponseMetadata, ResponseStatus,
    SearchOpts, SearchResponse, SearchResult, ENVELOPE_VERSION,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

/// Reciprocal-rank-fusion constant (standard k=60): dampens the gap between
/// rank 1 and rank 2 so cross-provider consensus outweighs any single
/// provider's top pick.
const RRF_K: f64 = 60.0;

/// Main entry point (called by main.rs): run `mode` over the configured
/// providers within the mode's deadline. Each provider runs with your own key
/// (direct) or keyless on this machine (local).
pub async fn run(
    ctx: Arc<AppContext>,
    query: &str,
    mode: Mode,
    count: usize,
    only_providers: &Option<Vec<String>>,
    opts: &SearchOpts,
) -> Result<SearchResponse, SearchError> {
    let store = spent::Store::load(spent::default_path());
    let request = Request {
        query,
        mode,
        count,
        only: only_providers.as_deref(),
        opts,
        deadline: registry::spec(mode).deadline,
        straggler_cap: registry::spec(mode).straggler_cap,
        spent: Some(&store),
    };
    execute_with(providers::build_providers(&ctx), &request).await
}

/// One search, independent of where adapters come from (tests pass fakes).
pub(crate) struct Request<'a> {
    pub query: &'a str,
    pub mode: Mode,
    pub count: usize,
    pub only: Option<&'a [String]>,
    pub opts: &'a SearchOpts,
    pub deadline: Duration,
    /// Fused: stop waiting this long in once one provider has answered
    /// (the registry's `straggler_cap`).
    pub straggler_cap: Option<Duration>,
    /// Keys marked out of credit, tried last; `None` remembers nothing.
    pub spent: Option<&'a spent::Store>,
}

/// What one provider call produced.
enum Outcome {
    Done(Output),
    /// A 2xx the mode would not accept (chain); paid for all the same.
    Rejected(ProviderFailure, Output),
    Failed(ProviderFailure),
    /// Still running at the mode deadline.
    TimedOut,
}

/// Which of a provider's own keys a call went out with. A key refused for
/// its own reasons (auth, credit, rate limit) hands the call to the next
/// one; `metadata.key_slots` names the key that answered when there are
/// several, and a warning names each key refused before it. A key the
/// provider answered as out of credit is remembered (`spent.rs`) and tried
/// last; `metadata.out_of_credit_keys` names it with both dates.
#[derive(Debug, Default)]
struct KeyUse {
    /// How many keys the provider has (0: keyless).
    of: usize,
    /// How many keys were sent.
    tried: usize,
    /// The key the outcome came from, by its place in the list (1 = the first).
    last: usize,
    /// Keys refused before it, with why.
    refused: Vec<(usize, ProviderFailure)>,
    /// The place of a key that answered while marked out of credit.
    answered_marked: Option<usize>,
    /// The first answer had no results, so the call was sent once more.
    asked_again: bool,
}

impl KeyUse {
    /// One failure for the provider that names every key it went through.
    fn failure(&self, provider: &str, mut last: ProviderFailure) -> ProviderFailure {
        if self.refused.is_empty() {
            return last;
        }
        // "Out of credit" in words, so the provider's raw tag is not the
        // only sign of it.
        let part = |slot: &usize, f: &ProviderFailure| {
            if f.code == "credits_exhausted" {
                format!("key {slot} (out of credit): {}", f.reason)
            } else {
                format!("key {slot}: {}", f.reason)
            }
        };
        let earlier = self
            .refused
            .iter()
            .map(|(slot, f)| part(slot, f))
            .collect::<Vec<_>>()
            .join("; ");
        let tail = part(&self.last, &last);
        let label = if last.code == "credits_exhausted" {
            " (out of credit)"
        } else {
            ""
        };
        let stopped = format!(
            "key {} of {}{label}: {}; refused before it: {earlier}",
            self.last, self.of, last.reason
        );
        // The cause that leads is the one a user can act on: anything that
        // is not the key's own fault (a rate limit, a timeout, …) first, then
        // a rejected key (401), then "out of credit", which marked keys going
        // last often repeat. So the owner is told only when every key's cause
        // is the owner's (`errors::owner_note`).
        let rank = |f: &ProviderFailure| {
            if f.code == "credits_exhausted" {
                2
            } else if f.category == FailureCategory::Auth && f.http_status == Some(401) {
                1
            } else {
                0
            }
        };
        let lead = std::iter::once(&last)
            .chain(self.refused.iter().rev().map(|(_, f)| f))
            .min_by_key(|f| rank(f))
            .map(|f| (f.category, f.code.clone(), f.http_status, f.retryable));
        if let Some((category, code, http_status, retryable)) = lead {
            last.category = category;
            last.code = code;
            last.http_status = http_status;
            last.retryable = retryable;
        }
        last.reason = if self.tried == self.of {
            format!("all {} {provider} keys failed: {earlier}; {tail}", self.of)
        } else {
            stopped
        };
        last
    }
}

/// One key in the order a call tries them. No `Debug`: it holds the key.
#[derive(Clone)]
struct Slot {
    /// Its place in the provider's list (1 = the first; 0: keyless).
    place: usize,
    key: String,
    fingerprint: String,
    /// Marked out of credit: tried after every other key.
    marked: bool,
}

/// The provider's keys in list order, the ones marked out of credit moved
/// to the end; one empty key for a keyless or hosted adapter.
fn key_order(adapter: &dyn Adapter, store: Option<&spent::Store>, now: u64) -> Vec<Slot> {
    let keys = adapter.keys();
    if keys.is_empty() {
        return vec![Slot {
            place: 0,
            key: String::new(),
            fingerprint: String::new(),
            marked: false,
        }];
    }
    let mut order: Vec<Slot> = keys
        .into_iter()
        .enumerate()
        .map(|(i, key)| {
            let fingerprint = spent::fingerprint(&key);
            Slot {
                place: i + 1,
                marked: store
                    .is_some_and(|s| s.active(adapter.name(), &fingerprint, now).is_some()),
                key,
                fingerprint,
            }
        })
        .collect();
    order.sort_by_key(|s| s.marked);
    order
}

pub(crate) async fn execute_with(
    adapters: Vec<Box<dyn Adapter>>,
    req: &Request<'_>,
) -> Result<SearchResponse, SearchError> {
    let start = Instant::now();
    let deadline = start + req.deadline;
    // `scrape` is an alias: route it exactly like `extract`.
    let mode = match req.mode {
        Mode::Scrape => Mode::Extract,
        m => m,
    };
    let mode_spec = registry::spec(mode);
    let mut plan = plan(
        adapters,
        mode,
        &registry::providers_for(mode, req.query),
        req.only,
    )?;
    if plan.is_empty() {
        return Err(no_providers_error(mode, req.only));
    }
    let mut warnings: Vec<String> = Vec::new();
    // Social with no platform cue runs the rule pick (two paid calls at
    // most).
    if req.only.is_none() {
        if let Some(cap) = registry::fan_out_cap(mode, req.query) {
            plan.truncate(cap);
        }
    }

    let names: Vec<&'static str> = plan.iter().map(|a| a.name()).collect();
    // Each provider's keys, for reporting the ones marked out of credit.
    let plan_keys: Vec<(&'static str, Vec<String>)> =
        plan.iter().map(|a| (a.name(), a.keys())).collect();
    // What the calls learn about keys marked out of credit, as they learn it.
    let learned = Arc::new(Mutex::new(spent::Changes::default()));
    // The filters each call really sends.
    let applied: HashMap<&'static str, FilterSupport> = plan
        .iter()
        .map(|a| (a.name(), a.applied_filters(req.opts)))
        .collect();
    // What a sent-but-unanswered call costs in this mode: billed providers
    // bill on send. Taken now, before the adapters move into their tasks.
    let sent_costs: HashMap<&'static str, u64> = plan
        .iter()
        .filter(|a| a.spec().billed_on_send)
        .map(|a| (a.name(), a.sent_cost(mode)))
        .collect();
    let mut transport: BTreeMap<String, String> = plan
        .iter()
        .map(|a| (a.name().to_string(), a.transport().as_str().to_string()))
        .collect();

    let mut outcomes: Vec<(&'static str, Outcome, KeyUse)> = match mode_spec.merge {
        MergeKind::Fused => {
            let cap = req.straggler_cap.map(|c| start + c);
            run_fused(plan, req, &learned, mode, deadline, cap).await
        }
        MergeKind::Chain | MergeKind::Single => {
            run_chain(plan, req, &learned, mode, deadline).await
        }
    };
    // Plan order, not completion order, so fusion ties break the same way
    // whichever provider answered first.
    outcomes.sort_by_key(|(n, _, _)| names.iter().position(|x| x == n));

    let mut buckets: Vec<(String, Vec<SearchResult>)> = Vec::new();
    let mut answers: Vec<Answer> = Vec::new();
    let mut failures: Vec<ProviderFailure> = Vec::new();
    let mut timed_out: Vec<String> = Vec::new();
    let mut by_provider: BTreeMap<String, u64> = BTreeMap::new();
    let mut queried: Vec<String> = Vec::new();
    let mut key_slots: BTreeMap<String, usize> = BTreeMap::new();
    // For maintainers (the search log), never printed: what providers said
    // beside their results, and what the tool did about it.
    let mut diagnostics: Vec<String> = Vec::new();
    for (name, outcome, keys) in outcomes {
        if keys.asked_again {
            diagnostics.push(format!("{name}: answered with no results; asked once more"));
        }
        if let Some(place) = keys.answered_marked {
            warnings.push(format!(
                "{name}: key {place} of {} answered again after being out of credit; its mark is cleared",
                keys.of
            ));
        }
        // A failure already names every key it went through.
        if keys.of > 1 && !matches!(outcome, Outcome::Failed(_)) {
            for (slot, f) in &keys.refused {
                // "Out of credit" only for the provider's own answer for it;
                // every other refusal by its category (auth, rate_limit, …).
                let why = match (f.code.as_str(), f.category) {
                    ("credits_exhausted", _) => "out of credit".to_string(),
                    (_, FailureCategory::BillingQuota) => {
                        "billing or quota, not confirmed out of credit".to_string()
                    }
                    (_, c) => c.as_str().to_string(),
                };
                warnings.push(format!(
                    "{name}: key {slot} of {} failed ({why}), so the next key was tried",
                    keys.of
                ));
            }
            if !matches!(outcome, Outcome::TimedOut) {
                key_slots.insert(name.to_string(), keys.last);
            }
        }
        queried.push(name.to_string());
        let spec = providers::find(name);
        let sent_cost = sent_costs.get(name).copied();
        // An answered call: the upstream's own figure, else the estimate.
        let answered_cost = |out: &Output| {
            out.cost_micros
                .unwrap_or_else(|| spec.map_or(0, |s| s.price.estimate(out.results.len() as u16)))
        };
        match outcome {
            Outcome::Done(out) => {
                by_provider.insert(name.to_string(), answered_cost(&out));
                diagnostics.extend(out.notes.iter().map(|n| format!("{name}: {n}")));
                answers.extend(out.answers);
                buckets.push((name.to_string(), out.results));
            }
            Outcome::Rejected(f, out) => {
                tracing::warn!("{name}: {}", redact_secrets(&f.reason));
                diagnostics.extend(out.notes.iter().map(|n| format!("{name}: {n}")));
                by_provider.insert(name.to_string(), answered_cost(&out));
                failures.push(f);
            }
            Outcome::Failed(f) => {
                tracing::warn!("{name}: {}", redact_secrets(&f.reason));
                if providers::billed_failure(f.category) {
                    if let Some(c) = sent_cost {
                        by_provider.insert(name.to_string(), c);
                    }
                }
                failures.push(f);
            }
            Outcome::TimedOut => {
                tracing::warn!("{name}: still running when the search stopped waiting");
                if let Some(c) = sent_cost {
                    by_provider.insert(name.to_string(), c);
                }
                timed_out.push(name.to_string());
                failures.push(timeout_failure(name));
            }
        }
    }

    // Remember the keys answered "out of credit" (and forget those that
    // answered again), then report every mark that applies to an engine of
    // this search: the key's place and both dates.
    let now = spent::now();
    let changes = learned.lock().map(|c| c.clone()).unwrap_or_default();
    let out_of_credit_keys: Vec<spent::SpentKey> = match req.spent {
        Some(store) => {
            let after = store.apply(&changes, now);
            plan_keys
                .iter()
                .filter(|(name, _)| queried.iter().any(|q| q == name))
                .flat_map(|(name, keys)| after.report(name, keys, now))
                .collect()
        }
        None => Vec::new(),
    };
    for k in &out_of_credit_keys {
        let alone = plan_keys
            .iter()
            .any(|(name, keys)| *name == k.provider && keys.len() == 1);
        warnings.push(if alone {
            format!(
                "{}: key {} is out of credit since {}; it is the only key, so it is still tried (marked until {})",
                k.provider, k.key, k.since, k.until
            )
        } else {
            format!(
                "{}: key {} is out of credit since {}; it is tried after the other keys until {}",
                k.provider, k.key, k.since, k.until
            )
        });
    }

    // A chain that stopped early never reached the rest.
    transport.retain(|name, _| queried.contains(name));
    let cost = (!by_provider.is_empty()).then(|| CostSummary::with_basis(by_provider, "estimate"));

    let provider_results: BTreeMap<String, usize> = buckets
        .iter()
        .filter(|(_, items)| !items.is_empty())
        .map(|(name, items)| (name.clone(), items.len()))
        .collect();

    // Only fused modes merge across providers. Chain and single keep the
    // answering provider's rows and order as sent: rows may share a URL
    // (images from one page) or have none (places).
    let mut results = match mode_spec.merge {
        MergeKind::Fused => fuse_rrf(buckets),
        MergeKind::Chain | MergeKind::Single => {
            buckets.into_iter().flat_map(|(_, items)| items).collect()
        }
    };

    // Total failure is an error, not a success-shaped envelope. Zero results
    // with at least one clean answer is `no_results`.
    if results.is_empty()
        && answers.is_empty()
        && !failures.is_empty()
        && failures.len() == queried.len()
    {
        return Err(SearchError::AllProvidersFailed {
            failed: failures,
            cost,
            out_of_credit_keys,
        });
    }

    results.truncate(req.count);

    if mode == Mode::Extract {
        // Scraped page content is untrusted input for the consuming agent —
        // label it so downstream prompts can treat it as data, not instructions.
        for r in &mut results {
            let extra = r.extra.get_or_insert_with(|| serde_json::json!({}));
            if let Some(obj) = extra.as_object_mut() {
                obj.insert(
                    "content_origin".to_string(),
                    serde_json::json!("untrusted_web_content"),
                );
            }
        }
    }

    let providers_failed: Vec<String> = failures.iter().map(|f| f.provider.clone()).collect();
    let status = ResponseStatus::classify(
        results.is_empty() && answers.is_empty(),
        !providers_failed.is_empty(),
    );
    Ok(SearchResponse {
        version: ENVELOPE_VERSION.to_string(),
        status: status.as_str().to_string(),
        query: req.query.to_string(),
        mode: req.mode.to_string(),
        metadata: ResponseMetadata {
            elapsed_ms: start.elapsed().as_millis(),
            result_count: results.len(),
            warnings: {
                warnings.extend(filter_warnings(mode, &queried, &applied, req.opts));
                warnings
            },
            providers_queried: queried,
            providers_failed,
            providers_cancelled: Vec::new(),
            providers_timed_out: timed_out,
            provider_results,
            key_slots,
            out_of_credit_keys,
            cached: false,
            cache_age_secs: None,
            provider_failures: failures,
            transport,
            cost,
            routing: None,
            notes: Vec::new(),
            diagnostics,
            for_owner: None,
        },
        results,
        answers,
    })
}

/// The mode's configured providers in registry order, narrowed by `-p`.
/// A `-p` name that does not serve the mode is bad input (exit 3), so a
/// typo never silently runs nothing or something else.
fn plan(
    adapters: Vec<Box<dyn Adapter>>,
    mode: Mode,
    order: &[&str],
    only: Option<&[String]>,
) -> Result<Vec<Box<dyn Adapter>>, SearchError> {
    if let Some(list) = only {
        for want in list {
            let serves = adapters
                .iter()
                .any(|a| a.name().eq_ignore_ascii_case(want) && a.spec().modes.contains(&mode));
            if !serves {
                let known = adapters.iter().any(|a| a.name().eq_ignore_ascii_case(want));
                let why = if known {
                    format!("provider '{want}' does not serve mode '{mode}'")
                } else {
                    format!("unknown provider '{want}'")
                };
                return Err(SearchError::InvalidInput {
                    message: format!(
                        "{why}; mode '{mode}' is served by [{}]",
                        servers(&adapters, mode).join(", ")
                    ),
                });
            }
        }
    }
    let rank = |name: &str| order.iter().position(|o| *o == name).unwrap_or(order.len());
    let mut picked: Vec<Box<dyn Adapter>> = adapters
        .into_iter()
        .filter(|a| match only {
            Some(list) => list.iter().any(|w| w.eq_ignore_ascii_case(a.name())),
            None => order.contains(&a.name()),
        })
        .filter(|a| a.spec().modes.contains(&mode) && a.is_configured())
        .collect();
    picked.sort_by_key(|a| rank(a.name()));
    Ok(picked)
}

fn servers(adapters: &[Box<dyn Adapter>], mode: Mode) -> Vec<&'static str> {
    adapters
        .iter()
        .filter(|a| a.spec().modes.contains(&mode))
        .map(|a| a.name())
        .collect()
}

fn per_call_count(adapter: &dyn Adapter, count: usize) -> u16 {
    (count.min(u16::MAX as usize) as u16).clamp(1, adapter.spec().max_count)
}

/// A call gets min(provider timeout, deadline left).
fn time_left(deadline: Instant, adapter: &dyn Adapter) -> Duration {
    adapter
        .spec()
        .timeout
        .min(deadline.saturating_duration_since(Instant::now()))
}

/// One provider's call. With several own keys, a key refused for its own
/// reasons ([`providers::next_key_may_help`]) hands the call to the next key
/// while the deadline allows; any other failure ends it.
/// How long before the mode deadline a second ask must end, so the engine
/// still keeps the first answer when the second call hangs.
const SECOND_ASK_MARGIN: Duration = Duration::from_millis(250);

/// Modes where an empty answer from an engine is unusual, so it is asked
/// once more before it counts as no results.
const ASK_AGAIN_MODES: &[Mode] = &[
    Mode::General,
    Mode::News,
    Mode::Academic,
    Mode::Deep,
    Mode::Scholar,
];

/// A call's keys in the order to try them, and where it notes what it
/// learns about marked keys.
struct Keys<'a> {
    order: &'a [Slot],
    learned: &'a Mutex<spent::Changes>,
}

async fn run_one(
    adapter: &dyn Adapter,
    keys: Keys<'_>,
    mode: Mode,
    query: &str,
    count: usize,
    opts: &SearchOpts,
    deadline: Instant,
) -> (Outcome, KeyUse) {
    let Keys { order, learned } = keys;
    let name = adapter.name();
    // Written as it happens, so a call the deadline cuts off still counts.
    let note = |spent: bool, slot: &Slot| {
        if let Ok(mut c) = learned.lock() {
            let entry = (name.to_string(), slot.fingerprint.clone());
            if spent {
                c.spent.push(entry);
            } else {
                c.answered.push(entry);
            }
        }
    };
    let mut used = KeyUse {
        of: order.iter().filter(|s| s.place > 0).count(),
        ..KeyUse::default()
    };
    for (i, slot) in order.iter().enumerate() {
        used.last = slot.place;
        used.tried += 1;
        let call = Call {
            mode,
            query,
            count: per_call_count(adapter, count),
            opts,
            timeout: time_left(deadline, adapter),
            key: &slot.key,
        };
        let sent = Instant::now();
        let mut answer = tokio::time::timeout_at(deadline.into(), adapter.call(&call)).await;
        // In the web modes an answer with nothing in it is asked once more,
        // with the same key, before it counts as no results (a service
        // sometimes answers empty with an OK status). Only when there is time
        // for a call as slow as the first; a failed second ask keeps the
        // first answer. Elsewhere (places, social, …) empty is a normal answer.
        if let Ok(Ok(first)) = &mut answer {
            let took = sent.elapsed();
            let free = first.cost_micros == Some(0);
            if first.results.is_empty()
                && first.answers.is_empty()
                && !free
                && ASK_AGAIN_MODES.contains(&mode)
                && deadline
                    .saturating_duration_since(Instant::now())
                    .saturating_sub(SECOND_ASK_MARGIN)
                    >= took
            {
                used.asked_again = true;
                let paid = first
                    .cost_micros
                    .unwrap_or_else(|| adapter.spec().price.estimate(0));
                // Ends before the mode deadline, so the first answer can still
                // be kept when the second call is slow.
                let cutoff = deadline - SECOND_ASK_MARGIN;
                let again = Call {
                    timeout: time_left(cutoff, adapter),
                    ..call
                };
                match tokio::time::timeout_at(cutoff.into(), adapter.call(&again)).await {
                    Ok(Ok(mut second)) => {
                        let n = second.results.len() as u16;
                        let cost = second
                            .cost_micros
                            .unwrap_or_else(|| adapter.spec().price.estimate(n));
                        second.cost_micros = Some(paid + cost);
                        *first = second;
                    }
                    // The first, empty answer stands. A second call that
                    // timed out or answered unreadably was billed anyway.
                    failed => {
                        let billed = match &failed {
                            Err(_) => true,
                            Ok(Err(e)) => providers::billed_failure(e.category()),
                            Ok(Ok(_)) => false,
                        };
                        let extra = if billed && adapter.spec().billed_on_send {
                            adapter.sent_cost(mode)
                        } else {
                            0
                        };
                        first.cost_micros = Some(paid + extra);
                    }
                }
            }
        }
        let failure = match answer {
            Ok(Ok(out)) => {
                if slot.marked {
                    note(false, slot);
                    used.answered_marked = Some(slot.place);
                }
                return (Outcome::Done(out), used);
            }
            Ok(Err(e)) => e.to_provider_failure(name),
            Err(_) => return (Outcome::TimedOut, used),
        };
        if failure.code == "credits_exhausted" && slot.place > 0 {
            note(true, slot);
        }
        let rest = &order[i + 1..];
        // A key refused for its own reasons hands over to the next. Once only
        // keys marked out of credit are left, they are tried after any
        // failure that was not billed, so a topped-up key is never missed.
        let next = providers::next_key_may_help(failure.category)
            || (!rest.is_empty()
                && rest.iter().all(|s| s.marked)
                && !providers::billed_failure(failure.category));
        if !rest.is_empty() && next && Instant::now() < deadline {
            used.refused.push((slot.place, failure));
            continue;
        }
        let mut failure = used.failure(name, failure);
        if !rest.is_empty() && next {
            failure
                .reason
                .push_str("; the search's time ran out before the next key");
        }
        return (Outcome::Failed(failure), used);
    }
    unreachable!("a key order is never empty")
}

/// Every provider at once; collect until all finish or the deadline passes.
/// With a straggler cap, once the cap has passed and one provider has
/// answered with something, the rest are no longer waited for: one slow
/// platform cannot hold up the whole call.
async fn run_fused(
    plan: Vec<Box<dyn Adapter>>,
    req: &Request<'_>,
    learned: &Arc<Mutex<spent::Changes>>,
    mode: Mode,
    deadline: Instant,
    straggler_cap: Option<Instant>,
) -> Vec<(&'static str, Outcome, KeyUse)> {
    let mut set: JoinSet<(&'static str, Outcome, KeyUse)> = JoinSet::new();
    let mut pending: Vec<&'static str> = Vec::new();
    let mut task_names: HashMap<tokio::task::Id, &'static str> = HashMap::new();
    let now = spent::now();
    for adapter in plan {
        let order = key_order(adapter.as_ref(), req.spent, now);
        let name = adapter.name();
        pending.push(name);
        let query = req.query.to_string();
        let opts = req.opts.clone();
        let count = req.count;
        let learned = learned.clone();
        let handle = set.spawn(async move {
            let (outcome, keys) = run_one(
                adapter.as_ref(),
                Keys {
                    order: &order,
                    learned: &learned,
                },
                mode,
                &query,
                count,
                &opts,
                deadline,
            )
            .await;
            (name, outcome, keys)
        });
        task_names.insert(handle.id(), name);
    }

    let mut outcomes = Vec::new();
    let mut answered = false;
    loop {
        let until = match straggler_cap {
            Some(cap) if answered => cap.min(deadline),
            _ => deadline,
        };
        match tokio::time::timeout_at(until.into(), set.join_next()).await {
            Ok(Some(Ok((name, outcome, keys)))) => {
                pending.retain(|n| *n != name);
                if let Outcome::Done(out) = &outcome {
                    answered |= !out.results.is_empty() || !out.answers.is_empty();
                }
                outcomes.push((name, outcome, keys));
            }
            // A crashed adapter is a failure of that provider, not a timeout,
            // and its cost is unknown, so nothing is charged for it.
            Ok(Some(Err(e))) => {
                if let Some(name) = task_names.get(&e.id()).copied() {
                    pending.retain(|n| *n != name);
                    outcomes.push((
                        name,
                        Outcome::Failed(ProviderFailure {
                            provider: name.to_string(),
                            category: FailureCategory::Other,
                            http_status: None,
                            code: "crashed".to_string(),
                            reason: format!("{name} crashed inside the CLI"),
                            retryable: false,
                        }),
                        KeyUse::default(),
                    ));
                }
            }
            Ok(None) => break,
            Err(_) => {
                // Deadline (or the straggler cap with an answer in hand):
                // nothing more will be waited for.
                set.abort_all();
                break;
            }
        }
    }
    outcomes.extend(
        pending
            .into_iter()
            .map(|n| (n, Outcome::TimedOut, KeyUse::default())),
    );
    outcomes
}

/// In order; the first output with results that the mode accepts wins.
async fn run_chain(
    plan: Vec<Box<dyn Adapter>>,
    req: &Request<'_>,
    learned: &Mutex<spent::Changes>,
    mode: Mode,
    deadline: Instant,
) -> Vec<(&'static str, Outcome, KeyUse)> {
    let mut outcomes = Vec::new();
    for adapter in plan {
        let name = adapter.name();
        if Instant::now() >= deadline {
            break; // never sent, so neither a failure nor a cost
        }
        // A URL direct found blocked, paywalled (402) or gone (404/410) is
        // not fetched again by a later rung (and stealth runs only when
        // direct was refused). The gate's errors are BadRequest or Rejected:
        // never billed.
        if let Err(e) = registry::rung_gate(name, req.query) {
            outcomes.push((
                name,
                Outcome::Failed(e.to_provider_failure(name)),
                KeyUse::default(),
            ));
            continue;
        }
        let order = key_order(adapter.as_ref(), req.spent, spent::now());
        let (outcome, keys) = run_one(
            adapter.as_ref(),
            Keys {
                order: &order,
                learned,
            },
            mode,
            req.query,
            req.count,
            req.opts,
            deadline,
        )
        .await;
        match outcome {
            Outcome::Done(out) if out.results.is_empty() => {
                outcomes.push((name, Outcome::Done(out), keys));
            }
            Outcome::Done(out) => match registry::accept(mode, &out) {
                Ok(()) => {
                    outcomes.push((name, Outcome::Done(out), keys));
                    break;
                }
                Err(reason) => {
                    outcomes.push((name, Outcome::Rejected(rejected(name, reason), out), keys))
                }
            },
            other => outcomes.push((name, other, keys)),
        }
    }
    outcomes
}

fn rejected(provider: &str, reason: &str) -> ProviderFailure {
    ProviderFailure {
        provider: provider.to_string(),
        category: FailureCategory::Rejected,
        http_status: None,
        code: "rejected".to_string(),
        reason: format!("{provider} returned {reason}"),
        retryable: false,
    }
}

/// Failure record for a provider still running at the mode deadline.
fn timeout_failure(provider: &str) -> ProviderFailure {
    ProviderFailure {
        provider: provider.to_string(),
        category: FailureCategory::Timeout,
        http_status: None,
        code: "timeout".to_string(),
        reason: format!("{provider} was still running when the search stopped waiting (the mode deadline, or in social the 20 s wait once another platform had answered)"),
        retryable: true,
    }
}

/// Zero providers ran. With `-p` that's the caller's filter selecting only
/// unconfigured providers — a bad-input error naming the mode's real provider
/// set. Without `-p` it's a configuration problem.
fn no_providers_error(mode: Mode, only: Option<&[String]>) -> SearchError {
    match only {
        Some(list) => SearchError::InvalidInput {
            message: format!(
                "no provider ran: mode '{}' is served by [{}], but -p {} selected none of them that are configured",
                mode,
                registry::spec(mode).providers.join(", "),
                list.join(",")
            ),
        },
        None => SearchError::NoProviders(mode.to_string()),
    }
}

/// Reciprocal rank fusion across provider buckets. Each URL scores
/// Σ 1/(RRF_K + rank) over the providers that returned it, so a result two
/// engines agree on outranks any single engine's top hit. Deterministic:
/// independent of provider completion order. Duplicates keep the first-seen
/// content (by bucket order), enriched with missing fields and an
/// `also_found_by` list in `extra`. A row without a URL (a citation or a
/// place with no link) is its own result: nothing proves two such rows match.
fn fuse_rrf(buckets: Vec<(String, Vec<SearchResult>)>) -> Vec<SearchResult> {
    struct Fused {
        result: SearchResult,
        score: f64,
        insertion: usize,
    }
    let mut map: HashMap<String, Fused> = HashMap::new();
    let mut insertion = 0usize;
    for (provider, items) in buckets {
        for (rank, item) in items.into_iter().enumerate() {
            let key = match normalize_url(&item.url) {
                k if k.is_empty() => format!("\0{insertion}"),
                k => k,
            };
            let add = 1.0 / (RRF_K + rank as f64 + 1.0);
            // A row names its own source (exa_people, ...), else its bucket's.
            let found_by = if item.source.is_empty() {
                provider.clone()
            } else {
                item.source.clone()
            };
            match map.get_mut(&key) {
                Some(f) => {
                    f.score += add;
                    if f.result.published.is_none() {
                        f.result.published = item.published;
                    }
                    if f.result.image_url.is_none() {
                        f.result.image_url = item.image_url;
                    }
                    let extra = f.result.extra.get_or_insert_with(|| serde_json::json!({}));
                    if let Some(obj) = extra.as_object_mut() {
                        let list = obj
                            .entry("also_found_by")
                            .or_insert_with(|| serde_json::json!([]));
                        if let Some(arr) = list.as_array_mut() {
                            arr.push(serde_json::json!(found_by));
                        }
                    }
                }
                None => {
                    insertion += 1;
                    map.insert(
                        key,
                        Fused {
                            result: item,
                            score: add,
                            insertion,
                        },
                    );
                }
            }
        }
    }
    let mut fused: Vec<Fused> = map.into_values().collect();
    fused.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.insertion.cmp(&b.insertion))
    });
    fused.into_iter().map(|f| f.result).collect()
}

/// Honesty notes for filtered searches: one per queried provider that drops
/// a requested filter, so filtered results stay auditable. Never excludes a
/// provider — the caller decides whether unfiltered results are acceptable.
fn filter_warnings(
    mode: Mode,
    queried: &[String],
    applied: &HashMap<&'static str, FilterSupport>,
    opts: &SearchOpts,
) -> Vec<String> {
    let wants_freshness = opts.freshness.is_some();
    let wants_domains = !opts.include_domains.is_empty() || !opts.exclude_domains.is_empty();
    let wants_country = opts.country.is_some();
    let wants_lang = opts.lang.is_some();
    if !(wants_freshness || wants_domains || wants_country || wants_lang) {
        return Vec::new();
    }
    if !registry::mode_applies_filters(mode) {
        return vec![format!(
            "mode '{mode}' does not apply -f/-d/--country/--lang filters; results are unfiltered"
        )];
    }
    let mut warnings = Vec::new();
    for name in queried {
        let Some(f) = applied.get(name.as_str()).copied() else {
            continue;
        };
        let missing: Vec<&str> = [
            (wants_freshness && !f.freshness, "-f freshness"),
            (wants_domains && !f.domains, "-d/--exclude-domain"),
            (wants_country && !f.country, "--country"),
            (wants_lang && !f.lang, "--lang"),
        ]
        .into_iter()
        .filter_map(|(drop, label)| drop.then_some(label))
        .collect();
        if !missing.is_empty() {
            warnings.push(format!(
                "{name} does not apply {}; its results are unfiltered",
                missing.join(" or ")
            ));
        }
        // The caveat describes what happens to domains that were sent.
        if wants_domains && f.domains {
            if let Some(note) = registry::filter_support(name).note {
                warnings.push(note.to_string());
            }
        }
    }
    warnings
}

/// Dedup key for a URL: scheme and a leading `www.` dropped, host lowercased, path
/// and query case kept, trailing `/` and `#fragment` dropped, tracking
/// parameters removed and the rest sorted.
pub(crate) fn normalize_url(url: &str) -> String {
    let url = url.trim();
    let rest = strip_prefix_ci(url, "https://")
        .or_else(|| strip_prefix_ci(url, "http://"))
        .unwrap_or(url);
    let rest = rest.split('#').next().unwrap_or(rest);
    let (before_query, query) = match rest.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (rest, None),
    };
    let (host, path) = match before_query.find('/') {
        Some(i) => before_query.split_at(i),
        None => (before_query, ""),
    };
    let host = host.to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host);
    let mut key = String::with_capacity(url.len());
    key.push_str(host);
    key.push_str(path.trim_end_matches('/'));
    if let Some(q) = query {
        let mut params: Vec<&str> = q
            .split('&')
            .filter(|p| !p.is_empty() && !is_tracker(p))
            .collect();
        params.sort_unstable();
        if !params.is_empty() {
            key.push('?');
            key.push_str(&params.join("&"));
        }
    }
    key
}

fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &s[prefix.len()..])
}

fn is_tracker(param: &str) -> bool {
    let name = param.split('=').next().unwrap_or(param);
    name.starts_with("utm_")
        || matches!(name, "gclid" | "fbclid" | "msclkid" | "mc_eid" | "ref_src")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{brave, exa, firecrawl, jina, linkup, parallel, perplexity, serpapi};
    use crate::providers::{serper, tavily, xai, ProviderSpec};
    use std::sync::Mutex;

    fn result(url: &str, source: &str) -> SearchResult {
        SearchResult {
            title: url.to_string(),
            url: url.to_string(),
            snippet: String::new(),
            source: source.to_string(),
            published: None,
            image_url: None,
            extra: None,
        }
    }

    /// What a fake adapter does when called.
    #[derive(Clone)]
    enum Act {
        /// Answer after a delay with these URLs (and optional answer/cost).
        Ok {
            delay_ms: u64,
            urls: Vec<String>,
            answer: Option<&'static str>,
            cost: Option<u64>,
            snippet: &'static str,
        },
        /// Fail with this category.
        Fail(FailureCategory),
    }

    type Log = Arc<Mutex<Vec<(String, u16)>>>;

    struct Fake {
        spec: &'static ProviderSpec,
        act: Act,
        log: Log,
        /// Overrides `Adapter::sent_cost` (as Tavily and Brave deep do).
        sent: Option<u64>,
    }

    #[async_trait::async_trait]
    impl Adapter for Fake {
        fn spec(&self) -> &'static ProviderSpec {
            self.spec
        }
        fn is_configured(&self) -> bool {
            true
        }
        fn sent_cost(&self, _mode: Mode) -> u64 {
            self.sent.unwrap_or(self.spec.price.estimate(0))
        }
        async fn call(&self, call: &Call<'_>) -> Result<Output, SearchError> {
            self.log
                .lock()
                .unwrap()
                .push((self.spec.name.to_string(), call.count));
            match &self.act {
                Act::Ok {
                    delay_ms,
                    urls,
                    answer,
                    cost,
                    snippet,
                } => {
                    tokio::time::sleep(Duration::from_millis(*delay_ms)).await;
                    let mut results: Vec<SearchResult> =
                        urls.iter().map(|u| result(u, self.spec.name)).collect();
                    for r in &mut results {
                        r.snippet = snippet.to_string();
                    }
                    Ok(Output {
                        results,
                        answers: answer
                            .map(|t| Answer {
                                provider: self.spec.name.to_string(),
                                text: t.to_string(),
                            })
                            .into_iter()
                            .collect(),
                        cost_micros: *cost,
                        notes: Vec::new(),
                    })
                }
                Act::Fail(category) => Err(SearchError::Upstream {
                    provider: self.spec.name,
                    status: 500,
                    category: *category,
                    message: "boom".into(),
                }),
            }
        }
    }

    fn ok(delay_ms: u64, n: usize, tag: &str) -> Act {
        Act::Ok {
            delay_ms,
            urls: (0..n)
                .map(|i| format!("https://{tag}.example/{i}"))
                .collect(),
            answer: None,
            cost: None,
            snippet: "a real page about something",
        }
    }

    fn fakes(specs: &[(&'static ProviderSpec, Act)]) -> (Vec<Box<dyn Adapter>>, Log) {
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let adapters = specs
            .iter()
            .map(|(spec, act)| {
                Box::new(Fake {
                    spec,
                    act: act.clone(),
                    log: log.clone(),
                    sent: None,
                }) as Box<dyn Adapter>
            })
            .collect();
        (adapters, log)
    }

    async fn search(
        adapters: Vec<Box<dyn Adapter>>,
        mode: Mode,
        count: usize,
        only: Option<&[String]>,
        deadline_ms: u64,
    ) -> Result<SearchResponse, SearchError> {
        search_marked(adapters, mode, count, only, deadline_ms, None).await
    }

    /// As [`search`], remembering out-of-credit keys in `store`.
    async fn search_marked(
        adapters: Vec<Box<dyn Adapter>>,
        mode: Mode,
        count: usize,
        only: Option<&[String]>,
        deadline_ms: u64,
        store: Option<&spent::Store>,
    ) -> Result<SearchResponse, SearchError> {
        let opts = SearchOpts::default();
        let req = Request {
            query: "q",
            mode,
            count,
            only,
            opts: &opts,
            deadline: Duration::from_millis(deadline_ms),
            straggler_cap: None,
            spent: store,
        };
        execute_with(adapters, &req).await
    }

    /// A provider with several keys; each key answers or fails its own way.
    struct Keyed {
        spec: &'static ProviderSpec,
        keys: Vec<(&'static str, Option<FailureCategory>)>,
        tried: Arc<Mutex<Vec<String>>>,
        /// How long each call takes before it answers or fails.
        delay: Duration,
        /// Keys the provider answers as out of credit (`credits_exhausted`).
        exhausted: Vec<&'static str>,
    }

    #[async_trait::async_trait]
    impl Adapter for Keyed {
        fn spec(&self) -> &'static ProviderSpec {
            self.spec
        }
        fn is_configured(&self) -> bool {
            !self.keys.is_empty()
        }
        fn keys(&self) -> Vec<String> {
            self.keys.iter().map(|(k, _)| k.to_string()).collect()
        }
        async fn call(&self, call: &Call<'_>) -> Result<Output, SearchError> {
            self.tried.lock().unwrap().push(call.key.to_string());
            tokio::time::sleep(self.delay).await;
            if self.exhausted.contains(&call.key) {
                return Err(SearchError::CreditsExhausted {
                    provider: self.spec.name,
                    status: 402,
                    message: "HTTP 402: {\"tag\":\"NO_MORE_CREDITS\"}".into(),
                });
            }
            let (key, fails) = self.keys.iter().find(|(k, _)| *k == call.key).unwrap();
            match fails {
                // The upstream echoes the key, as some do in error bodies:
                // after a marker, and bare.
                Some(category) => Err(SearchError::Upstream {
                    provider: self.spec.name,
                    status: 402,
                    category: *category,
                    message: format!("HTTP 402: key={key} refused, Bearer {key}; {key} is spent"),
                }),
                None => Ok(Output {
                    results: vec![result(
                        &format!("https://{}.example/{key}", self.spec.name),
                        self.spec.name,
                    )],
                    ..Output::default()
                }),
            }
        }
    }

    fn keyed(
        spec: &'static ProviderSpec,
        keys: &[(&'static str, Option<FailureCategory>)],
    ) -> (Box<dyn Adapter>, Arc<Mutex<Vec<String>>>) {
        slow_keyed(spec, keys, 0)
    }

    fn slow_keyed(
        spec: &'static ProviderSpec,
        keys: &[(&'static str, Option<FailureCategory>)],
        delay_ms: u64,
    ) -> (Box<dyn Adapter>, Arc<Mutex<Vec<String>>>) {
        let tried = Arc::new(Mutex::new(Vec::new()));
        let adapter = Keyed {
            spec,
            keys: keys.to_vec(),
            tried: tried.clone(),
            delay: Duration::from_millis(delay_ms),
            exhausted: Vec::new(),
        };
        (Box::new(adapter), tried)
    }

    /// A keyed fake whose `exhausted` keys answer "out of credit".
    fn keyed_spent(
        spec: &'static ProviderSpec,
        keys: &[(&'static str, Option<FailureCategory>)],
        exhausted: &[&'static str],
    ) -> (Box<dyn Adapter>, Arc<Mutex<Vec<String>>>) {
        let tried = Arc::new(Mutex::new(Vec::new()));
        let adapter = Keyed {
            spec,
            keys: keys.to_vec(),
            tried: tried.clone(),
            delay: Duration::ZERO,
            exhausted: exhausted.to_vec(),
        };
        (Box::new(adapter), tried)
    }

    /// A store in a fresh temporary file.
    fn temp_store(tag: &str) -> spent::Store {
        let path = std::env::temp_dir().join(format!(
            "search-cli-engine-spent-{tag}-{}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        spent::Store::load(path)
    }

    fn marks(store: &spent::Store, keys: &[&str]) -> Vec<usize> {
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        store
            .report("exa", &keys, spent::now())
            .iter()
            .map(|k| k.key)
            .collect()
    }

    /// An "out of credit" answer marks the key; the next search tries it
    /// after the other keys, and names it with both dates.
    #[tokio::test]
    async fn an_out_of_credit_key_is_remembered_and_tried_last() {
        let store = temp_store("remember");
        let (exa, log) = keyed_spent(&exa::SPEC, &[(K1, None), (K2, None)], &[K1]);
        let resp = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store))
            .await
            .unwrap();
        assert_eq!(tried(&log), [K1, K2]);
        assert_eq!(resp.metadata.key_slots.get("exa"), Some(&2));
        let w = resp.metadata.warnings.join("\n");
        assert!(w.contains("exa: key 1 of 2 failed (out of credit)"), "{w}");
        assert_eq!(resp.metadata.out_of_credit_keys.len(), 1, "{w}");
        let k = &resp.metadata.out_of_credit_keys[0];
        assert_eq!((k.provider.as_str(), k.key), ("exa", 1));
        assert!(k.since.ends_with('Z') && k.until > k.since);

        // The next search does not send key 1 while key 2 answers.
        let store = spent::Store::load(store.path().to_path_buf());
        let (exa, log) = keyed_spent(&exa::SPEC, &[(K1, None), (K2, None)], &[]);
        let resp = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store))
            .await
            .unwrap();
        assert_eq!(tried(&log), [K2]);
        assert_eq!(resp.metadata.key_slots.get("exa"), Some(&2));
        assert_eq!(resp.metadata.out_of_credit_keys[0].key, 1);
        assert!(resp
            .metadata
            .warnings
            .iter()
            .any(|w| w.starts_with("exa: key 1 is out of credit since ")));
        store.clear();
    }

    /// No false negative: when every other key fails, the marked key is
    /// still tried, and one that answers loses its mark.
    #[tokio::test]
    async fn a_marked_key_is_still_tried_before_the_engine_fails() {
        let store = temp_store("lastresort");
        let store = store.apply(
            &spent::Changes {
                spent: vec![("exa".into(), spent::fingerprint(K1))],
                answered: vec![],
            },
            spent::now(),
        );
        assert_eq!(marks(&store, &[K1, K2]), [1]);
        let (exa, log) = keyed(&exa::SPEC, &[(K1, None), (K2, Some(FailureCategory::Auth))]);
        let resp = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store))
            .await
            .unwrap();
        assert_eq!(tried(&log), [K2, K1]);
        assert_eq!(resp.metadata.key_slots.get("exa"), Some(&1));
        assert!(resp.metadata.out_of_credit_keys.is_empty());
        assert!(resp
            .metadata
            .warnings
            .iter()
            .any(|w| w.contains("key 1 of 2 answered again after being out of credit")));
        let store = spent::Store::load(store.path().to_path_buf());
        assert!(marks(&store, &[K1, K2]).is_empty());
        store.clear();
    }

    /// Only the provider's own "out of credit" answer marks a key: a rejected
    /// key, a rate limit, a quota-sounding answer the provider does not
    /// document as such, a timeout or a server error never does.
    #[tokio::test]
    async fn nothing_but_out_of_credit_marks_a_key() {
        use FailureCategory::*;
        for category in [Auth, RateLimit, BillingQuota, Timeout, Server, BadRequest] {
            let store = temp_store("strict");
            let (exa, _) = keyed(&exa::SPEC, &[(K1, Some(category)), (K2, None)]);
            let _ = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store)).await;
            let store = spent::Store::load(store.path().to_path_buf());
            assert!(
                marks(&store, &[K1, K2]).is_empty(),
                "{category:?} marked a key"
            );
            store.clear();
        }
    }

    fn mark(store: &spent::Store, key: &str) -> spent::Store {
        store.apply(
            &spent::Changes {
                spent: vec![("exa".into(), spent::fingerprint(key))],
                answered: vec![],
            },
            spent::now(),
        )
    }

    /// A call the deadline cuts off still leaves its "out of credit" mark.
    #[tokio::test]
    async fn a_mark_survives_the_deadline_cutting_the_call_off() {
        let store = temp_store("cutoff");
        let sent = Arc::new(Mutex::new(Vec::new()));
        let exa = Box::new(Keyed {
            spec: &exa::SPEC,
            keys: vec![(K1, None), (K2, None)],
            tried: sent.clone(),
            delay: Duration::from_millis(150),
            exhausted: vec![K1],
        }) as Box<dyn Adapter>;
        let (others, _) = fakes(&[(&parallel::SPEC, ok(0, 2, "p"))]);
        let mut plan = vec![exa];
        plan.extend(others);
        let _ = search_marked(plan, Mode::General, 10, None, 250, Some(&store)).await;
        assert_eq!(
            tried(&sent),
            [K1, K2],
            "key 2 was still running at the deadline"
        );
        let store = spent::Store::load(store.path().to_path_buf());
        assert_eq!(marks(&store, &[K1, K2]), [1]);
        store.clear();
    }

    /// Once only marked keys are left, they are tried after any failure that
    /// was not billed (here a bad request), not only after a key refusal.
    #[tokio::test]
    async fn marked_keys_follow_any_unbilled_failure() {
        let store = temp_store("anyfail");
        let store = mark(&store, K1);
        let (exa, log) = keyed(
            &exa::SPEC,
            &[(K1, None), (K2, Some(FailureCategory::BadRequest))],
        );
        let resp = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store))
            .await
            .unwrap();
        assert_eq!(tried(&log), [K2, K1]);
        assert_eq!(resp.metadata.key_slots.get("exa"), Some(&1));
        // A billed failure (timeout) does not send the marked key as well.
        let (exa, log) = keyed(
            &exa::SPEC,
            &[(K1, None), (K2, Some(FailureCategory::Timeout))],
        );
        let store = mark(&store, K1);
        let _ = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store)).await;
        assert_eq!(tried(&log), [K2]);
        store.clear();
    }

    /// When the whole search fails, the error carries the marked keys and
    /// leads with the failure a user can act on, not "out of credit" again.
    #[tokio::test]
    async fn a_total_failure_names_marked_keys_and_the_actionable_cause() {
        let store = temp_store("totalfail");
        let store = mark(&store, K1);
        let (exa, _) = keyed_spent(
            &exa::SPEC,
            &[(K1, None), (K2, Some(FailureCategory::Auth))],
            &[K1],
        );
        let err = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store))
            .await
            .unwrap_err();
        let SearchError::AllProvidersFailed {
            failed,
            out_of_credit_keys,
            ..
        } = err
        else {
            panic!()
        };
        assert_eq!(failed[0].category, FailureCategory::Auth);
        assert_eq!(failed[0].code, "auth_failed");
        assert!(
            failed[0].reason.contains("key 1 (out of credit): "),
            "{}",
            failed[0].reason
        );
        assert_eq!(out_of_credit_keys.len(), 1);
        assert_eq!(out_of_credit_keys[0].key, 1);
        store.clear();
    }

    /// Every key out of credit: each is still tried, the engine fails once,
    /// and both are remembered.
    #[tokio::test]
    async fn every_key_out_of_credit_tries_each_and_marks_each() {
        let store = temp_store("allspent");
        let (exa, log) = keyed_spent(&exa::SPEC, &[(K1, None), (K2, None)], &[K1, K2]);
        let err = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store))
            .await
            .unwrap_err();
        assert_eq!(tried(&log), [K1, K2]);
        let SearchError::AllProvidersFailed { failed, .. } = err else {
            panic!()
        };
        assert_eq!(failed[0].code, "credits_exhausted");
        assert_eq!(failed[0].category, FailureCategory::BillingQuota);
        let store = spent::Store::load(store.path().to_path_buf());
        assert_eq!(marks(&store, &[K1, K2]), [1, 2]);
        // Both marked: the next search still sends them, in list order.
        let (exa, log) = keyed(&exa::SPEC, &[(K1, None), (K2, None)]);
        let resp = search_marked(vec![exa], Mode::General, 10, None, 5_000, Some(&store))
            .await
            .unwrap();
        assert_eq!(tried(&log), [K1]);
        assert_eq!(resp.metadata.key_slots.get("exa"), Some(&1));
        store.clear();
    }

    fn tried(log: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        log.lock().unwrap().clone()
    }

    const K1: &str = "exa-first-0123456789";
    const K2: &str = "exa-second-0123456789";
    const K3: &str = "exa-third-0123456789";

    /// A key refused, out of credit or rate limited hands the call to the
    /// next key in the same search; the engine keeps its results, and the
    /// response names the key that answered and the ones refused before it.
    #[tokio::test]
    async fn a_refused_key_hands_the_call_to_the_next() {
        use FailureCategory::*;
        for refusal in [BillingQuota, Auth, RateLimit] {
            let (exa, log) = keyed(
                &exa::SPEC,
                &[(K1, Some(refusal)), (K2, Some(BillingQuota)), (K3, None)],
            );
            let (others, _) = fakes(&[(&parallel::SPEC, ok(0, 2, "p"))]);
            let mut plan = vec![exa];
            plan.extend(others);
            let resp = search(plan, Mode::General, 10, None, 5_000).await.unwrap();
            assert_eq!(tried(&log), [K1, K2, K3], "{refusal:?}");
            assert!(resp.metadata.providers_failed.is_empty());
            assert_eq!(resp.metadata.provider_results.get("exa"), Some(&1));
            // Only providers with several keys get a slot.
            assert_eq!(
                resp.metadata.key_slots,
                BTreeMap::from([("exa".to_string(), 3)])
            );
            let warnings = resp.metadata.warnings.join("\n");
            // A quota answer the provider does not document as "out of
            // credit" says so, apart from a confirmed one.
            let said = |c: FailureCategory| match c {
                BillingQuota => "billing or quota, not confirmed out of credit".to_string(),
                c => c.as_str().to_string(),
            };
            assert!(
                warnings.contains(&format!("exa: key 1 of 3 failed ({})", said(refusal))),
                "{warnings}"
            );
            assert!(warnings.contains(&format!("exa: key 2 of 3 failed ({})", said(BillingQuota))));
        }
    }

    /// A timeout, a server error, a bad request or a parse failure would
    /// fail the same way with any key (and a timeout was billed): the next
    /// key is not tried.
    #[tokio::test]
    async fn other_failures_do_not_try_another_key() {
        use FailureCategory::*;
        for category in [Server, Timeout, BadRequest, Parse, Network, Other] {
            let (exa, log) = keyed(&exa::SPEC, &[(K1, Some(category)), (K2, None)]);
            let err = search(vec![exa], Mode::General, 10, None, 5_000)
                .await
                .unwrap_err();
            assert_eq!(tried(&log), [K1], "{category:?}");
            let SearchError::AllProvidersFailed { failed, .. } = err else {
                panic!("{category:?}")
            };
            assert_eq!(failed[0].category, category);
            assert!(!failed[0].reason.contains("keys"), "{}", failed[0].reason);
        }
    }

    /// A key refused, then another failure that is not about the key: the
    /// failure names both, and the third key is never sent.
    #[tokio::test]
    async fn a_later_failure_not_about_the_key_ends_the_rotation() {
        use FailureCategory::*;
        let (exa, log) = keyed(
            &exa::SPEC,
            &[(K1, Some(Auth)), (K2, Some(Server)), (K3, None)],
        );
        let err = search(vec![exa], Mode::General, 10, None, 5_000)
            .await
            .unwrap_err();
        assert_eq!(tried(&log), [K1, K2]);
        let SearchError::AllProvidersFailed { failed, .. } = err else {
            panic!()
        };
        assert_eq!(failed[0].category, Server);
        assert!(
            failed[0].reason.starts_with("key 2 of 3: ")
                && failed[0].reason.contains("refused before it: key 1: "),
            "{}",
            failed[0].reason
        );
    }

    /// Every key spent: one failure for the engine, naming each key's cause,
    /// with the last key's category; the rest of the search still answers.
    #[tokio::test]
    async fn every_key_failing_is_one_failure_naming_each_key() {
        use FailureCategory::*;
        let (exa, log) = keyed(&exa::SPEC, &[(K1, Some(BillingQuota)), (K2, Some(Auth))]);
        let (others, _) = fakes(&[(&parallel::SPEC, ok(0, 2, "p"))]);
        let mut plan = vec![exa];
        plan.extend(others);
        let resp = search(plan, Mode::General, 10, None, 5_000).await.unwrap();
        assert_eq!(tried(&log), [K1, K2]);
        assert_eq!(resp.metadata.providers_failed, ["exa"]);
        assert!(resp.metadata.key_slots.is_empty());
        let f = &resp.metadata.provider_failures[0];
        assert_eq!(f.category, Auth);
        assert!(
            f.reason.starts_with("all 2 exa keys failed: key 1: ")
                && f.reason.contains("; key 2: "),
            "{}",
            f.reason
        );
        // The failure already names every key: no warning repeats it.
        assert!(
            resp.metadata.warnings.is_empty(),
            "{:?}",
            resp.metadata.warnings
        );

        // Alone, the spent keys are the whole search's failure (exit 2:
        // the user has to act).
        let (exa, _) = keyed(&exa::SPEC, &[(K1, Some(BillingQuota)), (K2, Some(Auth))]);
        let err = search(vec![exa], Mode::General, 10, None, 5_000)
            .await
            .unwrap_err();
        assert_eq!(err.exit_code(), 2);
    }

    /// One key: a refusal is reported exactly as before, with no key slot.
    #[tokio::test]
    async fn one_key_keeps_the_old_report() {
        let (exa, log) = keyed(&exa::SPEC, &[(K1, Some(FailureCategory::BillingQuota))]);
        let (others, _) = fakes(&[(&parallel::SPEC, ok(0, 2, "p"))]);
        let mut plan = vec![exa];
        plan.extend(others);
        let resp = search(plan, Mode::General, 10, None, 5_000).await.unwrap();
        assert_eq!(tried(&log), [K1]);
        assert!(resp.metadata.key_slots.is_empty());
        assert!(resp.metadata.warnings.is_empty());
        let reason = &resp.metadata.provider_failures[0].reason;
        assert!(
            reason.starts_with("API error from exa: HTTP 402"),
            "{reason}"
        );
    }

    /// No key value reaches the response, its warnings or its failures,
    /// even when the upstream echoes the key back bare: every key is
    /// registered for redaction, fallbacks included, as main.rs does with
    /// `providers::every_key`.
    #[tokio::test]
    async fn no_key_reaches_the_response() {
        use FailureCategory::*;
        crate::errors::register_secrets([K1, K2, K3].map(String::from));
        let (exa, _) = keyed(&exa::SPEC, &[(K1, Some(BillingQuota)), (K2, None)]);
        let resp = search(vec![exa], Mode::General, 10, None, 5_000)
            .await
            .unwrap();
        // The fake puts the serving key in its result URL; drop that row.
        let text = serde_json::to_string(&resp.metadata).unwrap();
        assert!(!text.contains(K1) && !text.contains(K2), "{text}");

        let (exa, _) = keyed(&exa::SPEC, &[(K1, Some(BillingQuota)), (K2, Some(Auth))]);
        let err = search(vec![exa], Mode::General, 10, None, 5_000)
            .await
            .unwrap_err();
        let SearchError::AllProvidersFailed { failed, .. } = err else {
            panic!()
        };
        let text = serde_json::to_string(&failed).unwrap();
        assert!(!text.contains(K1) && !text.contains(K2), "{text}");
    }

    /// Rotation stops at the mode deadline: a key refused after it is the
    /// failure, and the next key is never sent.
    #[tokio::test]
    async fn rotation_stops_at_the_deadline() {
        use FailureCategory::*;
        let (exa, log) = slow_keyed(&exa::SPEC, &[(K1, Some(BillingQuota)), (K2, None)], 150);
        let resp = search(vec![exa], Mode::Similar, 5, None, 200).await;
        // Key 1 answered at 150 ms; key 2 would end after the 200 ms deadline
        // and is sent, then still running at the deadline.
        assert_eq!(tried(&log), [K1, K2]);
        assert!(resp.is_err());

        let (exa, log) = slow_keyed(&exa::SPEC, &[(K1, Some(BillingQuota)), (K2, None)], 250);
        let err = search(vec![exa], Mode::Similar, 5, None, 200)
            .await
            .unwrap_err();
        // Key 1 was still running at the deadline: key 2 never went out.
        assert_eq!(tried(&log), [K1]);
        let SearchError::AllProvidersFailed { failed, .. } = err else {
            panic!()
        };
        assert_eq!(failed[0].category, Timeout);
    }

    /// Chain and single modes rotate keys the same way (similar here).
    #[tokio::test]
    async fn single_mode_rotates_keys_too() {
        let (exa, log) = keyed(
            &exa::SPEC,
            &[(K1, Some(FailureCategory::BillingQuota)), (K2, None)],
        );
        let resp = search(vec![exa], Mode::Similar, 5, None, 5_000)
            .await
            .unwrap();
        assert_eq!(tried(&log), [K1, K2]);
        assert_eq!(resp.metadata.key_slots.get("exa"), Some(&2));
        assert_eq!(resp.results.len(), 1);
    }

    /// Answers empty on its first call, then with one result (or empty
    /// again, or a server error).
    struct EmptyFirst {
        calls: Arc<Mutex<u32>>,
        always_empty: bool,
        fail_second: bool,
        /// The second call never answers.
        hang_second: bool,
        spec: &'static ProviderSpec,
    }

    #[async_trait::async_trait]
    impl Adapter for EmptyFirst {
        fn spec(&self) -> &'static ProviderSpec {
            self.spec
        }
        fn is_configured(&self) -> bool {
            true
        }
        async fn call(&self, _call: &Call<'_>) -> Result<Output, SearchError> {
            let n = {
                let mut c = self.calls.lock().unwrap();
                *c += 1;
                *c
            };
            if n == 2 && self.hang_second {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            if n == 2 && self.fail_second {
                return Err(SearchError::Upstream {
                    provider: "parallel",
                    status: 500,
                    category: FailureCategory::Server,
                    message: "boom".into(),
                });
            }
            let results = if n == 1 || self.always_empty {
                Vec::new()
            } else {
                vec![result("https://p.example/1", "parallel")]
            };
            Ok(Output {
                results,
                cost_micros: Some(5_000),
                ..Output::default()
            })
        }
    }

    /// A failed second ask keeps the first, empty answer (no results, not
    /// a failure); outside the web modes an empty answer is not asked again.
    #[tokio::test]
    async fn a_failed_second_ask_keeps_the_empty_answer() {
        let calls = Arc::new(Mutex::new(0));
        let adapter = Box::new(EmptyFirst {
            calls: calls.clone(),
            always_empty: false,
            fail_second: true,
            hang_second: false,
            spec: &parallel::SPEC,
        }) as Box<dyn Adapter>;
        let resp = search(vec![adapter], Mode::General, 10, None, 5_000)
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), 2);
        assert_eq!(resp.status, "no_results");
        assert!(resp.metadata.providers_failed.is_empty());
        // A refused second call (here a 500) is not billed; the first is.
        assert_eq!(resp.metadata.cost.unwrap().by_provider["parallel"], 5_000);

        let calls = Arc::new(Mutex::new(0));
        let adapter = Box::new(EmptyFirst {
            calls: calls.clone(),
            always_empty: true,
            fail_second: false,
            hang_second: false,
            spec: &serper::SPEC,
        }) as Box<dyn Adapter>;
        let _ = search(vec![adapter], Mode::Places, 10, None, 5_000).await;
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "places: empty is a normal answer"
        );
    }

    /// A second ask that hangs ends before the mode deadline, so the first,
    /// empty answer still stands (no results, not a timeout), with both
    /// calls counted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_hanging_second_ask_keeps_the_empty_answer() {
        for _ in 0..5 {
            let calls = Arc::new(Mutex::new(0));
            let adapter = Box::new(EmptyFirst {
                calls: calls.clone(),
                always_empty: false,
                fail_second: false,
                hang_second: true,
                spec: &parallel::SPEC,
            }) as Box<dyn Adapter>;
            let (others, _) = fakes(&[(&exa::SPEC, ok(0, 2, "e"))]);
            let mut plan = vec![adapter];
            plan.extend(others);
            let resp = search(plan, Mode::General, 10, None, 1_000).await.unwrap();
            assert_eq!(*calls.lock().unwrap(), 2);
            assert_eq!(
                resp.status, "success",
                "{:?}",
                resp.metadata.providers_failed
            );
            assert!(resp.metadata.providers_timed_out.is_empty());
            assert_eq!(resp.metadata.cost.unwrap().by_provider["parallel"], 10_000);
        }
    }

    /// The owner is told only when every key's cause is the owner's: a
    /// rate-limited key beside a rejected one leads with the rate limit.
    #[test]
    fn a_key_refused_for_another_cause_keeps_the_owner_out_of_it() {
        let f = |category: FailureCategory, status: u16, code: &str| ProviderFailure {
            provider: "exa".into(),
            category,
            http_status: Some(status),
            code: code.into(),
            reason: "r".into(),
            retryable: false,
        };
        let rejected = || f(FailureCategory::Auth, 401, "auth_failed");
        let limited = || f(FailureCategory::RateLimit, 429, "rate_limited");
        let spent = || f(FailureCategory::BillingQuota, 402, "credits_exhausted");
        let used = |refused: Vec<ProviderFailure>| KeyUse {
            of: 2,
            tried: 2,
            last: 2,
            refused: refused.into_iter().map(|f| (1, f)).collect(),
            ..KeyUse::default()
        };
        for (first, last) in [(limited(), rejected()), (rejected(), limited())] {
            let out = used(vec![first]).failure("exa", last);
            assert_eq!(out.category, FailureCategory::RateLimit);
            assert!(crate::errors::owner_note(&[out]).is_none());
        }
        let out = used(vec![spent()]).failure("exa", rejected());
        assert_eq!(out.http_status, Some(401));
        assert!(crate::errors::owner_note(&[out])
            .unwrap()
            .contains("every Exa key was rejected or is out of credit"));
        let out = used(vec![spent()]).failure("exa", spent());
        assert!(crate::errors::owner_note(&[out])
            .unwrap()
            .contains("every Exa key is out of credit"));
    }

    /// An empty answer is asked once more before it counts as no results,
    /// and both calls are counted in the cost.
    #[tokio::test]
    async fn an_empty_answer_is_asked_once_more() {
        for always_empty in [false, true] {
            let calls = Arc::new(Mutex::new(0));
            let adapter = Box::new(EmptyFirst {
                calls: calls.clone(),
                always_empty,
                fail_second: false,
                hang_second: false,
                spec: &parallel::SPEC,
            }) as Box<dyn Adapter>;
            let resp = search(vec![adapter], Mode::General, 10, None, 5_000)
                .await
                .unwrap();
            assert_eq!(*calls.lock().unwrap(), 2, "asked twice, never more");
            assert_eq!(resp.results.len(), usize::from(!always_empty));
            assert_eq!(resp.metadata.cost.unwrap().by_provider["parallel"], 10_000);
            assert!(resp
                .metadata
                .diagnostics
                .iter()
                .any(|d| d == "parallel: answered with no results; asked once more"));
        }
    }

    /// One slow platform does not hold up the social fan-out: once the wait
    /// has passed and one provider answered, the straggler is dropped (and
    /// its sent call counted as spent); with nothing answered yet, the
    /// search keeps waiting to the deadline.
    #[tokio::test]
    async fn social_stops_waiting_for_a_straggler_once_one_answered() {
        use crate::providers::{instagram, tiktok, youtube};
        let opts = SearchOpts::default();
        let run = |adapters, query: &'static str| {
            let opts = &opts;
            async move {
                let req = Request {
                    query,
                    mode: Mode::Social,
                    count: 10,
                    only: None,
                    opts,
                    deadline: Duration::from_millis(3_000),
                    straggler_cap: Some(Duration::from_millis(300)),
                    spent: None,
                };
                execute_with(adapters, &req).await
            }
        };
        // No cue, a review: the pick is X and YouTube, nothing else is called.
        let (adapters, log) = fakes(&[
            (&tiktok::SPEC, ok(20, 3, "tiktok")),
            (&youtube::SPEC, ok(150, 3, "youtube")),
            (&instagram::SPEC, ok(20, 3, "instagram")),
            (&xai::SPEC, ok(30_000, 3, "x")),
        ]);
        let started = Instant::now();
        let r = run(adapters, "mechanical keyboard review").await.unwrap();
        let took = started.elapsed();
        assert_eq!(called(&log), ["xai", "youtube"]);
        assert!(took >= Duration::from_millis(280), "{took:?}");
        assert!(
            took < Duration::from_millis(1_500),
            "waited for the straggler: {took:?}"
        );
        assert!(r.metadata.provider_results.contains_key("youtube"));
        assert_eq!(r.metadata.providers_timed_out, ["xai"]);
        let cost = r.metadata.cost.unwrap();
        assert_eq!(cost.by_provider["xai"], xai::SPEC.price.per_call as u64);

        // At most two paid calls, and a platform that cannot run here is
        // stood in for by the next one of the fan-out.
        let (adapters, log) = fakes(&[
            (&tiktok::SPEC, ok(5, 2, "tiktok")),
            (&youtube::SPEC, ok(5, 2, "youtube")),
            (&instagram::SPEC, ok(5, 2, "instagram")),
        ]);
        let r = run(adapters, "mechanical keyboard").await.unwrap();
        assert_eq!(called(&log), ["instagram", "tiktok"]);
        // Fused: the top hit of each platform comes before any second hit.
        let firsts: Vec<&str> = r.results[..2].iter().map(|x| x.source.as_str()).collect();
        assert!(
            firsts.contains(&"tiktok") && firsts.contains(&"instagram"),
            "{firsts:?}"
        );

        // Nobody has answered at the wait: the search waits on.
        let (adapters, _) = fakes(&[
            (&tiktok::SPEC, ok(600, 2, "tiktok")),
            (&youtube::SPEC, ok(30_000, 2, "youtube")),
        ]);
        let started = Instant::now();
        let r = run(adapters, "mechanical keyboard").await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(580));
        assert!(r.metadata.provider_results.contains_key("tiktok"));
        assert_eq!(r.metadata.providers_timed_out, ["youtube"]);

        // A cue pins the platform: only TikTok is called.
        let (adapters, log) = fakes(&[
            (&tiktok::SPEC, ok(5, 2, "tiktok")),
            (&youtube::SPEC, ok(5, 2, "youtube")),
            (&xai::SPEC, ok(5, 2, "x")),
        ]);
        run(adapters, "mechanical keyboard on tiktok")
            .await
            .unwrap();
        assert_eq!(called(&log), ["tiktok"]);
    }

    fn called(log: &Log) -> Vec<String> {
        let mut v: Vec<String> = log.lock().unwrap().iter().map(|(n, _)| n.clone()).collect();
        v.sort();
        v
    }

    #[tokio::test]
    async fn fused_waits_for_slow_providers_and_reports_the_stuck_one() {
        let (adapters, _) = fakes(&[
            (&serper::SPEC, ok(5, 3, "fast")),
            (&exa::SPEC, ok(250, 3, "slow")),
            (&jina::SPEC, ok(30_000, 3, "stuck")),
        ]);
        let started = Instant::now();
        let r = search(adapters, Mode::General, 3, None, 600).await.unwrap();
        let took = started.elapsed();
        // Early stop would have dropped exa once serper filled -c 3.
        assert!(r.metadata.provider_results.contains_key("exa"));
        assert!(r.metadata.provider_results.contains_key("serper"));
        assert_eq!(r.metadata.providers_timed_out, vec!["jina".to_string()]);
        assert!(r.metadata.providers_cancelled.is_empty());
        assert_eq!(r.status, "partial_success");
        assert!(
            took >= Duration::from_millis(550),
            "returned before the deadline"
        );
        assert!(took < Duration::from_secs(5), "waited past the deadline");
        // The stuck call was sent, so it counts as spend.
        let cost = r.metadata.cost.unwrap();
        assert_eq!(cost.by_provider["jina"], jina::SPEC.price.per_call as u64);
    }

    #[tokio::test]
    async fn p_restricts_every_call_in_deep() {
        let all = || {
            fakes(&[
                (&exa::SPEC, ok(1, 2, "exa")),
                (&serper::SPEC, ok(1, 2, "serper")),
                (&brave::SPEC, ok(1, 2, "brave")),
                (&linkup::SPEC, ok(1, 2, "linkup")),
                (&parallel::SPEC, ok(1, 2, "parallel")),
                (&tavily::SPEC, ok(1, 2, "tavily")),
                (&perplexity::SPEC, ok(1, 2, "perplexity")),
                (&xai::SPEC, ok(1, 2, "xai")),
            ])
        };
        for only in ["brave", "exa"] {
            let (adapters, log) = all();
            let list = vec![only.to_string()];
            let r = search(adapters, Mode::Deep, 10, Some(&list), 2_000)
                .await
                .unwrap();
            assert_eq!(called(&log), vec![only.to_string()]);
            assert_eq!(r.metadata.providers_queried, vec![only.to_string()]);
        }
        // Without -p every deep provider runs.
        let (adapters, log) = all();
        search(adapters, Mode::Deep, 10, None, 2_000).await.unwrap();
        assert_eq!(called(&log).len(), 8);
    }

    #[tokio::test]
    async fn scholar_fuses_serper_and_serpapi_concurrently() {
        let (adapters, _) = fakes(&[
            (&serper::SPEC, ok(200, 5, "serper")),
            (&serpapi::SPEC, ok(200, 5, "serpapi")),
        ]);
        let started = Instant::now();
        let r = search(adapters, Mode::Scholar, 4, None, 2_000)
            .await
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(390),
            "ran serially"
        );
        assert_eq!(r.metadata.provider_results.len(), 2);
        assert_eq!(r.results.len(), 4);
        // RRF interleaves the two top-ranked lists.
        let sources: Vec<&str> = r.results.iter().map(|x| x.source.as_str()).collect();
        assert!(sources.contains(&"serper") && sources.contains(&"serpapi"));
    }

    #[tokio::test]
    async fn count_is_clamped_per_provider() {
        let (adapters, log) = fakes(&[(&serper::SPEC, ok(1, 1, "a")), (&exa::SPEC, ok(1, 1, "b"))]);
        search(adapters, Mode::General, 50, None, 2_000)
            .await
            .unwrap();
        let mut counts = log.lock().unwrap().clone();
        counts.sort();
        assert_eq!(
            counts,
            vec![("exa".to_string(), 20), ("serper".to_string(), 10)]
        );
    }

    #[tokio::test]
    async fn answers_do_not_take_result_slots() {
        let act = Act::Ok {
            delay_ms: 1,
            urls: vec!["https://a.example/1".into(), "https://a.example/2".into()],
            answer: Some("the answer"),
            cost: None,
            snippet: "x",
        };
        let (adapters, _) = fakes(&[(&perplexity::SPEC, act)]);
        let r = search(adapters, Mode::General, 2, None, 2_000)
            .await
            .unwrap();
        assert_eq!(r.results.len(), 2);
        assert_eq!(r.metadata.result_count, 2);
        assert_eq!(r.answers.len(), 1);
        assert_eq!(r.answers[0].provider, "perplexity");
    }

    #[tokio::test]
    async fn cost_is_an_estimate_per_provider() {
        let reported = Act::Ok {
            delay_ms: 1,
            urls: vec!["https://t.example".into()],
            answer: None,
            cost: Some(16_000),
            snippet: "x",
        };
        let (adapters, _) = fakes(&[
            (&exa::SPEC, ok(1, 12, "exa")),
            (&tavily::SPEC, reported),
            (&serper::SPEC, Act::Fail(FailureCategory::Auth)),
        ]);
        let r = search(adapters, Mode::General, 20, None, 2_000)
            .await
            .unwrap();
        let cost = r.metadata.cost.unwrap();
        assert_eq!(cost.basis, "estimate");
        // exa: 7,000 + 2 results over 10 × 1,000; tavily reported its own.
        assert_eq!(cost.by_provider["exa"], 9_000);
        assert_eq!(cost.by_provider["tavily"], 16_000);
        // A rejected key is not billed.
        assert!(!cost.by_provider.contains_key("serper"));
        assert_eq!(cost.micros, 25_000);
        assert!((cost.usd - 0.025).abs() < 1e-9);
        assert_eq!(r.metadata.transport["exa"], "direct");
    }

    /// A paid call whose 200 the mode rejected (a challenge page) was still
    /// paid for: it counts, at the adapter's reported cost or the estimate.
    #[tokio::test]
    async fn rejected_billed_output_counts_as_spend() {
        let challenge = Act::Ok {
            delay_ms: 1,
            urls: vec!["https://site.example/page".into()],
            answer: None,
            cost: Some(700),
            snippet: "Just a moment...",
        };
        let (adapters, _) = fakes(&[(&jina::SPEC, challenge), (&firecrawl::SPEC, ok(1, 1, "fc"))]);
        let r = search(adapters, Mode::Extract, 1, None, 2_000)
            .await
            .unwrap();
        let cost = r.metadata.cost.unwrap();
        assert_eq!(cost.by_provider["jina"], 700);
        assert_eq!(cost.by_provider["firecrawl"], 5_000);
    }

    /// A 2xx whose body did not parse was billed upstream.
    #[tokio::test]
    async fn parse_failure_on_a_billed_provider_counts_as_spend() {
        let (adapters, _) = fakes(&[
            (&serper::SPEC, ok(1, 2, "s")),
            (&exa::SPEC, Act::Fail(FailureCategory::Parse)),
        ]);
        let r = search(adapters, Mode::General, 5, None, 2_000)
            .await
            .unwrap();
        let cost = r.metadata.cost.unwrap();
        assert_eq!(cost.by_provider["exa"], 7_000);
        assert_eq!(cost.by_provider["serper"], 1_000);
    }

    /// A call still running at the deadline is priced at what was sent for
    /// the mode (Tavily always sends the advanced depth: 16,000), not the
    /// base rate.
    #[tokio::test]
    async fn timeouts_are_priced_at_the_adapters_sent_cost() {
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        let adapters: Vec<Box<dyn Adapter>> = vec![
            Box::new(Fake {
                spec: &serper::SPEC,
                act: ok(1, 2, "s"),
                log: log.clone(),
                sent: None,
            }),
            Box::new(Fake {
                spec: &tavily::SPEC,
                act: ok(5_000, 2, "t"),
                log,
                sent: Some(16_000),
            }),
        ];
        let r = search(adapters, Mode::General, 5, None, 300).await.unwrap();
        assert_eq!(r.metadata.providers_timed_out, vec!["tavily".to_string()]);
        assert_eq!(r.metadata.cost.unwrap().by_provider["tavily"], 16_000);
    }

    fn urls(urls: &[&str], snippet: &'static str) -> Act {
        Act::Ok {
            delay_ms: 1,
            urls: urls.iter().map(|u| u.to_string()).collect(),
            answer: None,
            cost: None,
            snippet,
        }
    }

    /// Serper places rows carry no `link`, so every url is "": a single
    /// provider's rows come back as sent, never merged into one.
    #[tokio::test]
    async fn places_rows_without_url_are_all_kept() {
        let (adapters, _) = fakes(&[(&serper::SPEC, urls(&[""; 5], "a cafe"))]);
        let r = search(adapters, Mode::Places, 5, None, 2_000)
            .await
            .unwrap();
        assert_eq!(r.results.len(), 5);
    }

    /// Images from the same source page share a `link`: single mode keeps
    /// the provider's rows and order.
    #[tokio::test]
    async fn single_mode_keeps_rows_that_share_a_url() {
        let page = "https://site.example/gallery";
        let (adapters, _) = fakes(&[(&serper::SPEC, urls(&[page, page, page], "img"))]);
        let r = search(adapters, Mode::Images, 10, None, 2_000)
            .await
            .unwrap();
        assert_eq!(r.results.len(), 3);
    }

    /// Fusion never treats two link-less rows as the same result.
    #[tokio::test]
    async fn fused_rows_without_url_are_not_merged() {
        let (adapters, _) = fakes(&[
            (&serper::SPEC, urls(&["https://a.org/1", "", ""], "paper")),
            (&serpapi::SPEC, urls(&["", "https://b.org/2"], "paper")),
        ]);
        let r = search(adapters, Mode::Scholar, 10, None, 2_000)
            .await
            .unwrap();
        assert_eq!(r.results.len(), 5);
        assert!(r
            .results
            .iter()
            .filter(|x| x.url.is_empty())
            .all(|x| x.extra.is_none()));
    }

    #[tokio::test]
    async fn p_outside_the_mode_is_bad_input() {
        let (adapters, log) = fakes(&[
            (&serper::SPEC, ok(1, 1, "a")),
            (&firecrawl::SPEC, ok(1, 1, "b")),
        ]);
        let list = vec!["firecrawl".to_string()];
        let err = search(adapters, Mode::General, 5, Some(&list), 2_000)
            .await
            .unwrap_err();
        assert_eq!(err.exit_code(), 3);
        assert!(err.to_string().contains("does not serve mode 'general'"));
        assert!(log.lock().unwrap().is_empty(), "nothing may run");
    }

    #[tokio::test]
    async fn chain_moves_past_rejected_output() {
        let challenge = Act::Ok {
            delay_ms: 1,
            urls: vec!["https://site.example/page".into()],
            answer: None,
            cost: None,
            snippet: "Just a moment...",
        };
        let (adapters, log) =
            fakes(&[(&jina::SPEC, challenge), (&firecrawl::SPEC, ok(1, 1, "fc"))]);
        let r = search(adapters, Mode::Scrape, 1, None, 2_000)
            .await
            .unwrap();
        assert_eq!(r.mode, "scrape");
        assert_eq!(r.results[0].source, "firecrawl");
        assert_eq!(
            r.metadata.provider_failures[0].category,
            FailureCategory::Rejected
        );
        assert_eq!(r.status, "partial_success");
        assert_eq!(
            called(&log),
            vec!["firecrawl".to_string(), "jina".to_string()]
        );
        assert_eq!(
            r.results[0].extra.as_ref().unwrap()["content_origin"],
            "untrusted_web_content"
        );
    }

    #[tokio::test]
    async fn chain_gates_every_rung_on_a_url_direct_saw_blocked() {
        let url = "https://chain-gate.example/blocked";
        registry::note_direct(url, registry::DirectSaw::Blocked);
        let (adapters, log) = fakes(&[
            (&jina::SPEC, ok(1, 1, "j")),
            (&firecrawl::SPEC, ok(1, 1, "fc")),
        ]);
        let opts = SearchOpts::default();
        let req = Request {
            query: url,
            mode: Mode::Extract,
            count: 1,
            only: None,
            opts: &opts,
            deadline: Duration::from_millis(2_000),
            straggler_cap: None,
            spent: None,
        };
        let _ = execute_with(adapters, &req).await;
        assert!(called(&log).is_empty(), "a gated rung was sent the URL");
    }

    #[tokio::test]
    async fn chain_stops_at_the_first_accepted_output() {
        let (adapters, log) = fakes(&[
            (&jina::SPEC, ok(1, 1, "j")),
            (&firecrawl::SPEC, ok(1, 1, "fc")),
        ]);
        let r = search(adapters, Mode::Extract, 1, None, 2_000)
            .await
            .unwrap();
        assert_eq!(called(&log), vec!["jina".to_string()]);
        assert_eq!(r.metadata.transport.len(), 1);
    }

    #[tokio::test]
    async fn empty_results_with_one_failure_is_no_results_not_total_failure() {
        let (adapters, _) = fakes(&[
            (&serper::SPEC, ok(1, 0, "a")),
            (&exa::SPEC, Act::Fail(FailureCategory::Server)),
        ]);
        let r = search(adapters, Mode::General, 5, None, 2_000)
            .await
            .unwrap();
        assert_eq!(r.status, "no_results");

        let (adapters, _) = fakes(&[
            (&serper::SPEC, Act::Fail(FailureCategory::Auth)),
            (&exa::SPEC, Act::Fail(FailureCategory::BillingQuota)),
        ]);
        let err = search(adapters, Mode::General, 5, None, 2_000)
            .await
            .unwrap_err();
        assert_eq!(err.exit_code(), 2);
    }

    #[test]
    fn url_vectors_give_their_keys() {
        // The five URL vectors.
        assert_eq!(
            normalize_url("https://WWW.Example.com/Post/?utm_source=x#a"),
            "example.com/Post"
        );
        assert_eq!(
            normalize_url("https://example.com/post/"),
            normalize_url("https://example.com/post")
        );
        assert_ne!(
            normalize_url("https://youtube.com/watch?v=AbC"),
            normalize_url("https://youtube.com/watch?v=abc")
        );
        assert_eq!(
            normalize_url("https://example.com/p?b=2&a=1&gclid=z"),
            "example.com/p?a=1&b=2"
        );
        assert_eq!(
            normalize_url("http://example.com/x"),
            normalize_url("https://example.com/x")
        );
    }

    #[test]
    fn url_key_keeps_paths_and_queries_intact() {
        assert!(normalize_url("https://site.com/files/www.report.pdf").contains("www.report.pdf"));
        assert!(normalize_url("https://a.com/x?redirect=http://b.com")
            .contains("redirect=http://b.com"));
        assert_ne!(
            normalize_url("https://x.com/r?page=1"),
            normalize_url("https://x.com/r?page=2")
        );
        assert_eq!(normalize_url("https://www.example.com/"), "example.com");
    }

    #[test]
    fn rrf_consensus_beats_single_provider_top_rank() {
        let buckets = vec![
            (
                "a".to_string(),
                vec![
                    result("https://only-a.com", "a"),
                    result("https://both.com", "a"),
                ],
            ),
            (
                "b".to_string(),
                vec![
                    result("https://only-b.com", "b"),
                    result("https://both.com", "b"),
                ],
            ),
        ];
        let fused = fuse_rrf(buckets);
        assert_eq!(fused[0].url, "https://both.com");
        assert_eq!(fused.len(), 3);
    }

    #[test]
    fn rrf_is_independent_of_bucket_arrival_order() {
        let mk = |first: &str, second: &str| {
            vec![
                (
                    first.to_string(),
                    vec![
                        result("https://x.com", first),
                        result("https://y.com", first),
                    ],
                ),
                (
                    second.to_string(),
                    vec![
                        result("https://y.com", second),
                        result("https://z.com", second),
                    ],
                ),
            ]
        };
        let top = |b| fuse_rrf(b).into_iter().next().unwrap().url;
        assert_eq!(top(mk("a", "b")), "https://y.com");
        assert_eq!(top(mk("b", "a")), "https://y.com");
    }

    #[test]
    fn rrf_records_consensus_in_extra() {
        let buckets = vec![
            ("a".to_string(), vec![result("https://both.com", "a")]),
            ("b".to_string(), vec![result("https://both.com", "b")]),
        ];
        let fused = fuse_rrf(buckets);
        let also = fused[0].extra.as_ref().unwrap()["also_found_by"].clone();
        assert_eq!(also, serde_json::json!(["b"]));
    }

    #[test]
    fn filter_warnings_name_providers_that_drop_filters() {
        let opts = SearchOpts {
            freshness: Some("week".into()),
            country: Some("gb".into()),
            ..SearchOpts::default()
        };
        let specs: HashMap<&'static str, FilterSupport> = providers::all()
            .iter()
            .map(|s| (s.name, s.filters))
            .collect();
        let w = filter_warnings(
            Mode::General,
            &["jina".into(), "serper".into()],
            &specs,
            &opts,
        );
        assert_eq!(w.len(), 1);
        assert!(w[0].starts_with("jina does not apply -f freshness or --country"));
        let w = filter_warnings(Mode::Places, &["serper".into()], &specs, &opts);
        assert!(w[0].contains("mode 'places' does not apply"));
        assert!(filter_warnings(
            Mode::General,
            &["jina".into()],
            &specs,
            &SearchOpts::default()
        )
        .is_empty());
    }

    #[test]
    fn filter_warnings_follow_what_was_sent_not_the_spec() {
        // An adapter that sends freshness only: -d is dropped, and the
        // X-handle caveat would be false.
        let opts = SearchOpts {
            include_domains: vec!["nature.com".into()],
            ..SearchOpts::default()
        };
        let sent = FilterSupport {
            freshness: true,
            domains: false,
            country: false,
            lang: false,
        };
        let applied = HashMap::from([("xai", sent)]);
        let w = filter_warnings(Mode::Deep, &["xai".into()], &applied, &opts);
        assert_eq!(
            w,
            vec!["xai does not apply -d/--exclude-domain; its results are unfiltered".to_string()]
        );
    }
}
