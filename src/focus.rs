//! A focused read of one page (`-m extract --goal "<what I need>"`): the
//! page's outline with character offsets, then the passages that serve the
//! need, about [`BUDGET`] characters in all, so an agent reads what it needs
//! and opens any section in full with `--from <offset>`.
//!
//! The passages come from Parallel's extract with the need as its objective
//! when it answered (each placed in the page text by [`find_offset`]),
//! otherwise from [`local_passages`]: the page cut into chunks of about
//! [`CHUNK`] characters, scored with BM25 against the need's words (English
//! and Danish stop words dropped, words cut to [`STEM`] characters so
//! inflections and Danish compounds match), the best widened by [`CONTEXT`]
//! characters on each side and shown in page order.
//!
//! The method and its numbers are the ones tested in the research
//! department's round 17 (the same agent read about 30 % less, at about 24 %
//! lower model cost, with the same checklist coverage). Every offset counts
//! characters, not bytes, as `--from` does.

use std::collections::{HashMap, HashSet};

/// Characters of passage text in one focused view (Parallel's
/// `max_chars_total` too). A page this short is shown whole.
pub const BUDGET: usize = 8_000;
/// Target chunk size for scoring.
const CHUNK: usize = 700;
/// Characters of context added on each side of a chosen chunk.
const CONTEXT: usize = 300;
/// Words longer than this are compared by their first `STEM` characters.
const STEM: usize = 7;
/// Outline entries; the deepest levels are dropped first when there are more.
const OUTLINE_MAX: usize = 60;

const STOP: &str = "the a an of in on at to for and or is was were are be been by with from that this which what how did does do it its as he she his her they their than then into about over under after before not no has have had who whom whose when where why will would can could should may might also more most other some such only own same so too very s t just don now per og i at en et den det de der som er var på til af med for har ikke hvad hvor hvilke hvilken hvor mange ifølge blev fra om";

fn is_tok_char(c: char) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_digit() || "æøåäöüß".contains(c)
}

/// Lower-cased words of `s` without stop words or single letters; a number
/// keeps its decimal part ("1.5", "2,25").
fn toks(s: &str) -> Vec<String> {
    let stop: HashSet<&str> = STOP.split(' ').collect();
    let lower: Vec<char> = s.to_lowercase().chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lower.len() {
        if !is_tok_char(lower[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < lower.len() && is_tok_char(lower[i]) {
            i += 1;
        }
        while i + 1 < lower.len() && matches!(lower[i], '.' | ',') && lower[i + 1].is_ascii_digit()
        {
            i += 1;
            while i < lower.len() && lower[i].is_ascii_digit() {
                i += 1;
            }
        }
        let w: String = lower[start..i].iter().collect();
        if w.chars().count() > 1 && !stop.contains(w.as_str()) {
            out.push(w);
        }
    }
    out
}

fn stems(s: &str) -> Vec<String> {
    toks(s)
        .into_iter()
        .map(|w| w.chars().take(STEM).collect())
        .collect()
}

/// One heading: its character offset, its depth (1 = top) and its text.
#[derive(Debug, Clone, PartialEq)]
pub struct Heading {
    pub offset: usize,
    pub depth: usize,
    pub title: String,
}

/// A section number such as `2`, `1.3` or `4.2.1.` (one or two digits per
/// part, at most five parts), without its trailing dot; and its depth.
fn section_number(s: &str) -> Option<(String, usize)> {
    let s = s.strip_suffix('.').unwrap_or(s);
    let parts: Vec<&str> = s.split('.').collect();
    let ok = parts.len() <= 5
        && parts
            .iter()
            .all(|p| (1..=2).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_digit()));
    ok.then(|| (s.to_string(), parts.len()))
}

/// Markdown's escapes (`1\.1`, `\-`) removed, so "1\.1 Title" reads as
/// a section number.
fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' && chars.peek().is_some_and(|n| n.is_ascii_punctuation()) {
            continue;
        }
        out.push(c);
    }
    out
}

/// A contents line's dotted leader ("Title ........ 4").
fn leader_dots(t: &str) -> bool {
    t.contains("...") || t.contains(". . .") || t.contains(".. .") || t.contains(". ..")
}

/// A numbered heading's title: capitalised, short, not a contents line
/// ("Title ....... 4") or one ending in a page number.
fn title_ok(t: &str) -> Option<String> {
    let t = t.trim().trim_end_matches([' ', '.']);
    let n = t.chars().count();
    let first = t.chars().next()?;
    let dotted = leader_dots(t);
    let ends_in_number = t
        .rsplit_once(char::is_whitespace)
        .is_some_and(|(_, last)| !last.is_empty() && last.bytes().all(|b| b.is_ascii_digit()));
    (n > 2
        && n < 100
        && first.is_alphabetic()
        && first.is_uppercase()
        && t.split_whitespace().count() <= 14
        && !dotted
        && !ends_in_number)
        .then(|| t.to_string())
}

/// Markdown headings (`# …`) and numbered section headings ("1.2.3 Title"
/// on one line, or the number alone with the title on the next line, as PDF
/// text often has them). A contents list and repeated running headers are
/// skipped: each heading is kept once, at its first place after the
/// contents list.
pub fn outline(text: &str) -> Vec<Heading> {
    let lines: Vec<&str> = text.split('\n').collect();
    let mut pos = Vec::with_capacity(lines.len());
    let mut off = 0;
    for l in &lines {
        pos.push(off);
        off += l.chars().count() + 1;
    }
    let mut found: Vec<Heading> = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let s = unescape(line.trim());
        let s = s.as_str();
        let hashes = s.chars().take_while(|c| *c == '#').count();
        if (1..=6).contains(&hashes) {
            let rest = &s[hashes..];
            let title = rest.trim().trim_matches([' ', '#', '*']);
            if rest.starts_with(char::is_whitespace)
                && (2..=120).contains(&rest.trim().chars().count())
                && !leader_dots(title)
            {
                found.push(Heading {
                    offset: pos[i],
                    depth: hashes,
                    title: title.to_string(),
                });
            }
            continue;
        }
        let (head, rest) = match s.split_once(char::is_whitespace) {
            Some((h, r)) => (h, Some(r)),
            None => (s, None),
        };
        let Some((number, depth)) = section_number(head) else {
            continue;
        };
        let title = match rest {
            Some(r) => title_ok(r),
            // The number alone: the title on one of the next two lines.
            None => lines[i + 1..lines.len().min(i + 3)]
                .iter()
                .map(|l| l.trim())
                .find(|l| !l.is_empty())
                .and_then(title_ok),
        };
        if let Some(t) = title {
            found.push(Heading {
                offset: pos[i],
                depth,
                title: format!("{number} {t}"),
            });
        }
    }
    // A contents list ends where the first heading is seen a second time,
    // when that happens early in the page.
    let total = off;
    let mut seen: HashSet<String> = HashSet::new();
    let mut toc_end = 0;
    for h in &found {
        if !seen.insert(h.title.to_lowercase()) {
            if h.offset * 5 < total {
                toc_end = h.offset;
            }
            break;
        }
    }
    let mut keep: HashSet<String> = HashSet::new();
    let mut out: Vec<Heading> = found
        .into_iter()
        .filter(|h| !(toc_end > 0 && h.offset < toc_end) && keep.insert(h.title.to_lowercase()))
        .collect();
    let mut depth = out.iter().map(|h| h.depth).max().unwrap_or(1);
    while out.len() > OUTLINE_MAX && depth > 1 {
        out.retain(|h| h.depth < depth);
        depth -= 1;
    }
    out.truncate(OUTLINE_MAX);
    out
}

fn heading_at(outline: &[Heading], offset: usize) -> Option<&str> {
    outline
        .iter()
        .take_while(|h| h.offset <= offset)
        .last()
        .map(|h| h.title.as_str())
}

fn rfind(chars: &[char], pat: &[char], from: usize, to: usize) -> Option<usize> {
    let to = to.min(chars.len());
    if to < from + pat.len() {
        return None;
    }
    (from..=to - pat.len())
        .rev()
        .find(|&i| chars[i..i + pat.len()] == *pat)
}

/// Spans of about [`CHUNK`] characters, cut at blank lines, else at line breaks.
fn chunks(chars: &[char]) -> Vec<(usize, usize)> {
    let n = chars.len();
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let mut end = (start + CHUNK).min(n);
        if end < n {
            let (lo, hi) = (start + CHUNK / 2, end + CHUNK / 3);
            end = rfind(chars, &['\n', '\n'], lo, hi)
                .or_else(|| rfind(chars, &['\n'], lo, hi))
                .unwrap_or(end);
        }
        out.push((start, end));
        start = end;
    }
    out
}

fn merge(mut spans: Vec<(usize, usize)>) -> Vec<(usize, usize)> {
    spans.sort_unstable();
    let mut out: Vec<(usize, usize)> = Vec::new();
    for (a, z) in spans {
        match out.last_mut() {
            Some(last) if a <= last.1 => last.1 = last.1.max(z),
            _ => out.push((a, z)),
        }
    }
    out
}

/// The spans of the page that best match `goal`, in page order, about
/// `budget` characters in all (character offsets).
pub fn local_passages(text: &str, goal: &str, budget: usize) -> Vec<(usize, usize)> {
    let chars: Vec<char> = text.chars().collect();
    let n = chars.len();
    let q: HashSet<String> = stems(goal).into_iter().collect();
    let cs: Vec<(usize, usize, Vec<String>)> = chunks(&chars)
        .into_iter()
        .map(|(s, e)| (s, e, stems(&chars[s..e].iter().collect::<String>())))
        .collect();
    let count = cs.len().max(1) as f64;
    let mut df: HashMap<&str, usize> = HashMap::new();
    for (_, _, tk) in &cs {
        let distinct: HashSet<&str> = tk.iter().map(String::as_str).collect();
        for w in distinct {
            if q.contains(w) {
                *df.entry(w).or_default() += 1;
            }
        }
    }
    let idf = |w: &str| {
        let d = df.get(w).copied().unwrap_or(0) as f64;
        (1.0 + (count - d + 0.5) / (d + 0.5)).ln()
    };
    let avg = (cs.iter().map(|(s, e, _)| e - s).sum::<usize>() as f64 / count).max(1.0);
    let (k1, b) = (1.2, 0.75);
    let mut scored: Vec<(f64, usize, usize)> = Vec::new();
    for (s, e, tk) in &cs {
        let mut tf: HashMap<&str, f64> = HashMap::new();
        for w in tk {
            if q.contains(w) {
                *tf.entry(w.as_str()).or_default() += 1.0;
            }
        }
        let len = (e - s) as f64;
        let score: f64 = tf
            .iter()
            .map(|(w, f)| idf(w) * f * (k1 + 1.0) / (f + k1 * (1.0 - b + b * len / avg)))
            .sum();
        if score > 0.0 {
            scored.push((score, *s, *e));
        }
    }
    scored.sort_by(|x, y| {
        y.0.partial_cmp(&x.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(y.1.cmp(&x.1))
    });
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut used = 0;
    for (_, s, e) in scored {
        let a = if s > CONTEXT {
            rfind(&chars, &['\n'], 0, s - CONTEXT).map_or(0, |i| i + 1)
        } else {
            0
        };
        let from = (e + CONTEXT).min(n);
        let z = chars[from..]
            .iter()
            .position(|c| *c == '\n')
            .map_or(n, |i| from + i);
        let mut next = spans.clone();
        next.push((a, z));
        let next = merge(next);
        let size: usize = next.iter().map(|(x, y)| y - x).sum();
        if size > budget {
            if used * 5 >= budget * 3 {
                break;
            }
            continue;
        }
        spans = next;
        used = size;
    }
    spans
}

/// Words (letters, digits, `_`) with their character offsets.
fn words(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut start = 0;
    for (i, c) in text.chars().enumerate() {
        if c.is_alphanumeric() || c == '_' {
            if cur.is_empty() {
                start = i;
            }
            cur.push(c);
        } else if !cur.is_empty() {
            out.push((start, std::mem::take(&mut cur)));
        }
    }
    if !cur.is_empty() {
        out.push((start, cur));
    }
    out
}

/// Where an excerpt from a service sits in the page text: where a run of
/// eight of its words appears there in the same order, trying runs from a
/// few places in the excerpt. A run found once wins over one found several
/// times (a passage that opens like another, or a running page header);
/// offsets in `taken` are skipped. `None` when the excerpt is too short to
/// place or not found.
pub fn find_offset(
    page_words: &[(usize, String)],
    excerpt: &str,
    taken: &[usize],
) -> Option<usize> {
    let ex: Vec<String> = words(excerpt).into_iter().map(|(_, w)| w).collect();
    if ex.len() < 6 || page_words.is_empty() {
        return None;
    }
    let step = (ex.len() / 4).max(1);
    let last = ex.len().saturating_sub(8).max(1);
    let mut fallback = None;
    for i in (0..last).step_by(step) {
        let run = &ex[i..(i + 8).min(ex.len())];
        let mut found = page_words
            .windows(run.len())
            .filter(|w| w.iter().map(|(_, s)| s).eq(run.iter()))
            .map(|w| w[0].0)
            .filter(|o| !taken.contains(o));
        let Some(first) = found.next() else {
            continue;
        };
        if found.next().is_none() {
            return Some(first);
        }
        fallback = fallback.or(Some(first));
    }
    fallback
}

/// Parallel joins an excerpt's separate parts with a line of `...`.
pub fn split_excerpts(excerpts: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for blob in excerpts {
        let mut cur = String::new();
        for line in blob.split('\n') {
            if line.trim() == "..." {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            } else {
                cur.push_str(line);
                cur.push('\n');
            }
        }
        if !cur.trim().is_empty() {
            out.push(cur.trim().to_string());
        }
    }
    out
}

/// The text an agent reads for a focused read of `text` (the whole page as
/// the reading chain extracted it). `excerpts`: the passages Parallel chose
/// for the need, or `None` to choose them here.
pub fn render(text: &str, url: &str, goal: &str, excerpts: Option<&[String]>) -> String {
    let n = text.chars().count();
    if n <= BUDGET {
        return format!("[the page is {n} characters, so it is shown whole]\n\n{text}");
    }
    let ol = outline(text);
    let mut out = format!("[page length {n} characters; focused view for: {goal}]\n\n");
    if ol.is_empty() {
        out.push_str("Outline: no headings detected.\n");
    } else {
        out.push_str("Outline (offset: heading):\n");
        for h in &ol {
            out.push_str(&format!(
                "  {}: {}{}\n",
                h.offset,
                "  ".repeat(h.depth.saturating_sub(1)),
                h.title
            ));
        }
    }
    let how = format!("read any part in full: search search -q \"{url}\" --from <offset>");
    match excerpts {
        Some(excerpts) => {
            out.push_str(&format!(
                "\nPassages relevant to the need, chosen by Parallel ({how}):\n"
            ));
            let page_words = words(text);
            let mut taken = Vec::new();
            for ex in excerpts {
                let at = find_offset(&page_words, ex, &taken);
                taken.extend(at);
                let label = match at {
                    Some(o) => match heading_at(&ol, o) {
                        Some(h) => format!("[offset {o}] under \"{h}\""),
                        None => format!("[offset {o}]"),
                    },
                    None => "[offset not found in the page text]".to_string(),
                };
                out.push_str(&format!("\n--- {label}\n{}\n", ex.trim()));
            }
        }
        None => {
            out.push_str(&format!(
                "\nPassages that best match the need, in page order ({how}):\n"
            ));
            let spans = local_passages(text, goal, BUDGET);
            if spans.is_empty() {
                out.push_str("(no passage matches the need's words)\n");
            }
            let chars: Vec<char> = text.chars().collect();
            for (a, z) in spans {
                let label = match heading_at(&ol, a) {
                    Some(h) => format!("[{a}-{z}] under \"{h}\""),
                    None => format!("[{a}-{z}]"),
                };
                let passage: String = chars[a..z].iter().collect();
                out.push_str(&format!("\n--- {label}\n{}\n", passage.trim()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_drop_stop_words_and_keep_decimals() {
        assert_eq!(
            toks("The FCR price is 1.5 EUR/MW og prisen på Primærreserve"),
            [
                "fcr",
                "price",
                "1.5",
                "eur",
                "mw",
                "prisen",
                "primærreserve"
            ]
        );
        assert_eq!(stems("frequency reserves")[0], "frequen");
    }

    #[test]
    fn outline_finds_markdown_and_numbered_headings_after_the_contents() {
        let text = "Title page\n\n1 Systemydelser\n2 Kommercielle betingelser ........ 4\n\n\
                    # Report\n\nIntro text.\n\n1 Systemydelser\n\nText about it.\n\n1.1\nPrimær reserve, DK1 (FCR)\n\
                    More text.\n\n## Prices\n\n2 Kommercielle betingelser\n\n3 MW is not a heading 12\n"
            .to_string()
            + &"Body text.\n".repeat(40);
        let ol = outline(&text);
        let titles: Vec<&str> = ol.iter().map(|h| h.title.as_str()).collect();
        assert_eq!(
            titles,
            [
                // Everything before the contents list ends is skipped.
                "1 Systemydelser",
                "1.1 Primær reserve, DK1 (FCR)",
                "Prices",
                "2 Kommercielle betingelser"
            ]
        );
        // Offsets count characters (æ is one), so --from finds the line.
        let at = ol[1].offset;
        let rest: String = text.chars().skip(at).take(3).collect();
        assert_eq!(rest, "1.1");
        assert_eq!(ol[1].depth, 2);
    }

    /// Parallel's markdown of a PDF escapes section numbers and keeps the
    /// contents list as headings with dotted leaders.
    #[test]
    fn escaped_numbers_and_contents_lines_in_markdown() {
        let text = "## 1\\. Systemydelser ........ 4\n\n### 1\\. Systemydelser\n\nText.\n\n1\\.1 Primær reserve, DK1 (FCR)\n\nMore.\n";
        let titles: Vec<String> = outline(text).into_iter().map(|h| h.title).collect();
        assert_eq!(
            titles,
            ["1. Systemydelser", "1.1 Primær reserve, DK1 (FCR)"]
        );
    }

    #[test]
    fn local_passages_pick_the_matching_part_within_the_budget() {
        let filler = "Unrelated words about something else entirely.\n".repeat(200);
        let text =
            format!("{filler}\nThe delivery duration for FCR in DK1 is 30 minutes.\n\n{filler}");
        let spans = local_passages(&text, "FCR delivery duration DK1", 2_000);
        assert_eq!(spans.len(), 1);
        let (a, z) = spans[0];
        let chars: Vec<char> = text.chars().collect();
        let got: String = chars[a..z].iter().collect();
        assert!(got.contains("30 minutes"), "{got}");
        assert!(z - a <= 2_000);
        assert!(local_passages(&text, "nothing matches zzz", 2_000).is_empty());
    }

    #[test]
    fn excerpts_are_placed_by_a_run_of_their_words() {
        let text = "Ærø intro.\n\nEnerginet buys the primary reserve through daily auctions; the need is published on the website.";
        let w = words(text);
        let o = find_offset(
            &w,
            "buys the primary reserve through daily auctions; the need",
            &[],
        )
        .unwrap();
        let rest: String = text.chars().skip(o).take(4).collect();
        assert_eq!(rest, "buys");
        assert_eq!(find_offset(&w, "too short", &[]), None);
        let twice =
            "One two three four five six seven eight. One two three four five six seven eight.";
        let w2 = words(twice);
        let first = find_offset(&w2, "One two three four five six seven eight", &[]).unwrap();
        let second = find_offset(&w2, "One two three four five six seven eight", &[first]).unwrap();
        assert_eq!((first, second), (0, 41));
        assert_eq!(
            find_offset(
                &w,
                "a completely different sentence that the page never had",
                &[]
            ),
            None
        );
    }

    #[test]
    fn parallel_parts_are_split_on_dot_lines() {
        let got = split_excerpts(&["first part\n...\nsecond part\n\n...\n".to_string()]);
        assert_eq!(got, ["first part", "second part"]);
    }

    #[test]
    fn a_short_page_is_shown_whole_and_a_long_one_focused() {
        let short = render("Short page.", "https://x.dk/a", "need", None);
        assert!(short.ends_with("Short page."));
        let long = format!(
            "# Market requirements\n\n{}\n## DK1 FCR\n\nDelivery duration: 30 minutes.\n",
            "Cookie text and navigation.\n".repeat(400)
        );
        let view = render(&long, "https://x.dk/a", "DK1 FCR delivery duration", None);
        assert!(view.contains("Outline (offset: heading):\n  0: Market requirements\n"));
        assert!(view.contains("Delivery duration: 30 minutes."));
        assert!(view.contains("--from <offset>"));
        assert!(view.chars().count() < BUDGET + 1_000);
        let ex = vec![
            "Delivery duration: 30 minutes. That is the rule for every unit here.".to_string(),
        ];
        let view = render(&long, "https://x.dk/a", "duration", Some(&ex));
        assert!(view.contains("chosen by Parallel"));
    }
}
