//! Telegram `ParseMode::Html` safety: escaping, a strict validator for the subset Telegram
//! accepts, a plain-text fallback and length-safe truncation.
//!
//! Telegram rejects the *whole* message when one `<`, `>` or `&` is not part of a tag or entity
//! ("can't parse entities"), so a single unescaped exit reason, role name ("Treasury & Core") or
//! attacker-chosen string from a launch's calldata used to make an alert or a menu vanish.

/// Telegram's hard limit on message text (after entity parsing), in UTF-16 code units.
pub const MAX_MESSAGE_CHARS: usize = 4096;

const ALLOWED_TAGS: &[&str] = &[
    "b", "strong", "i", "em", "u", "ins", "s", "strike", "del", "span", "tg-spoiler", "a", "code", "pre", "blockquote",
    "tg-emoji",
];
/// Tags that may carry attributes (`<a href>`, `<span class>`, `<code class>`, ...).
const TAGS_WITH_ATTRIBUTES: &[&str] = &["a", "span", "code", "pre", "blockquote", "tg-emoji"];

/// Escape text for `ParseMode::Html`.
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 8);
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// Length of an entity starting at `s[0] == '&'`, if it is one Telegram accepts.
fn entity_len(s: &str) -> Option<usize> {
    for named in ["&lt;", "&gt;", "&amp;", "&quot;"] {
        if s.starts_with(named) {
            return Some(named.len());
        }
    }
    let rest = s.strip_prefix("&#")?;
    let (digits, hex) = match rest.strip_prefix(['x', 'X']) {
        Some(h) => (h, true),
        None => (rest, false),
    };
    let n = digits.chars().take_while(|c| if hex { c.is_ascii_hexdigit() } else { c.is_ascii_digit() }).count();
    if n == 0 || n > 8 || !digits[n..].starts_with(';') {
        return None;
    }
    Some(2 + usize::from(hex) + n + 1)
}

/// Walk `text` as Telegram would. `Ok(open tags)` (outermost first) if every `<`, `>` and `&` is
/// part of a well-formed, allowed tag or an accepted entity and closing tags match.
fn scan(text: &str) -> Result<Vec<String>, String> {
    let mut stack: Vec<String> = Vec::new();
    let mut i = 0;
    while i < text.len() {
        let rest = &text[i..];
        let c = rest.chars().next().unwrap_or_default();
        match c {
            '&' => {
                let n = entity_len(rest).ok_or_else(|| format!("bare '&' at byte {i}"))?;
                i += n;
            }
            '>' => return Err(format!("bare '>' at byte {i}")),
            '<' => {
                let end = rest.find('>').ok_or_else(|| format!("unterminated tag at byte {i}"))?;
                let inner = &rest[1..end];
                if inner.contains('<') {
                    return Err(format!("'<' inside a tag at byte {i}"));
                }
                if let Some(name) = inner.strip_prefix('/') {
                    let name = name.trim();
                    match stack.pop() {
                        Some(open) if open == name => {}
                        Some(open) => return Err(format!("</{name}> closes <{open}> at byte {i}")),
                        None => return Err(format!("</{name}> without an open tag at byte {i}")),
                    }
                } else {
                    let name = inner.split(|ch: char| ch.is_whitespace()).next().unwrap_or_default();
                    if !ALLOWED_TAGS.contains(&name) {
                        return Err(format!("tag <{name}> not supported by Telegram at byte {i}"));
                    }
                    let attrs = inner[name.len()..].trim();
                    if !attrs.is_empty() && !TAGS_WITH_ATTRIBUTES.contains(&name) {
                        return Err(format!("<{name}> cannot carry attributes at byte {i}"));
                    }
                    if !attrs.matches('"').count().is_multiple_of(2) {
                        return Err(format!("unbalanced quote in <{name}> at byte {i}"));
                    }
                    stack.push(name.to_string());
                }
                i += end + 1;
            }
            _ => i += c.len_utf8(),
        }
    }
    Ok(stack)
}

/// Is `text` something Telegram's HTML parser accepts (and short enough to send)?
pub fn validate(text: &str) -> Result<(), String> {
    let open = scan(text)?;
    if let Some(tag) = open.last() {
        return Err(format!("<{tag}> is never closed"));
    }
    let n = plain_len(text);
    if n > MAX_MESSAGE_CHARS {
        return Err(format!("{n} characters, Telegram allows {MAX_MESSAGE_CHARS}"));
    }
    Ok(())
}

/// Is `inner` (the text between `<` and `>`) a tag Telegram renders?
fn is_markup(inner: &str) -> bool {
    let name = inner.strip_prefix('/').unwrap_or(inner);
    let name = name.split(|ch: char| ch.is_whitespace()).next().unwrap_or_default();
    !inner.contains('<') && ALLOWED_TAGS.contains(&name)
}

/// What the reader sees: Telegram's tags removed, entities decoded, anything else (a stray `<`,
/// `<script>` from a token name) kept verbatim. Used as the fallback when the HTML is invalid or
/// refused, and to measure length.
pub fn to_plain(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        let rest = &text[i..];
        let c = rest.chars().next().unwrap_or_default();
        if c == '<'
            && let Some(end) = rest.find('>')
            && is_markup(&rest[1..end])
        {
            i += end + 1;
            continue;
        }
        if c == '&'
            && let Some(n) = entity_len(rest)
        {
            let ent = &rest[..n];
            let decoded = match ent {
                "&lt;" => Some('<'),
                "&gt;" => Some('>'),
                "&amp;" => Some('&'),
                "&quot;" => Some('"'),
                _ => {
                    let body = &ent[2..ent.len() - 1];
                    let code = match body.strip_prefix(['x', 'X']) {
                        Some(h) => u32::from_str_radix(h, 16).ok(),
                        None => body.parse().ok(),
                    };
                    code.and_then(char::from_u32)
                }
            };
            if let Some(d) = decoded {
                out.push(d);
                i += n;
                continue;
            }
        }
        out.push(c);
        i += c.len_utf8();
    }
    out
}

/// Telegram counts the rendered text in UTF-16 code units.
pub fn plain_len(text: &str) -> usize {
    to_plain(text).encode_utf16().count()
}

/// Plain text cut to at most `max_chars` UTF-16 code units (Telegram's unit), marked when cut.
pub fn truncate_plain(text: &str, max_chars: usize) -> String {
    if text.encode_utf16().count() <= max_chars {
        return text.to_string();
    }
    const MARK: &str = "\n… (dipotong)";
    let budget = max_chars.saturating_sub(MARK.encode_utf16().count());
    let mut used = 0;
    let mut out = String::new();
    for c in text.chars() {
        used += c.len_utf16();
        if used > budget {
            break;
        }
        out.push(c);
    }
    out + MARK
}

/// `text` cut down (at a line boundary, tags closed) so that it renders within `max_chars`.
/// Valid input stays valid; text that already fits is returned unchanged.
pub fn fit(text: &str, max_chars: usize) -> String {
    if plain_len(text) <= max_chars {
        return text.to_string();
    }
    const MARK: &str = "\n<i>… (dipotong)</i>";
    let budget = max_chars.saturating_sub(plain_len(MARK) + 1);
    let mut kept = String::new();
    for line in text.split_inclusive('\n') {
        let candidate = format!("{kept}{line}");
        if plain_len(&candidate) > budget {
            break;
        }
        kept = candidate;
    }
    // Lines of our own screens are balanced; if a cut still left a tag open, close it.
    let closing: String = scan(&kept).unwrap_or_default().iter().rev().map(|t| format!("</{t}>")).collect();
    format!("{}{}{}", kept.trim_end_matches('\n'), closing, MARK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_what_telegram_accepts() {
        for ok in [
            "plain",
            "<b>bold</b> <i>it</i> <code>0xabc</code>",
            "a &amp; b &lt;c&gt; &quot;d&quot; &#128512; &#x1F600;",
            "<a href=\"https://x.y/?a=1&amp;b=2\">link</a>",
            "<b>outer <i>inner</i></b>",
            "<span class=\"tg-spoiler\">s</span>",
        ] {
            assert_eq!(validate(ok), Ok(()), "{ok}");
        }
    }

    #[test]
    fn rejects_what_telegram_rejects() {
        for bad in [
            "TREASURY & WALLETS",
            "Market Cap > $8k",
            "x < 0.1 ETH",
            "<b>unclosed",
            "<b>wrong</i>",
            "</b>",
            "<script>alert(1)</script>",
            "<b class=\"x\">attr</b>",
            "&nbsp;",
            "&#;",
            "<b",
        ] {
            assert!(validate(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn escape_round_trips_through_plain() {
        let hostile = "<script>&\"rug\" > 0.1 & co</script>";
        let html = format!("<b>{}</b>", escape(hostile));
        assert_eq!(validate(&html), Ok(()));
        assert_eq!(to_plain(&html), hostile);
    }

    #[test]
    fn plain_truncation_counts_utf16_units() {
        let emoji = "🚨".repeat(3_000); // 6000 UTF-16 units
        let cut = truncate_plain(&emoji, MAX_MESSAGE_CHARS);
        assert!(cut.encode_utf16().count() <= MAX_MESSAGE_CHARS);
        assert!(cut.ends_with("(dipotong)"));
        assert_eq!(truncate_plain("short", 10), "short");
    }

    #[test]
    fn plain_fallback_drops_only_telegram_markup() {
        assert_eq!(to_plain("<b>Rug</b> & <script>x</script> &lt;3 a<b"), "Rug & <script>x</script> <3 a<b");
        assert_eq!(to_plain("<a href=\"https://x\">link</a> <tg-spoiler>s</tg-spoiler>"), "link s");
    }

    #[test]
    fn fit_keeps_valid_html_within_the_limit() {
        let line = format!("<b>{}</b> <code>0x{}</code>\n", escape("a & b"), "ab".repeat(20));
        let long = line.repeat(400);
        assert!(plain_len(&long) > MAX_MESSAGE_CHARS);
        let cut = fit(&long, MAX_MESSAGE_CHARS);
        assert_eq!(validate(&cut), Ok(()));
        assert!(plain_len(&cut) <= MAX_MESSAGE_CHARS);
        assert!(cut.ends_with("(dipotong)</i>"));
        assert_eq!(fit("short", 100), "short");
    }
}
