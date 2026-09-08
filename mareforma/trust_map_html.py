"""Self-contained HTML rendering of a :class:`mareforma.trust_map.TrustMap`.

One file, no external requests: all CSS is inline, there are no scripts, no
web fonts, no remote images. A reviewer can open the file offline, mail it, or
drop it into a paper figure, and it renders identically. The output is
deterministic, no timestamps, no randomness, so a golden-file test pins it
byte for byte.
"""
from __future__ import annotations

from html import escape

from .trust_map import TrustMap, Tier, display_value, is_tamper_value

# Tier → a stable accent the badge uses. Kept as plain hex so the page needs no
# external stylesheet and renders the same everywhere.
# A tamper value overrides the tier accent. The tier says where an answer came
# from and the accent renders that, so a tamper report computed from evidence
# was painted in the same green as a healthy computed axis: the word said
# TAMPERED and the colour said fine. Whether an answer is alarming is a
# different question from where it came from, and the page has to answer both.
_TAMPER_ACCENT = "#b3261e"

_TIER_ACCENT = {
    Tier.COMPUTED.value: "#1a7f5a",
    Tier.PROXIED.value: "#b8860b",
    Tier.DEFERRED.value: "#6b6b6b",
}

_STYLE = """
* { box-sizing: border-box; }
body { margin: 0; padding: 2rem; background: #f6f7f9; color: #16181d;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  line-height: 1.5; }
.card { max-width: 60rem; margin: 0 auto; background: #fff; border: 1px solid #e2e5ea;
  border-radius: 10px; overflow: hidden; }
.head { padding: 1.5rem 1.75rem; border-bottom: 1px solid #eceef1; }
.head h1 { margin: 0 0 .35rem; font-size: 1.15rem; }
.head .sub { color: #5b6472; font-size: .85rem; font-family: ui-monospace, "SF Mono", Menlo, monospace; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: .8rem 1.75rem; border-bottom: 1px solid #f0f2f5; vertical-align: top; }
th { font-size: .72rem; letter-spacing: .04em; text-transform: uppercase; color: #7a828f;
  background: #fafbfc; }
tr:last-child td { border-bottom: none; }
.prop { font-weight: 600; white-space: nowrap; }
.badge { display: inline-block; padding: .1rem .5rem; border-radius: 999px; font-size: .7rem;
  font-weight: 700; letter-spacing: .03em; color: #fff; }
.value { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: .85rem; }
.residual { color: #5b6472; font-size: .85rem; }
"""


def render_html(trust_map: TrustMap) -> str:
    """Render *trust_map* as one self-contained HTML document (deterministic)."""
    rows = []
    for p in trust_map.properties:
        tampered = is_tamper_value(p.value)
        accent = (_TAMPER_ACCENT if tampered
                  else _TIER_ACCENT.get(p.tier.value, "#6b6b6b"))
        value = display_value(p.value)
        # Built here rather than inside the f-string below: a backslash
        # in an f-string expression is a syntax error before 3.12, and
        # this package supports 3.10.
        tamper_style = (
            f' style="color:{_TAMPER_ACCENT};font-weight:600"'
            if tampered else ""
        )
        rows.append(
            "      <tr>\n"
            f"        <td class=\"prop\">{escape(p.name)}</td>\n"
            f"        <td><span class=\"badge\" style=\"background:{accent}\">"
            f"{escape(p.tier.value)}</span></td>\n"
            f"        <td class=\"value\"{tamper_style}>"
            f"{escape(value)}</td>\n"
            f"        <td class=\"residual\">{escape(p.residual)}</td>\n"
            "      </tr>"
        )
    rows_html = "\n".join(rows)
    subject = escape(trust_map.subject_id)
    kind = escape(trust_map.subject_kind)
    version = escape(trust_map.version)
    return (
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\">\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        f"  <title>trust map, {kind} {subject}</title>\n"
        f"  <style>{_STYLE}</style>\n"
        "</head>\n"
        "<body>\n"
        "  <div class=\"card\">\n"
        "    <div class=\"head\">\n"
        "      <h1>Trust map</h1>\n"
        f"      <div class=\"sub\">{kind} {subject} · map {version}</div>\n"
        "    </div>\n"
        "    <table>\n"
        "      <thead>\n"
        "        <tr><th>Property</th><th>Tier</th><th>Value</th><th>Residual</th></tr>\n"
        "      </thead>\n"
        "      <tbody>\n"
        f"{rows_html}\n"
        "      </tbody>\n"
        "    </table>\n"
        "  </div>\n"
        "</body>\n"
        "</html>\n"
    )
