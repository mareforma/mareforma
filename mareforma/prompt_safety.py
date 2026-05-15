"""
prompt_safety.py — sanitize-and-wrap helpers for feeding claim text to an LLM.

When an agent retrieves claims via :meth:`mareforma.EpistemicGraph.query`
and feeds the results back into an LLM prompt, the claim text was written
by an *earlier* agent (or human) and could contain prompt-injection
payloads or display-spoofing tricks: zero-width characters that smuggle
hidden instructions, RTL/LTR overrides that visually reorder text, or
a forged ``</untrusted_data>`` closing tag that breaks out of the
wrapper.

This module provides two minimal operations:

- :func:`sanitize_for_llm` strips zero-width / bidi / C0-C1 control
  characters (whitespace except ``\\n`` and ``\\t`` is kept) and caps
  pathologically long inputs.
- :func:`wrap_untrusted` strips any forged opening/closing tag from the
  inner content and wraps the result in
  ``<untrusted_data>...</untrusted_data>`` delimiters.

Callers should be opinionated about what they wrap. The graph's
``query_for_llm`` method wraps the ``text`` and ``comparison_summary``
fields and sanitizes-only on the short metadata labels.

Threat model
------------
The wrapper is one half of a contract. The other half — telling the
LLM that everything inside ``<untrusted_data>`` is data, not
instructions — lives in the caller's system prompt. Anthropic's prompt
guidance documents the pattern; we provide the wrapping primitive,
not the system prompt.
"""

from __future__ import annotations

import re
from typing import Final

# Hard ceiling on a single text field. A 1 MB claim is almost certainly
# either an attack (token-flood DoS against the consuming LLM) or a
# data-shape error. Truncate with a visible marker so the LLM sees the
# elision rather than silently consuming whatever fits.
_MAX_FIELD_LEN: Final = 100_000
_TRUNCATION_MARKER: Final = "\n…[mareforma: truncated, original exceeded 100k chars]"

# Singleton zero-width / bidi-override / tag-lookalike codepoints we
# refuse. Subset of ``validators._FORBIDDEN_DISPLAY_CHARS`` plus the
# fullwidth ``<`` / ``>`` / ``/`` lookalikes — a hostile claim using
# ``＜/untrusted_data＞`` could survive both sanitize and wrap if a
# downstream NFKC normaliser (logging, RAG vectorizer, the LLM's own
# tokenizer) folds the fullwidth glyphs to ASCII at read time.
_FORBIDDEN_CODEPOINTS: Final = frozenset({
    0x200B,  # ZERO WIDTH SPACE
    0x200C,  # ZERO WIDTH NON-JOINER
    0x200D,  # ZERO WIDTH JOINER
    0x200E,  # LEFT-TO-RIGHT MARK
    0x200F,  # RIGHT-TO-LEFT MARK
    0x202A,  # LEFT-TO-RIGHT EMBEDDING
    0x202B,  # RIGHT-TO-LEFT EMBEDDING
    0x202C,  # POP DIRECTIONAL FORMATTING
    0x202D,  # LEFT-TO-RIGHT OVERRIDE
    0x202E,  # RIGHT-TO-LEFT OVERRIDE
    0x2066,  # LEFT-TO-RIGHT ISOLATE
    0x2067,  # RIGHT-TO-LEFT ISOLATE
    0x2068,  # FIRST STRONG ISOLATE
    0x2069,  # POP DIRECTIONAL ISOLATE
    0xFEFF,  # ZERO WIDTH NO-BREAK SPACE / BOM
    0xFF1C,  # FULLWIDTH LESS-THAN SIGN (NFKC → '<')
    0xFF1E,  # FULLWIDTH GREATER-THAN SIGN (NFKC → '>')
    0xFF0F,  # FULLWIDTH SOLIDUS (NFKC → '/')
})


# Codepoint ranges of invisible / steganographic characters. These are
# known prompt-injection vectors — most famously the U+E0000–U+E007F
# "language tag" plane that Goodside-style "ASCII smuggler" attacks
# use to hide instructions inside a payload that looks like plain
# ASCII. Variation selectors and interlinear annotation are similar:
# invisible to a human reader, present in the token stream.
_FORBIDDEN_RANGES: Final = (
    (0x0180B, 0x0180D),  # Mongolian variation selectors
    (0x0FE00, 0x0FE0F),  # Variation selectors (base plane)
    (0x0FFF9, 0x0FFFB),  # Interlinear annotation anchors
    (0xE0000, 0xE007F),  # Tags block (language tag plane)
    (0xE0100, 0xE01EF),  # Variation selectors supplement
)


def _is_forbidden_codepoint(cp: int) -> bool:
    """True if *cp* is a zero-width / bidi / tag-lookalike / steganographic
    codepoint we strip from LLM-bound text."""
    if cp in _FORBIDDEN_CODEPOINTS:
        return True
    for lo, hi in _FORBIDDEN_RANGES:
        if lo <= cp <= hi:
            return True
    return False

def _forged_tag_re(tag: str) -> re.Pattern[str]:
    """Compile a case-insensitive regex that matches opening or closing
    ``<{tag}>`` (with optional whitespace and trailing attributes)."""
    return re.compile(
        rf"<\s*/?\s*{re.escape(tag)}\b[^>]*>",
        flags=re.IGNORECASE,
    )


def sanitize_for_llm(text: str | None) -> str | None:
    """Strip prompt-injection-hostile codepoints and cap length.

    Stripped codepoint classes:

    - Zero-width characters (ZWSP, ZWJ, ZWNJ, BOM)
    - Bidirectional overrides (LRO, RLO, LRE, RLE, isolates, marks)
    - C0 (``< 0x20``) and C1 (``0x7F-0x9F``) control characters,
      except ``\\n`` and ``\\t`` which are kept (legitimate claim
      text contains them)
    - Fullwidth ``<``, ``>``, ``/`` — would NFKC-fold to ASCII and
      reconstruct a forged delimiter post-wrap
    - Variation selectors (U+FE00-FE0F, U+E0100-E01EF, U+180B-180D)
    - Interlinear annotation anchors (U+FFF9-FFFB)
    - **Tag plane (U+E0000-E007F)** — Goodside's "ASCII smuggler"
      prompt-injection vector. Invisible to a human reader, present
      in the LLM token stream.

    Returns ``None`` for ``None`` input — callers can pass an optional
    field through without conditionals.

    Idempotent: ``sanitize_for_llm(sanitize_for_llm(x)) == sanitize_for_llm(x)``.
    """
    if text is None:
        return None
    if not isinstance(text, str):
        raise TypeError(
            f"sanitize_for_llm expects str or None, got {type(text).__name__}"
        )

    out: list[str] = []
    for ch in text:
        cp = ord(ch)
        if _is_forbidden_codepoint(cp):
            continue
        # C0 controls (0x00-0x1F) and DEL (0x7F) and C1 controls
        # (0x80-0x9F). Keep \n (0x0A) and \t (0x09) — legitimate
        # in claim text.
        if cp < 0x20:
            if cp in (0x09, 0x0A):
                out.append(ch)
            # else: drop
            continue
        if 0x7F <= cp <= 0x9F:
            continue
        out.append(ch)

    sanitized = "".join(out)
    if len(sanitized) > _MAX_FIELD_LEN:
        sanitized = sanitized[:_MAX_FIELD_LEN] + _TRUNCATION_MARKER
    return sanitized


def wrap_untrusted(text: str | None, *, tag: str = "untrusted_data") -> str:
    """Wrap *text* in ``<{tag}>...</{tag}>`` delimiters for an LLM prompt.

    Any literal occurrence of the opening or closing tag in *text* is
    replaced with ``[stripped]`` before wrapping so a hostile claim
    cannot break out of the wrapper. Matching is case-insensitive and
    tolerant of whitespace inside the tag.

    .. warning::

        This is the tag-forgery layer ONLY. Call :func:`sanitize_for_llm`
        on the input first, or use :func:`safe_for_llm` which composes
        both. A hostile claim like ``</untrusted​_data>`` (zero-width
        space hidden inside the tag) bypasses this regex but is
        sanitized away by the codepoint stripper. Without sanitize-first,
        an attacker can break out of the wrapper.

    ``None`` is treated as an empty string so callers can wrap optional
    fields uniformly.
    """
    if text is None:
        text = ""
    if not isinstance(text, str):
        raise TypeError(
            f"wrap_untrusted expects str or None, got {type(text).__name__}"
        )
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", tag):
        # The tag is a static identifier in callers we control, but
        # bound the contract: a tag with whitespace or `>` would let
        # the wrapper itself become injectable.
        raise ValueError(
            f"tag {tag!r} must be a simple ASCII identifier (letters, "
            "digits, underscore; not starting with a digit)."
        )

    stripped = _forged_tag_re(tag).sub("[stripped]", text)
    return f"<{tag}>\n{stripped}\n</{tag}>"


def safe_for_llm(text: str | None, *, tag: str = "untrusted_data") -> str:
    """Sanitize *then* wrap *text* — the recommended one-call entry point.

    Composes :func:`sanitize_for_llm` (strips zero-width / bidi /
    steganographic codepoints) with :func:`wrap_untrusted` (strips
    forged delimiters, wraps in ``<{tag}>...</{tag}>``). Use this
    whenever you have a string from outside the trust boundary that
    needs to land in an LLM context window.

    ``None`` is treated as an empty string so the result is always a
    fully-formed wrapped block — useful when splicing into a prompt
    template that expects the tag to be present even for missing data.
    """
    return wrap_untrusted(sanitize_for_llm(text), tag=tag)
