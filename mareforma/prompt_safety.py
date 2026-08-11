"""
prompt_safety.py: sanitize-and-wrap helpers for feeding claim text to an LLM.

When an agent retrieves claims via :meth:`mareforma.EpistemicGraph.query`
and feeds the results back into an LLM prompt, the claim text was written
by an *earlier* agent (or human) and could contain prompt-injection
payloads or display-spoofing tricks: zero-width characters that smuggle
hidden instructions, RTL/LTR overrides that visually reorder text, or
a forged ``</untrusted_data>`` closing tag that breaks out of the
wrapper.

This module provides three minimal operations:

- :func:`sanitize_for_llm` strips zero-width / bidi / C0-C1 control
  characters (whitespace except ``\\n`` and ``\\t`` is kept) and caps
  pathologically long inputs.
- :func:`strip_forged_tags` replaces any forged opening/closing tag
  with ``[stripped]``, leaving the content otherwise intact.
- :func:`wrap_untrusted` strips forged tags and wraps the result in
  ``<untrusted_data>...</untrusted_data>`` delimiters.

Callers should be opinionated about what they wrap. The graph's
``query_for_llm`` method wraps the ``text`` and ``comparison_summary``
fields, sanitizes-only on the short metadata labels, and sanitizes plus
strips forged tags on every other string in the row.

Threat model
------------
The wrapper is one half of a contract. The other half, telling the
LLM that everything inside ``<untrusted_data>`` is data, not
instructions, lives in the caller's system prompt. Anthropic's prompt
guidance documents the pattern; we provide the wrapping primitive,
not the system prompt.
"""

from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from typing import Final

# Hard ceiling on a single text field. A 1 MB claim is almost certainly
# either an attack (token-flood DoS against the consuming LLM) or a
# data-shape error. Truncate with a visible marker so the LLM sees the
# elision rather than silently consuming whatever fits.
_MAX_FIELD_LEN: Final = 100_000
_TRUNCATION_MARKER: Final = "\n…[mareforma: truncated, original exceeded 100k chars]"

# Singleton zero-width / bidi-override / tag-lookalike codepoints we
# refuse. Subset of ``validators._FORBIDDEN_DISPLAY_CHARS`` plus every
# non-ASCII character that NFKC-folds to ``<``, ``>`` or ``/`` — a
# hostile claim using ``＜/untrusted_data＞`` could survive both
# sanitize and wrap if a downstream NFKC normaliser (logging, RAG
# vectorizer, the LLM's own tokenizer) folds the glyphs to ASCII at
# read time. The lookalike entries below are the full derivation over
# the codepoint space, {0xFE64, 0xFE65, 0xFF0F, 0xFF1C, 0xFF1E}, which
# tests/test_prompt_safety.py re-derives so the set cannot drift.
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
    0xFE64,  # SMALL LESS-THAN SIGN (NFKC → '<')
    0xFE65,  # SMALL GREATER-THAN SIGN (NFKC → '>')
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


def _build_forbidden_re() -> re.Pattern[str]:
    """Compile the tables above, plus the control ranges, into one
    character class.

    ``sanitize_for_llm`` runs on every string of every row a query hands
    to an LLM, including the payload and signature columns, so the strip
    has to cost a scan of the bytes rather than a Python loop over each
    one of them. The class is derived from the tables so the two cannot
    drift; tests/test_prompt_safety.py checks it against a
    codepoint-by-codepoint walk over the whole codepoint space.
    """
    parts = [
        r"\x00-\x08",  # C0 controls, keeping \t (0x09) and \n (0x0A)
        r"\x0b-\x1f",
        r"\x7f-\x9f",  # DEL and the C1 controls
    ]
    parts += [f"\\U{lo:08X}-\\U{hi:08X}" for lo, hi in _FORBIDDEN_RANGES]
    parts += [f"\\U{cp:08X}" for cp in sorted(_FORBIDDEN_CODEPOINTS)]
    return re.compile("[" + "".join(parts) + "]")


_FORBIDDEN_RE: Final = _build_forbidden_re()


@lru_cache(maxsize=8)
def _forged_tag_re(tag: str) -> re.Pattern[str]:
    """Compile a case-insensitive regex that matches opening or closing
    ``<{tag}>`` (with optional whitespace and trailing attributes).

    Memoised. This is called once per string field of every row on the
    LLM-bound read paths, and the tag is a static identifier at every call site
    we control, so the same pattern was being rebuilt thousands of times per
    page. Measured over a 200-row page: 6.14 ms to 2.63 ms for the whole row
    formatting. The cache is small on purpose: :func:`_validate_tag` bounds the
    tag to a simple identifier, and the callers use one.
    """
    return re.compile(
        rf"<\s*/?\s*{re.escape(tag)}\b[^>]*>",
        flags=re.IGNORECASE,
    )


def strip_forged_tags(text: str | None, *, tag: str = "untrusted_data") -> str | None:
    """Replace every literal ``<{tag}>`` / ``</{tag}>`` in *text* with
    ``[stripped]``.

    The tag-forgery half of :func:`wrap_untrusted`, without the wrapping.
    Use it on content that must be splice-safe but cannot carry delimiters
    of its own, such as a JSON column whose shape the caller parses.

    Returns ``None`` for ``None`` input.
    """
    if text is None:
        return None
    if not isinstance(text, str):
        raise TypeError(
            f"strip_forged_tags expects str or None, got {type(text).__name__}"
        )
    _validate_tag(tag)
    pattern = _forged_tag_re(tag)
    stripped = pattern.sub("[stripped]", text)
    # A model reads the delimiter as a HUMAN does, and a fullwidth 'd' looks
    # exactly like an ASCII one: `</untrusteｄ_data>` matched no pattern here and
    # closed the wrapper on the way in. The codepoint stripper does not catch it
    # either, because the character folds to a letter, not to a delimiter.
    #
    # So the same text is checked again under NFKC. Matching on the folded form
    # and substituting on it is safe for this field because the result is a
    # display string a model reads, never bytes anything verifies: the signature
    # surfaces read the raw row. Only used when folding actually revealed a
    # delimiter, so ordinary text is returned exactly as written.
    folded = unicodedata.normalize("NFKC", stripped)
    if folded != stripped and pattern.search(folded):
        return pattern.sub("[stripped]", folded)
    return stripped


def _validate_tag(tag: str) -> None:
    """Reject a tag that would make the wrapper itself injectable.

    The tag is a static identifier in callers we control, but bound the
    contract: a tag with whitespace or ``>`` would let an attacker close
    the delimiter from inside.
    """
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", tag):
        raise ValueError(
            f"tag {tag!r} must be a simple ASCII identifier (letters, "
            "digits, underscore; not starting with a digit)."
        )


def sanitize_for_llm(text: str | None) -> str | None:
    """Strip prompt-injection-hostile codepoints and cap length.

    Stripped codepoint classes:

    - Zero-width characters (ZWSP, ZWJ, ZWNJ, BOM)
    - Bidirectional overrides (LRO, RLO, LRE, RLE, isolates, marks)
    - C0 (``< 0x20``) and C1 (``0x7F-0x9F``) control characters,
      except ``\\n`` and ``\\t`` which are kept (legitimate claim
      text contains them)
    - Fullwidth and small-form ``<``, ``>``, ``/``: would NFKC-fold to
      ASCII and reconstruct a forged delimiter post-wrap
    - Variation selectors (U+FE00-FE0F, U+E0100-E01EF, U+180B-180D)
    - Interlinear annotation anchors (U+FFF9-FFFB)
    - **Tag plane (U+E0000-E007F)**: Goodside's "ASCII smuggler"
      prompt-injection vector. Invisible to a human reader, present
      in the LLM token stream.

    Returns ``None`` for ``None`` input: callers can pass an optional
    field through without conditionals.

    Idempotent: ``sanitize_for_llm(sanitize_for_llm(x)) == sanitize_for_llm(x)``.
    """
    if text is None:
        return None
    if not isinstance(text, str):
        raise TypeError(
            f"sanitize_for_llm expects str or None, got {type(text).__name__}"
        )

    sanitized = _FORBIDDEN_RE.sub("", text)
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
    _validate_tag(tag)
    return f"<{tag}>\n{strip_forged_tags(text, tag=tag)}\n</{tag}>"


def safe_for_llm(text: str | None, *, tag: str = "untrusted_data") -> str:
    """Sanitize *then* wrap *text*: the recommended one-call entry point.

    Composes :func:`sanitize_for_llm` (strips zero-width / bidi /
    steganographic codepoints) with :func:`wrap_untrusted` (strips
    forged delimiters, wraps in ``<{tag}>...</{tag}>``). Use this
    whenever you have a string from outside the trust boundary that
    needs to land in an LLM context window.

    ``None`` is treated as an empty string so the result is always a
    fully-formed wrapped block, useful when splicing into a prompt
    template that expects the tag to be present even for missing data.
    """
    return wrap_untrusted(sanitize_for_llm(text), tag=tag)
