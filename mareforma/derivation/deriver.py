"""
deriver: Classification derivation engine with evidence binding.

Combines log-template extraction and source-profile analysis to
deterministically derive a classification (ANALYTICAL or INFERRED)
from an agent's source code and runtime logs.

ANALYTICAL requires positive evidence from BOTH artifacts:
  - Source profile contains data-access patterns (static)
  - Log templates contain evidence of data-access execution (dynamic)

INFERRED is the conservative default for everything else.

Phase 4 additions:
  - Evidence data structure with matched/unmatched patterns and confidence
  - predicate_payload binding for v0.3.1 item 300
  - Verification with evidence payload tamper detection
  - Cross-artifact correlation with per-kind matching

Phase 6 additions:
  - Word-boundary matching for log-signal detection (avoids false positives
    like "put" in "computing" or "connect" in "connection failed")
  - Negative-evidence filtering (failed/error/unreachable/timeout/refused/
    denied/unavailable lines excluded from positive evidence)
  - LLM-context disambiguation (LLM/OpenAI/GPT/synthesis lines excluded
    from data-access signal matching)
  - Raw-log fallback scan when Drain templates are over-generalized
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from mareforma.derivation.log_templates import extract_templates
from mareforma.derivation.source_profile import extract_source_profile

# Derivation version — bumped when evidence format changes.
DERIVATION_VERSION = "0.1.0"


@dataclass
class ClassificationResult:
    """Result of core-derived classification with evidence binding.

    The evidence chain captures the full derivation path:
    source-profile digest + log-template digest + which source-level
    patterns were corroborated by log evidence + confidence scoring.
    """

    classification: str  # "ANALYTICAL" or "INFERRED"
    source_profile_digest: str
    log_template_digest: str
    matched_patterns: list[str] = field(default_factory=list)
    unmatched_patterns: list[str] = field(default_factory=list)
    reasoning: str = ""
    _source_data_access_kinds: int = 0  # total data-access kind count

    def to_evidence(self) -> dict:
        """Build the structured evidence payload.

        Contains all information a verifier needs to understand
        why the classification was derived:
        - source_profile_digest: SHA-256 of the source profile
        - log_template_digest: SHA-256 of the log templates
        - matched_patterns: data-access kinds corroborated by log evidence
        - unmatched_patterns: data-access kinds without log evidence
        - classification: the derived classification
        - reasoning: human-readable explanation
        - confidence: corroborated/total/ratio scoring
        """
        total = self._source_data_access_kinds
        corroborated = len(self.matched_patterns)
        ratio = corroborated / total if total > 0 else 0.0

        return {
            "source_profile_digest": self.source_profile_digest,
            "log_template_digest": self.log_template_digest,
            "matched_patterns": list(self.matched_patterns),
            "unmatched_patterns": list(self.unmatched_patterns),
            "classification": self.classification,
            "reasoning": self.reasoning,
            "confidence": {
                "corroborated": corroborated,
                "total": total,
                "ratio": round(ratio, 4),
            },
        }

    def to_predicate_payload(self) -> dict:
        """Serialize the derivation evidence into predicate_payload format.

        Matches v0.3.1 item 300's design: the predicate_payload field
        carries structured evidence that the signed envelope covers.

        Returns a dict with:
        - derivation_version: version of the derivation format
        - derived_classification: the classification this derivation produced
        - evidence: the full evidence payload (same as to_evidence())
        """
        return {
            "derivation_version": DERIVATION_VERSION,
            "derived_classification": self.classification,
            "evidence": self.to_evidence(),
        }


@dataclass
class VerificationResult:
    """Result of classification verification.

    When an evidence_payload is provided, the verifier also checks
    that the evidence matches the re-derived result (tamper detection).
    """

    agrees: bool
    derived_classification: str
    claimed_classification: str
    reasoning: str = ""
    evidence_agrees: bool | None = None  # None = not checked (no payload)


# Log-template keywords that indicate actual data-access execution.
# These are matched against log templates to confirm that the
# data-access patterns found in source actually ran.
#
# Phase 6 refinement: signals use word-boundary matching (regex \b)
# to avoid false positives like "put" matching "computing" or
# "connect" matching "connection failed". Negative-evidence phrases
# filter out log lines that describe failures, not successes.
_LOG_DATA_ACCESS_SIGNALS: dict[str, list[re.Pattern[str]]] = {
    "database": [
        re.compile(r"\bselect\b", re.IGNORECASE),
        re.compile(r"\binsert\b", re.IGNORECASE),
        re.compile(r"\bupdate\b", re.IGNORECASE),
        re.compile(r"\bdelete\b", re.IGNORECASE),
        re.compile(r"\bquery\b", re.IGNORECASE),
        re.compile(r"\bconnect(?:ed|ing)?\b", re.IGNORECASE),
        re.compile(r"\bdatabase\b", re.IGNORECASE),
        re.compile(r"\brows\b", re.IGNORECASE),
        re.compile(r"\bcursor\b", re.IGNORECASE),
        re.compile(r"\btable\b", re.IGNORECASE),
    ],
    "http": [
        re.compile(r"\bhttp\b", re.IGNORECASE),
        re.compile(r"\bHTTP\s+(?:GET|POST|PUT|PATCH|DELETE)\b"),
        re.compile(r"\bendpoint\b", re.IGNORECASE),
        re.compile(r"\burl\b", re.IGNORECASE),
    ],
    "file_io": [
        re.compile(r"\bfile\b", re.IGNORECASE),
        re.compile(r"\bread\b", re.IGNORECASE),
        re.compile(r"\bwrite\b", re.IGNORECASE),
        re.compile(r"\bopen\b", re.IGNORECASE),
        re.compile(r"\bpath\b", re.IGNORECASE),
        re.compile(r"\bdirectory\b", re.IGNORECASE),
        re.compile(r"\bcsv\b", re.IGNORECASE),
    ],
}

# Negative-evidence phrases: if a log template contains one of these,
# it indicates the data-access FAILED or was SKIPPED, not that it
# succeeded. Such templates are excluded from positive evidence.
_NEGATIVE_EVIDENCE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bfailed\b", re.IGNORECASE),
    re.compile(r"\berror\b", re.IGNORECASE),
    re.compile(r"\bunreachable\b", re.IGNORECASE),
    re.compile(r"\btimeout\b", re.IGNORECASE),
    re.compile(r"\brefused\b", re.IGNORECASE),
    re.compile(r"\bdenied\b", re.IGNORECASE),
    re.compile(r"\bunavailable\b", re.IGNORECASE),
]

# Disambiguation: these phrases in a template indicate LLM activity,
# not data-access. If a template matches BOTH a data-access signal
# AND an LLM-context phrase, the match is discarded to avoid
# conflating LLM logging with data-access evidence.
_LLM_CONTEXT_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bllm\b", re.IGNORECASE),
    re.compile(r"\bopenai\b", re.IGNORECASE),
    re.compile(r"\banthropic\b", re.IGNORECASE),
    re.compile(r"\bgpt-\d", re.IGNORECASE),
    re.compile(r"\bclaude\b", re.IGNORECASE),
    re.compile(r"\bchat\.completions\b", re.IGNORECASE),
    re.compile(r"\bsynthesis\b", re.IGNORECASE),
]


def _is_negative_evidence(pattern_text: str) -> bool:
    """Check if a template pattern indicates failed/skipped data access."""
    return any(p.search(pattern_text) for p in _NEGATIVE_EVIDENCE_PATTERNS)


def _is_llm_context(pattern_text: str) -> bool:
    """Check if a template pattern is about LLM activity, not data access."""
    return any(p.search(pattern_text) for p in _LLM_CONTEXT_PATTERNS)


def _check_log_evidence(
    log_templates: list,  # noqa: ANN001 — LogTemplate from log_templates module
    source_pattern_kinds: set[str],
    *,
    raw_logs: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    """Check which source-profile pattern kinds have supporting log evidence.

    Uses word-boundary matching for signals and filters out negative-evidence
    (failed/error) and LLM-context templates to avoid false positives.

    Phase 6 addition: when template-based matching misses a kind (because
    Drain merged specific log lines into overly generic templates like
    ``<*> INFO Step <*> : <*>``), a raw-log fallback scan supplements the
    template check. The fallback uses the same signal patterns, negative-
    evidence filters, and LLM-context filters. This maintains determinism
    (same logs -> same result) while handling Drain over-generalization.

    Returns a tuple of (matched_kinds, unmatched_kinds).
    """
    matched: list[str] = []
    unmatched: list[str] = []

    for kind in sorted(source_pattern_kinds):
        signals = _LOG_DATA_ACCESS_SIGNALS.get(kind, [])
        if not signals:
            # Unknown kind — no signals to match, so unmatched
            unmatched.append(kind)
            continue

        found = False
        # Primary: check templates
        for template in log_templates:
            pattern_text = template.pattern

            # Skip templates that indicate failures (negative evidence)
            if _is_negative_evidence(pattern_text):
                continue

            # Skip templates that are about LLM activity, not data access
            if _is_llm_context(pattern_text):
                continue

            if any(signal.search(pattern_text) for signal in signals):
                found = True
                break

        # Fallback: when templates are over-generalized (Drain merged
        # specific lines), scan raw log lines for data-access evidence.
        # This is deterministic and uses the same filters.
        if not found and raw_logs:
            for line in raw_logs:
                if _is_negative_evidence(line):
                    continue
                if _is_llm_context(line):
                    continue
                if any(signal.search(line) for signal in signals):
                    found = True
                    break

        if found:
            matched.append(kind)
        else:
            unmatched.append(kind)

    return matched, unmatched


def derive_classification(
    source: str,
    logs: list[str],
) -> ClassificationResult:
    """Derive classification from source code and runtime logs.

    This is a pure function: given the same (source, logs) inputs,
    it always produces the same classification on any machine.

    Args:
        source: Python source code of the agent.
        logs: Raw log lines (stdout/stderr) from the agent's run.

    Returns:
        ClassificationResult with the derived classification, evidence,
        and confidence scoring.
    """
    # Step 1: Extract source profile (static analysis)
    profile = extract_source_profile(source)

    # Step 2: Extract log templates (dynamic analysis)
    template_result = extract_templates(logs)

    # Collect the distinct data-access kinds from source
    source_data_access_kinds = {
        p.kind for p in profile.patterns
        if p.kind in ("database", "http", "file_io")
    }
    total_kinds = len(source_data_access_kinds)

    # Rule 1: If source has no data-access patterns -> INFERRED
    if not profile.has_data_access:
        return ClassificationResult(
            classification="INFERRED",
            source_profile_digest=profile.digest,
            log_template_digest=template_result.digest,
            matched_patterns=[],
            unmatched_patterns=[],
            reasoning="Source code contains no data-access patterns.",
            _source_data_access_kinds=0,
        )

    # Rule 2: If logs are empty -> INFERRED (no dynamic evidence)
    if not template_result.templates:
        return ClassificationResult(
            classification="INFERRED",
            source_profile_digest=profile.digest,
            log_template_digest=template_result.digest,
            matched_patterns=[],
            unmatched_patterns=sorted(source_data_access_kinds),
            reasoning="No log output to confirm data-access execution.",
            _source_data_access_kinds=total_kinds,
        )

    # Rule 3: Check log evidence for each source-level data-access kind
    # Pass raw logs for fallback scan when templates are over-generalized
    matched, unmatched = _check_log_evidence(
        template_result.templates, source_data_access_kinds,
        raw_logs=logs,
    )

    # Rule 4: At least one corroborated pattern -> ANALYTICAL
    if matched:
        return ClassificationResult(
            classification="ANALYTICAL",
            source_profile_digest=profile.digest,
            log_template_digest=template_result.digest,
            matched_patterns=matched,
            unmatched_patterns=unmatched,
            reasoning=(
                f"Source-level data-access patterns corroborated by log evidence: "
                f"{', '.join(matched)}."
            ),
            _source_data_access_kinds=total_kinds,
        )

    # Default: INFERRED (source has data-access patterns but logs don't confirm)
    return ClassificationResult(
        classification="INFERRED",
        source_profile_digest=profile.digest,
        log_template_digest=template_result.digest,
        matched_patterns=[],
        unmatched_patterns=unmatched,
        reasoning=(
            "Source code has data-access patterns but log templates "
            "do not contain evidence of data-access execution."
        ),
        _source_data_access_kinds=total_kinds,
    )


def verify_classification(
    source: str,
    logs: list[str],
    claimed_classification: str,
    *,
    evidence_payload: dict | None = None,
) -> VerificationResult:
    """Verify whether a claimed classification matches the derivation.

    Re-derives classification from the same (source, logs) artifacts
    and checks agreement with the claimed value. When an evidence_payload
    is provided, also checks that the payload matches the re-derivation
    (tamper detection for digests and classification).

    Args:
        source: Python source code of the agent.
        logs: Raw log lines (stdout/stderr) from the agent's run.
        claimed_classification: The classification the wrapper declared.
        evidence_payload: Optional evidence dict (from to_evidence()) to
            validate against re-derivation.

    Returns:
        VerificationResult indicating whether the derivation agrees
        and whether the evidence payload is untampered.
    """
    result = derive_classification(source, logs)

    agrees = result.classification == claimed_classification
    evidence_agrees: bool | None = None
    reasoning_parts = [
        f"Derived: {result.classification}.",
        f"Claimed: {claimed_classification}.",
        "Agreement confirmed." if agrees else "MISMATCH detected.",
        f"Derivation reasoning: {result.reasoning}",
    ]

    # Evidence-payload tamper detection
    if evidence_payload is not None:
        re_derived_evidence = result.to_evidence()
        evidence_agrees = True
        mismatches: list[str] = []

        # Check digest agreement
        if evidence_payload.get("source_profile_digest") != re_derived_evidence["source_profile_digest"]:
            evidence_agrees = False
            mismatches.append("source_profile_digest mismatch")

        if evidence_payload.get("log_template_digest") != re_derived_evidence["log_template_digest"]:
            evidence_agrees = False
            mismatches.append("log_template_digest mismatch")

        # Check classification agreement within evidence
        if evidence_payload.get("classification") != re_derived_evidence["classification"]:
            evidence_agrees = False
            mismatches.append("evidence classification mismatch")

        if mismatches:
            reasoning_parts.append(
                f"Evidence payload tamper detected: {'; '.join(mismatches)}."
            )
        else:
            reasoning_parts.append("Evidence payload verified: all digests match re-derivation.")

    return VerificationResult(
        agrees=agrees,
        derived_classification=result.classification,
        claimed_classification=claimed_classification,
        reasoning=" ".join(reasoning_parts),
        evidence_agrees=evidence_agrees,
    )
