"""Core-derived classification.

Deterministically derives a claim's ``classification`` (ANALYTICAL or
INFERRED) from two evidence sources: a static profile of the agent's
source code and dynamic templates extracted from its runtime logs.
ANALYTICAL requires positive evidence from BOTH; INFERRED is the
conservative default.

The public surface:

- :func:`derive_classification` — produce a :class:`ClassificationResult`
  from source + logs.
- :func:`verify_classification` — re-derive and compare against a
  claimed classification; flag tamper if evidence drifts.
- :func:`extract_source_profile` / :func:`extract_directory_profile`
  — static source profilers (require the ``[derivation]`` extra:
  ``pip install mareforma[derivation]``).
- :func:`extract_templates` — runtime-log template extraction via
  Drain parser.
- :data:`DERIVATION_VERSION` — version of the derivation algorithm.
  Bumped when the evidence format changes.
"""

from __future__ import annotations

from mareforma.derivation.deriver import (
    DERIVATION_VERSION,
    ClassificationResult,
    VerificationResult,
    derive_classification,
    verify_classification,
)
from mareforma.derivation.log_templates import (
    DrainParser,
    LogTemplate,
    TemplateResult,
    extract_templates,
)
from mareforma.derivation.source_profile import (
    HAS_TREE_SITTER,
    SourcePattern,
    SourceProfile,
    extract_directory_profile,
    extract_source_profile,
)


__all__ = [
    "ClassificationResult",
    "DERIVATION_VERSION",
    "DrainParser",
    "HAS_TREE_SITTER",
    "LogTemplate",
    "SourcePattern",
    "SourceProfile",
    "TemplateResult",
    "VerificationResult",
    "derive_classification",
    "extract_directory_profile",
    "extract_source_profile",
    "extract_templates",
    "verify_classification",
]
