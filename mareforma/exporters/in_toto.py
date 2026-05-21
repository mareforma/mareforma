"""In-toto Statement v1 export of the whole epistemic graph.

Substrate-level adapter (whole graph → in-toto v1 envelope). Distinct
from :mod:`mareforma.export_bundle`: this module produces an **unsigned**
Statement v1 dict, leaving signing + DSSE wrap to the caller. Use cases:

* Pipe to ``cosign attest`` / ``slsa-verifier`` / GUAC, which sign with
  their own keys.
* Embed inside a higher-level provenance package (e.g. an RO-Crate
  Process Run Crate that also carries an in-toto attestation).
* Test fixtures + cross-conformance checks (item T7 in eng review)
  that compare in-toto + RO-Crate views of the same graph.

For a signed bundle ready to verify with ``mareforma verify``, use
:mod:`mareforma.export_bundle` (`mareforma export --bundle`).

Predicate type: ``urn:mareforma:predicate:epistemic-graph:v1`` (matches
existing v0.3.0 signed-bundle predicate type for round-trip
compatibility). Each claim appears as a subject under
``urn:mareforma:claim:<uuid>`` with a SHA-256 of the canonical
Statement v1 bytes — same shape :mod:`mareforma.export_bundle` already
uses, so verifiers that already know the signed-bundle shape need no
new code path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


__all__ = ["build_statement", "IN_TOTO_STATEMENT_TYPE", "PREDICATE_TYPE"]


IN_TOTO_STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
PREDICATE_TYPE = "urn:mareforma:predicate:epistemic-graph:v1"


def build_statement(root: Path) -> dict[str, Any]:
    """Build an unsigned in-toto Statement v1 dict from the local graph.

    Re-uses :func:`mareforma.export_bundle.build_statement` so the
    substrate has one canonical shape for "the graph as an in-toto
    Statement." The caller is responsible for signing (or not) the
    returned dict.

    Parameters
    ----------
    root
        Project root (the directory holding ``.mareforma/graph.db``).

    Returns
    -------
    dict
        An in-toto Statement v1 dict ready for ``json.dumps`` or for
        wrapping in a DSSE envelope.
    """
    # Delegate to the signed-bundle helper which already builds the
    # Statement v1 dict in the right shape. This keeps a single source
    # of truth for the predicate shape; the v0.3.0 bundle verifier and
    # the v0.3.1 unsigned exporter cannot drift.
    from mareforma.export_bundle import build_statement as _build
    return _build(root)
