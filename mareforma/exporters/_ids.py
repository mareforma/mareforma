"""@id-safety rules shared by the JSON-LD exporters.

Every exporter writes claim_ids and agent identifiers into the same URN
and fragment space, so the rule for what may be spliced in lives here
once. A per-exporter copy would let a tightening land on one side only,
and the two exports of one graph would then disagree on which claims
can leave.

UUID-shape claim_ids only. Federation imports preserve foreign IDs in
the graph; exporters refuse to splice non-UUID values into
``urn:mareforma:claim:<id>`` URIs because they would silently break
downstream URN parsing and JSON-LD @id resolution.
"""

from __future__ import annotations

import re
from urllib.parse import quote


__all__ = [
    "UUID_RE",
    "AGENT_SAFE_CHARS",
    "safe_agent_id",
    "require_uuid_claim_id",
]


# End-anchored with \Z (not $, which would let a trailing newline
# through). A claim_id and the same claim_id with a trailing newline are
# two rows in the graph but one @id to any consumer that trims IRIs, so
# the guard has to see the difference the export would lose.
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)
# generated_by is allowed slashes ("model/version/context") and dashes,
# but `#`, whitespace, or shell-meta chars would break JSON-LD @id.
AGENT_SAFE_CHARS = "._/-"


def safe_agent_id(agent: str) -> str:
    """Coerce an agent identifier into a JSON-LD-@id-safe form.

    Percent-encodes any character outside ``[A-Za-z0-9._/-]`` so the
    resulting ``#agent/<escaped>`` fragment parses correctly in every
    JSON-LD consumer. The escape is reversible, so two producers whose
    names differ only in an unsafe character keep distinct ids.
    """
    return quote(agent, safe=AGENT_SAFE_CHARS)


def require_uuid_claim_id(claim_id: str, exporter: str) -> str:
    """Return ``claim_id`` if it is UUID-shaped, else refuse.

    Refuse, don't sanitise: a foreign id remapped by the exporter would
    lose the identity the graph recorded. ``exporter`` names the caller
    in the message ("PROV-O", "RO-Crate").
    """
    if not isinstance(claim_id, str) or not UUID_RE.match(claim_id):
        raise ValueError(
            f"{exporter} export refuses non-UUID claim_id: {claim_id!r}. "
            "Federation-imported foreign IDs must be remapped to UUIDs "
            "before export."
        )
    return claim_id
