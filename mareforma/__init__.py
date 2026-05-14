"""Mareforma — local epistemic substrate for AI-assisted research."""

__description__ = "Mareforma — local epistemic substrate for AI-assisted research."
__version__ = "0.3.0"

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mareforma._graph import EpistemicGraph


def open(  # noqa: A001
    path: "str | Path | None" = None,
    *,
    key_path: "str | Path | None" = None,
    require_signed: bool = False,
    rekor_url: "str | None" = None,
    require_rekor: bool = False,
    trust_insecure_rekor: bool = False,
    rekor_log_pubkey_pem: "bytes | None" = None,
    rekor_log_pubkey_path: "str | Path | None" = None,
) -> "EpistemicGraph":
    """Open the epistemic graph at *path* and return an EpistemicGraph.

    Parameters
    ----------
    path:
        Project root directory. Defaults to the current working directory.
        The graph is stored at <path>/.mareforma/graph.db and is created
        on first use.
    key_path:
        Path to an Ed25519 private key (PEM). If ``None``, the XDG default
        ``~/.config/mareforma/key`` is used. Run ``mareforma bootstrap`` once
        to create it. If the path does not exist and ``require_signed`` is
        False, the graph operates in unsigned mode (claims persist with
        ``signature_bundle=NULL``).
    require_signed:
        When True, raise :class:`mareforma.signing.KeyNotFoundError` if no
        key is found at ``key_path``. Use for high-assurance contexts where
        an unsigned claim is unacceptable.
    rekor_url:
        Transparency-log endpoint. When set, every signed claim is submitted
        to Rekor at INSERT time; the entry uuid + logIndex are attached to
        the signature bundle and ``transparency_logged`` is set to 1.
        Submission failure persists the claim with ``transparency_logged=0``
        and blocks REPLICATED promotion (mirrors the DOI ``unresolved``
        pattern). ``EpistemicGraph.refresh_unsigned()`` retries the
        pending entries. ``None`` (default) disables Rekor entirely — signed
        claims still REPLICATE based on the local signature alone.
        Use :data:`mareforma.signing.PUBLIC_REKOR_URL` for the public
        sigstore instance.
    require_rekor:
        When True, ``rekor_url`` must be set and the initial submission must
        succeed; otherwise :class:`mareforma.signing.SigningError` is
        raised. Use for production-grade high-assurance flows.
    trust_insecure_rekor:
        Skip the SSRF-defense URL validation that otherwise rejects
        non-https schemes and loopback / private / link-local IP literals.
        Only use this when intentionally pointing at a private Rekor
        instance on an internal network.
    rekor_log_pubkey_pem:
        Rekor log operator's public key as PEM bytes. When supplied,
        every signed-claim submit and every restore cross-verifies the
        log's signed Merkle root via :func:`mareforma.signing.verify_rekor_inclusion`.
        Without it, the substrate trusts only the submit-time response
        binding — see "Limits of the Rekor integration" in the README.
        Accepts both Ed25519 (private Rekor deployments) and
        ECDSA-P256 (public Sigstore Rekor). Persisted on first use to
        ``<root>/.mareforma/rekor_log_pubkey.pem``; a mismatch on
        subsequent open() raises (TOFU pin).
    rekor_log_pubkey_path:
        Alternative to ``rekor_log_pubkey_pem``: filesystem path to a
        PEM file. The two are mutually exclusive. If neither is
        supplied AND ``<root>/.mareforma/rekor_log_pubkey.pem`` exists
        from a prior open(), it is loaded automatically.

    Returns
    -------
    EpistemicGraph
        Agent-native interface for asserting and querying claims.
        Use as a context manager to ensure the connection is closed.

    Example
    -------
    >>> graph = mareforma.open()                       # signs if XDG key exists
    >>> graph = mareforma.open(require_signed=True)    # raises if no key
    >>> graph = mareforma.open(rekor_url=mareforma.signing.PUBLIC_REKOR_URL)
    >>> claim_id = graph.assert_claim("...", classification="ANALYTICAL")
    >>> graph.close()

    Or with a context manager::

        with mareforma.open() as graph:
            graph.assert_claim("...")
    """
    from mareforma._graph import EpistemicGraph
    from mareforma.db import open_db
    from mareforma import signing as _signing

    root = Path(path) if path is not None else Path.cwd()
    conn = open_db(root)

    resolved_key_path = (
        Path(key_path) if key_path is not None else _signing.default_key_path()
    )
    signer = None
    if resolved_key_path.exists():
        signer = _signing.load_private_key(resolved_key_path)
    elif require_signed:
        raise _signing.KeyNotFoundError(
            f"No private key at {resolved_key_path}. Run `mareforma bootstrap` "
            "to create one, or call mareforma.open(require_signed=False) to "
            "operate in unsigned mode."
        )

    if require_rekor and rekor_url is None:
        raise _signing.SigningError(
            "require_rekor=True needs an explicit rekor_url. Pass "
            "mareforma.signing.PUBLIC_REKOR_URL or your private Rekor "
            "instance URL."
        )

    if rekor_url is not None:
        # Fail fast: validate before anyone calls assert_claim. Internal
        # Rekor instances on private networks need the explicit opt-in.
        _signing.validate_rekor_url(
            rekor_url, allow_insecure=trust_insecure_rekor,
        )

    # Rekor log-operator pubkey resolution with TOFU pin. The pinned PEM
    # lives at <root>/.mareforma/rekor_log_pubkey.pem after first use.
    # Resolution precedence:
    #   1. rekor_log_pubkey_pem (explicit bytes)        — highest
    #   2. rekor_log_pubkey_path (explicit filesystem)
    #   3. pinned file from prior open()                — TOFU continuation
    #   4. None                                          — verification disabled
    if rekor_log_pubkey_pem is not None and rekor_log_pubkey_path is not None:
        raise ValueError(
            "Pass either rekor_log_pubkey_pem or rekor_log_pubkey_path, "
            "not both — the two are mutually exclusive."
        )
    if rekor_log_pubkey_path is not None:
        rekor_log_pubkey_pem = Path(rekor_log_pubkey_path).read_bytes()
    _pinned_path = root / ".mareforma" / "rekor_log_pubkey.pem"
    if rekor_log_pubkey_pem is None and _pinned_path.exists():
        # Continue with the pinned key from a prior session.
        rekor_log_pubkey_pem = _pinned_path.read_bytes()
    elif rekor_log_pubkey_pem is not None and _pinned_path.exists():
        # TOFU pin enforcement: refuse silent log-key rotation. Any
        # rotation must be done explicitly (delete the pin file).
        existing = _pinned_path.read_bytes()
        if existing.strip() != rekor_log_pubkey_pem.strip():
            raise _signing.SigningError(
                f"Rekor log pubkey at {_pinned_path} pins a different key "
                "than the one supplied. Silent key rotation is refused. "
                "To intentionally rotate, delete the pin file first."
            )
    elif rekor_log_pubkey_pem is not None:
        # First use: persist the pin.
        _pinned_path.parent.mkdir(parents=True, exist_ok=True)
        _pinned_path.write_bytes(rekor_log_pubkey_pem)
    # NOTE: TOFU auto-fetch is intentionally off by default. Callers
    # who want Merkle inclusion-proof verification must pass
    # ``rekor_log_pubkey_pem`` or ``rekor_log_pubkey_path`` explicitly
    # (or pre-pin the file at ``.mareforma/rekor_log_pubkey.pem``).
    # This keeps the open() path I/O-free when no key is configured
    # and avoids surprise GETs to /api/v1/log/publicKey for every
    # Rekor-enabled session.

    return EpistemicGraph(
        conn, root,
        signer=signer,
        rekor_url=rekor_url,
        require_rekor=require_rekor,
        rekor_log_pubkey_pem=rekor_log_pubkey_pem,
    )


def schema() -> dict:
    """Return the mareforma epistemic schema — valid values and state transitions.

    Intended for agents that need to reason about the system before calling it.
    The returned dict is stable across patch releases; fields are only added,
    never removed, within a major version.

    Returns
    -------
    dict with keys:
        schema_version  : int — schema version stored in graph.db
        classifications : list[str] — valid classification values
        support_levels  : list[str] — valid support_level values, ordered low→high
        statuses        : list[str] — valid claim status values
        defaults        : dict — default value for each field at assert_claim() time
        transitions     : list[dict] — valid support_level state transitions

    Example
    -------
    >>> s = mareforma.schema()
    >>> s["classifications"]
    ['INFERRED', 'ANALYTICAL', 'DERIVED']
    >>> s["transitions"]
    [{'from': 'PRELIMINARY', 'to': 'REPLICATED', ...}, ...]
    """
    from mareforma.db import (
        _SCHEMA_VERSION,
        VALID_CLASSIFICATIONS,
        VALID_SUPPORT_LEVELS,
        VALID_STATUSES,
    )

    return {
        "schema_version": _SCHEMA_VERSION,
        "classifications": list(VALID_CLASSIFICATIONS),
        "support_levels": list(VALID_SUPPORT_LEVELS),
        "statuses": list(VALID_STATUSES),
        "defaults": {
            "classification": "INFERRED",
            "support_level": "PRELIMINARY",
            "status": "open",
            "generated_by": "agent",  # EpistemicGraph default
        },
        "transitions": [
            {
                "from": "PRELIMINARY",
                "to": "REPLICATED",
                "trigger": "automatic",
                "condition": (
                    "≥2 claims with different generated_by share the same "
                    "upstream claim_id in supports[]"
                ),
            },
            {
                "from": "REPLICATED",
                "to": "ESTABLISHED",
                "trigger": "human",
                "condition": "graph.validate(claim_id, validated_by=...) — no automated path",
            },
        ],
    }


from mareforma.prompt_safety import safe_for_llm, sanitize_for_llm, wrap_untrusted


def restore(
    project_root: "str | Path",
    *,
    claims_toml: "str | Path | None" = None,
) -> dict:
    """Rebuild a fresh graph.db from claims.toml.

    Catastrophic-loss recovery: the project's ``graph.db`` is missing
    or corrupt, the operator has a recent ``claims.toml``, and the
    graph must be reconstructed. Restore is **fresh-only** — it
    refuses to run when the target ``graph.db`` already contains
    claims. Wipe ``graph.db`` first if overwriting is the intent.

    Every signature is verified: validator enrollment envelopes
    against their parent keys, claim signature bundles against the
    enrolled signer keys, and validation envelopes against the
    validator keys. Any failure rolls back the entire transaction —
    fail-all-or-nothing.

    Parameters
    ----------
    project_root:
        Project directory. ``graph.db`` is reconstructed under
        ``<project_root>/.mareforma/``.
    claims_toml:
        Path to the source TOML. Defaults to
        ``<project_root>/claims.toml``.

    Returns
    -------
    dict
        ``{"validators_restored": N, "claims_restored": M}``.

    Raises
    ------
    mareforma.db.RestoreError
        With a ``.kind`` field naming the failure mode: graph_not_empty,
        toml_not_found, toml_malformed, enrollment_unverified,
        claim_unverified, mode_inconsistent, or orphan_signer.
    """
    from mareforma.db import restore as _restore
    return _restore(project_root, claims_toml=claims_toml)


from mareforma._evidence import EvidenceVector, EvidenceVectorError

# Re-export the user-catchable exception surface. AGENTS.md / docstrings
# document these as raise paths from the public API (assert_claim,
# validate, update_claim, restore, refresh_unsigned, etc.), and users
# previously had to import them from submodules (mareforma.db,
# mareforma.signing, mareforma.validators, mareforma._evidence) — the
# last of which is underscore-prefixed and therefore confusing. Make
# the catch surface match the documented contract by exposing
# everything at the top level.
from mareforma.db import (
    MareformaError,
    DatabaseError,
    ClaimNotFoundError,
    SignedClaimImmutableError,
    IdempotencyConflictError,
    IllegalStateTransitionError,
    ChainIntegrityError,
    LLMValidatorPromotionError,
    SelfValidationError,
    EvidenceCitationError,
    InvalidValidationEnvelopeError,
    RestoreError,
    CycleDetectedError,
    VerdictIssuerError,
)
from mareforma.signing import (
    SigningError,
    KeyNotFoundError,
    KeyPermissionError,
    InvalidEnvelopeError,
    RekorInclusionError,
)
from mareforma.validators import ValidatorNotEnrolledError


__all__ = [
    "open",
    "schema",
    "restore",
    "EvidenceVector",
    "safe_for_llm",
    "sanitize_for_llm",
    "wrap_untrusted",
    "__version__",
    # User-catchable exceptions (alphabetical under MareformaError).
    "MareformaError",
    "ChainIntegrityError",
    "ClaimNotFoundError",
    "CycleDetectedError",
    "DatabaseError",
    "EvidenceCitationError",
    "EvidenceVectorError",
    "IdempotencyConflictError",
    "IllegalStateTransitionError",
    "InvalidEnvelopeError",
    "InvalidValidationEnvelopeError",
    "KeyNotFoundError",
    "KeyPermissionError",
    "RekorInclusionError",
    "LLMValidatorPromotionError",
    "RestoreError",
    "SelfValidationError",
    "SignedClaimImmutableError",
    "SigningError",
    "ValidatorNotEnrolledError",
    "VerdictIssuerError",
]


def __dir__() -> list[str]:
    """Filter ``dir(mareforma)`` to the public API.

    ``Path`` and ``TYPE_CHECKING`` are imported at module scope because
    ``open()`` uses them at runtime, but they should not surface in
    tab-completion or be confused for public mareforma surface.
    """
    return sorted(__all__)