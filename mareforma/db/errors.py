"""Exception hierarchy for mareforma."""


class MareformaError(Exception):
    """Base exception for all mareforma errors."""


class DatabaseError(MareformaError):
    """Raised when a graph.db operation fails."""


class ScanCeilingReached(DatabaseError):
    """Raised when a read surface exhausts its scan ceiling short of ``limit``.

    ``query_claims`` and ``search_claims`` bound how many ordered rows they
    materialise. Rows dropped by verify-on-read do not count as survivors, so a
    flood of rows that fail that check can fill the ceiling before ``limit``
    real matches are reached. Returning the short list would be indistinguishable
    from an empty graph, and the query-before-asserting pattern would then
    re-assert what is already recorded, so the read refuses instead. Narrow the
    query or lower ``limit`` to bring the survivors inside the ceiling.
    """


class UnverifiedClaimError(MareformaError):
    """Raised when an export is asked to publish a claim that failed verify-on-read.

    The interop exports (JSON-LD, PROV-O, RO-Crate) carry a claim's
    ``support_level`` off the machine, where nothing re-checks it. A REPLICATED
    or ESTABLISHED row whose signature does not re-verify must not leave with
    that level attached, and demoting it silently would hide the tamper, so the
    export refuses and names the rows. Run ``mareforma verify <claim_id>`` for
    the detail, then retract or repair the row.
    """


class ClaimNotFoundError(MareformaError):
    """Raised when a claim lookup finds no matching record."""


class SignedClaimImmutableError(MareformaError):
    """Raised when `update_claim` is asked to mutate a signed-surface field.

    Once a claim has a signature attached, mutating any field that was part
    of the signed payload (``text``, ``supports``, ``contradicts``,
    ``classification``, ``generated_by``, ``source_name``) would invalidate
    the signature without surfacing the change. To revise a signed claim,
    retract the old one (``status='retracted'``) and assert a new one that
    cites the old via ``contradicts=[<old_claim_id>]``.
    """


class IdempotencyConflictError(MareformaError):
    """Raised when an idempotency_key replay arrives with conflicting fields.

    Idempotency means "same logical operation." A retry that supplies a
    different ``artifact_hash`` is not a retry: it is a different claim
    that happens to share a key. Silently returning the first claim_id
    would let a caller believe their new hash was registered when it was
    not, losing tamper-evidence in the process. Surface the inconsistency.
    """


class IllegalStateTransitionError(MareformaError):
    """Raised when an SQLite state-machine trigger refuses a transition.

    The trigger raises ``mareforma:state:<from>-><to>`` strings via
    ``RAISE(ABORT, ...)``. Python catches the resulting
    ``sqlite3.IntegrityError`` and re-raises this exception with the
    parsed transition so callers can pattern-match on it instead of
    parsing opaque ``CHECK CONSTRAINT FAILED`` messages.
    """


class ChainIntegrityError(MareformaError):
    """Raised when the ``prev_hash`` append-only chain cannot extend.

    The chain hash is computed under ``BEGIN IMMEDIATE`` to serialize
    writers, and the ``prev_hash`` column carries a ``UNIQUE`` index.
    If a second writer races past the lock, or a raw-SQL tamper
    re-uses an existing chain link, the UNIQUE violation surfaces
    here. Treat it as a corruption signal, not a retry.
    """


class LLMValidatorPromotionError(MareformaError):
    """Raised when a validator with ``validator_type='llm'`` attempts
    a promotion past REPLICATED.

    The trust ladder treats human validators as the only path to
    ESTABLISHED. An LLM-typed validator may enroll and may sign
    validation envelopes, but those envelopes cannot promote a claim
    past REPLICATED. To promote, the claim must be co-signed (or
    re-signed) by an enrolled human validator.
    """


class SelfValidationError(MareformaError):
    """Raised when a validator attempts to promote a claim it signed itself.

    Self-validation is the trivial-loop attack: an agent asserts a claim
    under its own key, then promotes that same claim to ESTABLISHED under
    the same key. The trust ladder rests on the principle that promotion
    is an *external* witnessing event. ``validate_claim`` compares the
    signing keyid of the validation envelope with the keyid recorded in
    the claim's ``signature_bundle`` and refuses when they match.
    """


class EvidenceCitationError(MareformaError):
    """Raised when ``evidence_seen`` on a validation envelope is malformed.

    ``validate_claim`` accepts an ``evidence_seen`` list of claim_ids the
    validator declares to have reviewed before signing the promotion.
    Mareforma cannot prove the validator actually opened those claims,
    but it CAN verify that every cited entry is a strict-v4 UUID pointing
    at an existing claim with ``created_at <= validated_at``. Any failure
    in that check raises this exception:

      * non-string entry,
      * UUID that does not match the strict-v4 pattern,
      * claim_id that does not exist in the graph,
      * claim_id whose ``created_at`` is later than the validation
        timestamp (the validator could not have reviewed a claim that
        didn't exist yet),
      * envelope's ``evidence_seen`` field does not equal the
        ``evidence_seen`` kwarg passed alongside (the envelope's signed
        citations must match the mareforma-validated kwarg byte-for-byte).

    The error message names the first failing entry so the caller can
    fix it without trial-and-error.
    """


class InvalidValidationEnvelopeError(MareformaError):
    """Raised when a validation envelope is structurally or cryptographically
    invalid.

    Distinct from :class:`EvidenceCitationError` (which is specifically
    about evidence_seen citations failing mareforma's existence /
    timestamp check). This exception fires when the envelope itself
    fails any of mareforma's defense-in-depth gates inside
    :func:`validate_claim`:

      * envelope cannot be parsed as JSON or is missing required fields,
      * envelope's ``payloadType`` is neither validation nor seed,
      * envelope's signing keyid is not an enrolled validator,
      * envelope fails Ed25519 verification against the claimed signer's
        public key (cryptographic forgery or wrong signer),
      * envelope's payload binds a different ``claim_id`` than the row
        being promoted (replay across claims),
      * envelope's payload binds a ``validator_keyid`` that does not
        equal the signing keyid (internal inconsistency),
      * envelope's payload's timestamp (``validated_at`` for validation
        envelopes, ``seeded_at`` for seed envelopes) does not equal the
        ``validated_at`` value being written.

    These checks make :func:`validate_claim` safe to call directly:
    bypassing :meth:`EpistemicGraph.validate` does not relax any
    mareforma-level invariant. A caller that hand-crafts an envelope to
    impersonate an enrolled human validator will fail one of these
    gates before any row is mutated.
    """


class RestoreError(MareformaError):
    """Raised by :func:`restore` when the rebuild refuses or fails.

    The ``kind`` attribute lets callers pattern-match on the failure
    mode without parsing the message string:

      - ``'graph_not_empty'``          : existing graph.db has claims
      - ``'toml_not_found'``           : claims.toml does not exist
      - ``'toml_unreadable'``          : claims.toml exists but cannot be read
      - ``'toml_malformed'``           : TOML parse error
      - ``'enrollment_unverified'``    : enrollment envelope fails verify
      - ``'claim_unverified'``         : claim signature fails verify
      - ``'mode_inconsistent'``        : signed-mode graph with unsigned claim
      - ``'orphan_signer'``            : claim signed by an unenrolled keyid
      - ``'policy_absent'``            : enforce_rekor_policy set but no signed policy
      - ``'policy_unverifiable'``      : enforced policy has no pinned Rekor log key
      - ``'policy_unverified'``        : project_policy envelope fails verify
      - ``'policy_violation'``         : rebuilt row breaks the signed policy
      - ``'rekor_inclusion_invalid'``  : Rekor inclusion entry or proof invalid
    """

    def __init__(self, message: str, *, kind: str) -> None:
        super().__init__(message)
        self.kind = kind


class CycleDetectedError(MareformaError):
    """Raised when an INSERT or UPDATE would create a cycle in ``supports[]``.

    The graph of claim → upstream supports is required to be acyclic.
    Self-loops (a claim that supports itself) and indirect cycles
    introduced by mutating ``supports`` on an unsigned claim are both
    rejected. Signed claims cannot mutate ``supports`` at all (see
    :class:`SignedClaimImmutableError`), so the cycle window is the
    unsigned-edit path.
    """


class GraphTooLargeError(MareformaError):
    """Raised when a new claim's ``supports[]`` transitively reach more distinct
    upstream nodes than the reachable-claim cap allows.

    This is a pathological-graph guard, NOT a cycle. The acyclicity walk stops
    at the cap; a legitimate but enormous fan-out (a foundational claim cited
    across a mature graph) can reach very many claims at shallow depth.
    Rejecting that write is a safety valve against a runaway walk, and it is
    reported as its own condition so a caller is never told "cycle" when the
    graph is merely large. The walk says nothing about the part it did not
    reach, so a cycle beyond the cap surfaces here rather than as
    :class:`CycleDetectedError`. Either way the write is refused.
    """




class ProjectPolicyError(MareformaError, ValueError):
    """Raised when a project-policy declaration is refused.

    The project policy (Rekor witnessing, strict promotion) is root-signed,
    project-wide and one-way, so declaring one is a privileged write, not a
    handle setting. This fires when the caller cannot make that declaration:
    no signing key is loaded, the loaded key is not the project's single root
    validator, or a concurrent writer already declared a rule the envelope in
    hand does not carry. Refusing is the point: a caller who asked for the
    stricter rule and cannot record it must not proceed believing it holds.

    Subclasses ``ValueError`` because the declaration surfaces raised that
    before the policy grew its own type.
    """


class VerdictIssuerError(MareformaError):
    """Raised when a verdict-issuer write is refused.

    Reasons: issuer not enrolled, referenced claim_id missing, method
    not in the allowed enum, or the signature payload binding fails.
    """


class RekorSidecarSectionAbsentWarning(UserWarning):
    """Emitted once per restore when claims.toml has no ``[rekor_inclusions]`` section.

    This is the expected state for TOML files written by v0.3.1 or earlier.
    Every Rekor-logged claim restores successfully; the sidecar entries are
    simply absent. Run ``refresh_unsigned()`` after restore to re-fetch
    inclusion proofs from the log and rebuild the sidecar.
    """


class RekorSidecarEntryMissingWarning(UserWarning):
    """Emitted per claim when the ``[rekor_inclusions]`` section exists but
    lacks an entry for a Rekor-logged claim.

    Unlike :class:`RekorSidecarSectionAbsentWarning` (which signals a
    legitimate pre-v0.3.2 upgrade), a present-but-incomplete section
    suggests the TOML was edited to remove specific entries. The claim
    restores successfully, but the operator should investigate why the
    entry is missing.
    """


