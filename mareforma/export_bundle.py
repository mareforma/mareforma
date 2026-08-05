"""
export_bundle.py: signed export bundle.

Wraps the JSON-LD graph export in an in-toto Statement v1 envelope:

    {
      "_type": "https://in-toto.io/Statement/v1",
      "subject": [
        {
          "name":   "urn:mareforma:claim:<uuid>",
          "digest": {"sha256": "<canonical_statement_hash>"}
        },
        ...
      ],
      "predicateType": "urn:mareforma:predicate:epistemic-graph:v1",
      "predicate": <JSON-LD export>
    }

The bundle is then signed by the local Ed25519 key using a DSSE-style
envelope. Verification checks the bundle signature, every per-claim
asserter signature (bound to the claim's presented content), and the
displayed support level: ESTABLISHED against a validator-signed
validation envelope, REPLICATED against distinct-signer corroboration.
Editorial status (``retracted`` / ``contested``) and comparison
summaries are exporter-attested only: the data model records no
signature for a status change, so a verified bundle does not attest
them (use the retract-then-supersede pattern for a signed retraction).
Completeness is outside the bound as well. A verified bundle attests
the claims it carries, not that they are all the claims in the graph:
a claim removed together with its subject entry and re-signed by the
same key verifies clean, because nothing here counts the claims or
chains them.

Design choices (one-way doors, locked currently):

- **Subject naming**: ``urn:mareforma:claim:<uuid>``. The URN namespace
  is forever once published; consumers will key off this prefix.
- **predicateType**: ``urn:mareforma:predicate:epistemic-graph:v1``.
  URN (not DNS) defers a perpetual-ownership commitment on
  ``mareforma.dev`` for schema dereferencing. Evolution to v2 carries
  a new predicateType, leaving v1 verifiers intact.
- **Predicate body**: the existing JSON-LD export. No additional
  PROV-O modelling: the JSON-LD scoping rationale already covers
  why (see ``mareforma/exporters/jsonld.py`` module docstring).

The bundle shape is written up in ``docs/for-agents/agents.mdx`` under
"Export and signed bundles"; the command surface in
``docs/reference/cli.mdx`` under ``mareforma export``.
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from mareforma import __version__
from mareforma._atomic import atomic_write_text


STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
PREDICATE_TYPE = "urn:mareforma:predicate:epistemic-graph:v1"
SUBJECT_PREFIX = "urn:mareforma:claim:"
BUNDLE_PAYLOAD_TYPE = "application/vnd.in-toto+json"


def _subject_for_claim(claim: dict) -> dict[str, Any]:
    """Build one in-toto subject entry from a claim row.

    Hash material: the canonical Statement v1 bytes of the claim (the
    same bytes the per-claim signature is computed over via DSSE PAE).
    Reusing ``signing.canonical_statement`` keeps bundle digests
    aligned with per-claim signatures: a downstream tool that
    re-derives the digest from the row's fields + evidence_json must
    agree with the bundle.
    """
    from mareforma import signing as _signing
    try:
        evidence_dict = json.loads(claim.get("evidence_json") or "{}")
    except (ValueError, TypeError):
        evidence_dict = {}
    chain_input = _signing.canonical_statement({
        "claim_id": claim["claim_id"],
        "text": claim["text"],
        "classification": claim["classification"],
        "generated_by": claim["generated_by"],
        "supports": json.loads(claim.get("supports_json") or "[]"),
        "contradicts": json.loads(claim.get("contradicts_json") or "[]"),
        "source_name": claim.get("source_name"),
        "artifact_hash": claim.get("artifact_hash"),
        "created_at": claim["created_at"],
    }, evidence_dict)
    digest = hashlib.sha256(chain_input).hexdigest()
    return {
        "name": f"{SUBJECT_PREFIX}{claim['claim_id']}",
        "digest": {"sha256": digest},
    }


def build_statement(root: Path) -> dict[str, Any]:
    """Build the in-toto Statement v1 for the graph at *root*.

    The Statement is unsigned: call :func:`sign_bundle` to produce
    the DSSE envelope.

    Raises ``FileNotFoundError`` if *root* holds no graph: ``open_db``
    would otherwise create one and sign an empty statement.
    """
    from mareforma.db import open_db, list_claims
    from mareforma.exporters.jsonld import JSONLDExporter
    from mareforma import validators as _validators

    db_path = root / ".mareforma" / "graph.db"
    if not db_path.exists():
        raise FileNotFoundError(
            f"No epistemic graph found at {db_path}. "
            "Run `mareforma bootstrap` to initialize one."
        )

    conn = open_db(root)
    try:
        claims = list_claims(conn)
        single_trust_domain = _validators.single_trust_domain(conn)
        trust_domain_root = _validators.trust_domain_root(conn)
        validator_rows = _validators.list_validators(conn)
    finally:
        conn.close()
    subjects = [_subject_for_claim(c) for c in claims]
    predicate = JSONLDExporter(root).export()
    # Carry each claim's own asserter signature into its node so verify_bundle
    # can check it offline, and the enrolled validator set (with enrollment
    # envelopes) so those signatures verify against a chain-checked key rather
    # than collapsing to the exporter's. The digest is derived from named
    # fields, so this added field does not perturb it.
    sig_by_claim = {c["claim_id"]: c.get("signature_bundle") for c in claims}
    val_by_claim = {c["claim_id"]: c.get("validation_signature") for c in claims}
    for node in predicate.get("@graph", []):
        node_id = node.get("@id", "")
        if node_id.startswith("mare:claim/"):
            cid = node_id[len("mare:claim/"):]
            bundle_json = sig_by_claim.get(cid)
            if bundle_json:
                node["signatureBundle"] = json.loads(bundle_json)
            # An ESTABLISHED claim's promotion is attested by a validator's
            # signed validation (or seed) envelope; carry it so verify_bundle
            # can confirm the displayed support level, not just trust the
            # exporter for it.
            val_json = val_by_claim.get(cid)
            if val_json:
                node["validationSignature"] = json.loads(val_json)
    # Disclose the validator topology of the exporting graph: singleTrustDomain
    # is true when every validator traces to one root of trust. It labels
    # trust-domain concentration over the ESTABLISHED rows in this bundle; it is
    # a disclosure, not a Sybil guard over the participant topology.
    predicate = {
        **predicate,
        "mare:singleTrustDomain": single_trust_domain,
        "mare:trustDomainRoot": trust_domain_root,
        "mare:validators": [
            {
                "keyid": v["keyid"],
                "pubkey_pem": v["pubkey_pem"],
                "identity": v["identity"],
                "validator_type": v["validator_type"],
                "enrolled_at": v["enrolled_at"],
                "enrolled_by_keyid": v["enrolled_by_keyid"],
                "enrollment_envelope": v["enrollment_envelope"],
            }
            for v in validator_rows
        ],
    }
    return {
        "_type": STATEMENT_TYPE,
        "subject": subjects,
        "predicateType": PREDICATE_TYPE,
        "predicate": predicate,
    }


def sign_bundle(
    statement: dict[str, Any],
    private_key,  # Ed25519PrivateKey
) -> dict[str, Any]:
    """Wrap *statement* in a DSSE envelope signed by *private_key*.

    The envelope shape mirrors ``signing.sign_claim`` so consumers
    that already know how to verify mareforma claim envelopes can
    verify the bundle with the same primitives.
    """
    from mareforma import signing as _signing
    payload_bytes = json.dumps(
        statement, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    # Sign over the DSSE PAE encoding, not the raw payload, so the envelope
    # verifies with mareforma's own verify_envelope and any standard DSSE
    # verifier — the type is bound into the signature, not just carried beside
    # it.
    sig = private_key.sign(_signing.dsse_pae(BUNDLE_PAYLOAD_TYPE, payload_bytes))
    keyid = _signing.public_key_id(private_key.public_key())
    return {
        "payloadType": BUNDLE_PAYLOAD_TYPE,
        "payload": base64.standard_b64encode(payload_bytes).decode("ascii"),
        "signatures": [
            {
                "keyid": keyid,
                "sig": base64.standard_b64encode(sig).decode("ascii"),
            }
        ],
        "mare:bundleVersion": __version__,
    }


class BundleExportError(Exception):
    """Raised when a bundle would be written as an unverifiable artifact.

    The message states its own remedy, so the caller reports it verbatim.
    """


def _unsigned_claim_ids(statement: dict[str, Any]) -> list[str]:
    """Claim ids in *statement* that carry no asserter signature.

    Claims recorded before the project had a signing key stay unsigned, and
    nothing signs them after the fact. verify_bundle refuses such a row once
    the graph carries validators, so a bundle holding one reads as tampered.
    """
    nodes = (statement.get("predicate") or {}).get("@graph") or []
    prefix = "mare:claim/"
    return [
        node["@id"][len(prefix):]
        for node in nodes
        if node.get("@id", "").startswith(prefix) and not node.get("signatureBundle")
    ]


def write_bundle(root: Path, output_path: Path, private_key) -> Path:
    """Build, sign, and write a bundle. Returns the path written.

    Refuses when *private_key* is not the graph's root of trust. verify_bundle
    requires the signer to be the root every exported validator chains to, so
    signing with any other key writes a file no key can verify, and the
    recipient reads that as tamper. Fail here instead. Unsigned claims in a
    signed graph are refused for the same reason.
    """
    from mareforma import signing as _signing
    statement = build_statement(root)
    trust_root = statement["predicate"].get("mare:trustDomainRoot")
    if trust_root is None:
        raise BundleExportError(
            "this project has no single root of trust, so no bundle from it "
            "can verify; open it once with the key that should be its root. "
            "Pass the root key with --key."
        )
    keyid = _signing.public_key_id(private_key.public_key())
    if keyid != trust_root:
        raise BundleExportError(
            f"signing key {keyid[:12]}… is not this project's root of trust "
            f"{trust_root[:12]}…; a bundle must be signed by its root. "
            "Pass the root key with --key."
        )
    unsigned = _unsigned_claim_ids(statement)
    if unsigned:
        raise BundleExportError(
            "these claims carry no signature, and this project's graph is "
            "signed, so any bundle holding them verifies as tampered: "
            + ", ".join(unsigned)
            + ". They were recorded before the signing key existed and nothing "
            "signs them now. Export without --bundle to carry them."
        )
    bundle = sign_bundle(statement, private_key)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        output_path, json.dumps(bundle, indent=2, ensure_ascii=False),
    )
    return output_path


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


class BundleVerificationError(Exception):
    """Raised when a bundle fails verification.

    The exception message names the first failing check so the
    caller can route between "this is corrupt" and "this is a
    cross-version skew" without parsing English.
    """


class BundleKeyMismatchError(BundleVerificationError):
    """Raised when no signature on the bundle was made by the given key.

    A distinct type because this is a wrong-key condition, not evidence of
    tamper: the caller reports it as unverifiable unless the key was pinned.
    """


def _verify_exported_validators(
    validators: list[dict],
) -> tuple[dict[str, Any], dict[str, str], str]:
    """Verify the exported validator set.

    Returns (keyid -> public-key, keyid -> validator_type, root). The type
    comes from the enrollment payload the parent signed, so it is as
    trustworthy as the key it accompanies, and the ESTABLISHED check needs it
    to enforce the human-witnessed rule the graph enforces in process.

    Each enrollment envelope is verified against its parent's pubkey (the root
    is self-verified), mirroring the restore path, and every validator must
    chain to a single root of trust with no islands or cycles. Per-claim
    asserter signatures are then checked against these chain-verified keys, so a
    malicious exporter cannot substitute fabricated asserters. Any failure
    raises :class:`BundleVerificationError`.
    """
    from mareforma import signing as _signing
    from mareforma import validators as _validators

    by_keyid = {v.get("keyid"): v for v in validators}
    roots = [
        k for k, v in by_keyid.items() if v.get("enrolled_by_keyid") == k
    ]
    if len(roots) != 1:
        raise BundleVerificationError(
            f"validators:expected exactly one root of trust, found {len(roots)}"
        )
    root = roots[0]

    verified: dict[str, Any] = {}
    verified_types: dict[str, str] = {}
    for keyid, v in by_keyid.items():
        parent = by_keyid.get(v.get("enrolled_by_keyid"))
        if parent is None:
            raise BundleVerificationError(
                f"validators:{str(keyid)[:12]}… enrolled by a key absent "
                "from the bundle"
            )
        try:
            parent_pem = base64.standard_b64decode(parent["pubkey_pem"])
        except (ValueError, TypeError, KeyError) as exc:
            raise BundleVerificationError(
                f"validators:{str(keyid)[:12]}… parent pubkey not base64"
            ) from exc
        if not _validators.verify_enrollment(v, parent_pem):
            raise BundleVerificationError(
                f"validators:{str(keyid)[:12]}… enrollment failed verification"
            )
        try:
            verified[keyid] = _signing.public_key_from_pem(
                base64.standard_b64decode(v["pubkey_pem"])
            )
        except (ValueError, TypeError, KeyError, _signing.SigningError) as exc:
            raise BundleVerificationError(
                f"validators:{str(keyid)[:12]}… pubkey unparseable"
            ) from exc
        verified_types[keyid] = v.get("validator_type")

    # Every validator must reach the single root by walking parents — no
    # island component and no cycle can smuggle in an off-root asserter key.
    for keyid in by_keyid:
        seen: set = set()
        cur = keyid
        while cur != root:
            if cur in seen or cur not in by_keyid:
                raise BundleVerificationError(
                    f"validators:{str(keyid)[:12]}… does not chain to the root"
                )
            seen.add(cur)
            cur = by_keyid[cur].get("enrolled_by_keyid")
    return verified, verified_types, root


def _string_supports(supports: Any) -> list[str]:
    """The string entries of a node's ``supports``, ignoring anything else.

    A hand-crafted bundle could carry a non-list or nested/unhashable supports
    value; this keeps the distinct-signer pre-pass and REPLICATED check within
    the module's ``BundleVerificationError`` contract instead of leaking a
    TypeError. A malformed supports value simply contributes no upstream.
    """
    if not isinstance(supports, list):
        return []
    return [s for s in supports if isinstance(s, str)]


def _verify_established_level(
    node: dict, claim_id: str, verified_validators: dict,
    validator_types: dict, _signing,
) -> None:
    """Confirm a node displayed as ESTABLISHED carries a validator-signed
    promotion for THIS claim, so the exporter cannot inflate a claim's support
    level. Mirrors the validation-envelope checks the restore path applies."""
    vs = node.get("validationSignature")
    if not vs:
        raise BundleVerificationError(
            f"claim:{claim_id} is shown ESTABLISHED but carries no validation "
            "signature"
        )
    try:
        val_keyid = vs["signatures"][0]["keyid"]
        declared = vs["payloadType"]
    except (KeyError, IndexError, TypeError) as exc:
        raise BundleVerificationError(
            f"claim:{claim_id} validation signature is malformed"
        ) from exc
    if declared not in (
        _signing.PAYLOAD_TYPE_VALIDATION, _signing.PAYLOAD_TYPE_SEED,
    ):
        raise BundleVerificationError(
            f"claim:{claim_id} validation signature has unexpected payloadType "
            f"{declared!r}"
        )
    val_pub = verified_validators.get(val_keyid)
    if val_pub is None:
        raise BundleVerificationError(
            f"claim:{claim_id} validation signed by {str(val_keyid)[:12]}… "
            "which is not a chain-verified validator"
        )
    # ESTABLISHED means a human-typed validator witnessed the claim. The graph
    # refuses an llm-typed promotion in process; the bundle carries the
    # enrollment-bound validator_type, so the verifier refuses it too.
    if validator_types.get(val_keyid) == "llm":
        raise BundleVerificationError(
            f"claim:{claim_id} is shown ESTABLISHED but its validation is "
            f"signed by {str(val_keyid)[:12]}…, enrolled with "
            "validator_type='llm'"
        )
    try:
        ok = _signing.verify_envelope(vs, val_pub, expected_payload_type=declared)
    except _signing.InvalidEnvelopeError as exc:
        raise BundleVerificationError(
            f"claim:{claim_id} validation signature is structurally invalid: "
            f"{exc}"
        ) from exc
    if not ok:
        raise BundleVerificationError(
            f"claim:{claim_id} validation signature failed verification"
        )
    try:
        payload = _signing.envelope_payload(vs)
    except _signing.InvalidEnvelopeError as exc:
        raise BundleVerificationError(
            f"claim:{claim_id} validation payload is unparseable: {exc}"
        ) from exc
    if payload.get("claim_id") != claim_id:
        raise BundleVerificationError(
            f"claim:{claim_id} validation envelope binds a different claim_id "
            f"({payload.get('claim_id')!r})"
        )
    # The node's validatedBy is a cosmetic display label, not an identity, so
    # it is not checked here. The envelope's own declared validator is, and it
    # has to be the key that signed it.
    if payload.get("validator_keyid") != val_keyid:
        raise BundleVerificationError(
            f"claim:{claim_id} validation envelope declares validator "
            f"{str(payload.get('validator_keyid'))[:12]}… but is signed by "
            f"{str(val_keyid)[:12]}…"
        )


def verify_bundle(
    bundle_path: Path,
    public_key,  # Ed25519PublicKey
) -> dict[str, Any]:
    """Verify a bundle's DSSE envelope AND each per-claim signature.

    Returns the parsed Statement on success. Raises
    :class:`BundleVerificationError` on any check failure (envelope
    type wrong, signature mismatch, predicateType skew, per-claim
    digest mismatch, per-claim signature failure).

    A per-claim signature failure raises immediately: partial
    verification doesn't surface "you have an authenticated bundle
    that contains some invalid claims" as success.
    """
    from mareforma import signing as _signing

    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    if bundle.get("payloadType") != BUNDLE_PAYLOAD_TYPE:
        raise BundleVerificationError(
            f"bundle:payloadType mismatch: got "
            f"{bundle.get('payloadType')!r}, expected {BUNDLE_PAYLOAD_TYPE!r}"
        )

    # Verify bundle-level DSSE.
    try:
        payload_bytes = base64.standard_b64decode(bundle["payload"])
    except (KeyError, ValueError) as exc:
        raise BundleVerificationError(f"bundle:payload decode failed: {exc}") from exc

    sigs = bundle.get("signatures") or []
    if not sigs:
        raise BundleVerificationError("bundle:signatures missing or empty")

    keyid = _signing.public_key_id(public_key)
    matching = [s for s in sigs if s.get("keyid") == keyid]
    if not matching:
        raise BundleKeyMismatchError(
            f"bundle:no signature matches the given public key (keyid {keyid[:12]}…)"
        )
    try:
        sig_bytes = base64.standard_b64decode(matching[0]["sig"])
    except (KeyError, ValueError) as exc:
        raise BundleVerificationError(f"bundle:signature decode failed: {exc}") from exc

    from cryptography.exceptions import InvalidSignature
    try:
        public_key.verify(
            sig_bytes, _signing.dsse_pae(BUNDLE_PAYLOAD_TYPE, payload_bytes),
        )
    except InvalidSignature as exc:
        raise BundleVerificationError(
            "bundle:signature verification failed — bundle has been tampered"
        ) from exc

    # Parse the verified Statement.
    try:
        statement = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BundleVerificationError(f"bundle:payload not JSON: {exc}") from exc

    if statement.get("_type") != STATEMENT_TYPE:
        raise BundleVerificationError(
            f"statement:_type mismatch: got {statement.get('_type')!r}, "
            f"expected {STATEMENT_TYPE!r}"
        )
    if statement.get("predicateType") != PREDICATE_TYPE:
        raise BundleVerificationError(
            f"statement:predicateType mismatch: got "
            f"{statement.get('predicateType')!r}, expected {PREDICATE_TYPE!r}"
        )

    # Verify each subject digest against the corresponding claim's
    # canonical_statement bytes in the predicate.
    subjects = {s["name"]: s["digest"]["sha256"] for s in statement.get("subject", [])}
    predicate = statement.get("predicate") or {}
    nodes = predicate.get("@graph") or []
    # Chain-verify the exported validator set: every validator descends from a
    # single root of trust. The bundle must then be signed BY that root, so the
    # public key the caller pins is the same key every per-claim asserter chains
    # to — the whole bundle anchors to one key the caller chose to trust, not to
    # the exporter as a separate party. This is the precondition every check
    # below rests on: a bundle either carries a single-rooted, chain-verified
    # validator set or it is refused here, so nothing downstream has to ask
    # whether the bundle is anchored. A signed graph cannot be stripped to a
    # digest-only bundle that skips per-claim verification.
    verified_validators, validator_types, trust_root = _verify_exported_validators(
        predicate.get("mare:validators") or []
    )
    if keyid != trust_root:
        raise BundleVerificationError(
            f"bundle:signed by {keyid[:12]}… but the validators chain to root "
            f"{str(trust_root)[:12]}…; a bundle must be signed by its root"
        )
    # Map each support value to the distinct chain-verified asserters that carry
    # a claim supporting it. A REPLICATED display is checked against this for
    # the distinct-signer corroboration that support level requires. Necessary
    # condition, so a genuine REPLICATED never false-rejects; it forbids a lone
    # claim from displaying REPLICATED with no independent corroborator.
    support_asserters: dict[str, set] = {}
    for n in nodes:
        if not n.get("@id", "").startswith("mare:claim/"):
            continue
        n_sig = n.get("signatureBundle")
        if not n_sig:
            continue
        try:
            n_asserter = n_sig["signatures"][0]["keyid"]
        except (KeyError, IndexError, TypeError):
            continue
        if n_asserter not in verified_validators:
            continue
        for sup in _string_supports(n.get("supports")):
            support_asserters.setdefault(sup, set()).add(n_asserter)
    for node in nodes:
        node_id = node.get("@id", "")
        if not node_id.startswith("mare:claim/"):
            continue
        claim_id = node_id[len("mare:claim/"):]
        subject_name = f"{SUBJECT_PREFIX}{claim_id}"
        if subject_name not in subjects:
            raise BundleVerificationError(
                f"statement:subject missing for claim {claim_id!r}"
            )
        # Per-claim asserter signature. The digest check above is
        # self-referential to the exporter key; this proves each claim was
        # signed by its stated asserter. In a signed graph every claim must
        # carry its bundle, so a stripped signature cannot hide an unsigned row.
        sig_bundle = node.get("signatureBundle")
        if sig_bundle is None:
            raise BundleVerificationError(
                f"claim:{claim_id} carries no signature bundle but the "
                "graph is signed"
            )
        try:
            asserter_keyid = sig_bundle["signatures"][0]["keyid"]
        except (KeyError, IndexError, TypeError) as exc:
            raise BundleVerificationError(
                f"claim:{claim_id} signature bundle is malformed"
            ) from exc
        asserter_pub = verified_validators.get(asserter_keyid)
        if asserter_pub is None:
            raise BundleVerificationError(
                f"claim:{claim_id} asserter {str(asserter_keyid)[:12]}… is "
                "not a chain-verified validator in the bundle"
            )
        try:
            sig_ok = _signing.verify_envelope(sig_bundle, asserter_pub)
        except _signing.InvalidEnvelopeError as exc:
            raise BundleVerificationError(
                f"claim:{claim_id} signature bundle is structurally "
                f"invalid: {exc}"
            ) from exc
        if not sig_ok:
            raise BundleVerificationError(
                f"claim:{claim_id} asserter signature failed verification"
            )
        try:
            bound = _signing.claim_predicate_from_envelope(sig_bundle)
        except _signing.InvalidEnvelopeError as exc:
            raise BundleVerificationError(
                f"claim:{claim_id} signature payload is unparseable: {exc}"
            ) from exc
        if bound.get("claim_id") != claim_id:
            raise BundleVerificationError(
                f"claim:{claim_id} signature binds a different claim_id "
                f"({bound.get('claim_id')!r})"
            )
        # Bind the asserter signature to the CONTENT, not just the claim_id:
        # re-derive the subject digest from the validator-signed fields and
        # require it equals the bundle's subject digest. Without this the
        # asserter signs only an id while the exporter alone vouches for the
        # displayed text/evidence — the signature would be decorative.
        asserter_digest = hashlib.sha256(
            _signing.canonical_statement({
                "claim_id": claim_id,
                "text": bound.get("text"),
                "classification": bound.get("classification"),
                "generated_by": bound.get("generated_by"),
                "supports": bound.get("supports") or [],
                "contradicts": bound.get("contradicts") or [],
                "source_name": bound.get("source_name"),
                "artifact_hash": bound.get("artifact_hash"),
                "created_at": bound.get("created_at"),
            }, bound.get("evidence") or {})
        ).hexdigest()
        if asserter_digest != subjects.get(subject_name):
            raise BundleVerificationError(
                f"claim:{claim_id} asserter signature does not cover the "
                "presented content — text or evidence differs from what "
                "was signed"
            )
        # Support level: verify the DISPLAYED level is backed by signed
        # material, so the exporter cannot inflate it. ESTABLISHED needs a
        # validator-signed validation envelope for this claim; REPLICATED needs
        # distinct-signer corroboration on a shared upstream. Editorial status
        # (retracted/contested) and comparison summaries are NOT attested here —
        # they carry no signature in the data model (see the module docstring).
        level = node.get("supportLevel", "PRELIMINARY")
        if level == "ESTABLISHED":
            _verify_established_level(
                node, claim_id, verified_validators, validator_types, _signing,
            )
        elif level == "REPLICATED":
            corroborated = any(
                len(support_asserters.get(sup, set())) >= 2
                for sup in _string_supports(node.get("supports"))
            )
            if not corroborated:
                raise BundleVerificationError(
                    f"claim:{claim_id} is shown REPLICATED but no shared "
                    "upstream carries a second distinct-signer claim in the "
                    "bundle"
                )
        # Re-derive the canonical Statement v1 hash from the @graph
        # node. evidence is part of the signed predicate, so the
        # JSON-LD node carries it and verify uses the same shape that
        # the build path used.
        chain_input = _signing.canonical_statement({
            "claim_id": claim_id,
            "text": node.get("claimText", ""),
            "classification": node.get("classification", "INFERRED"),
            "generated_by": node.get("generatedBy", "agent"),
            "supports": node.get("supports", []),
            "contradicts": node.get("contradicts", []),
            "source_name": node.get("sourceName"),
            "artifact_hash": node.get("artifactHash"),
            "created_at": node.get("dateCreated", ""),
        }, node.get("evidence") or {})
        expected = hashlib.sha256(chain_input).hexdigest()
        if subjects[subject_name] != expected:
            raise BundleVerificationError(
                f"statement:subject digest mismatch for {claim_id!r} — "
                "bundle contents have been tampered"
            )
    return statement
