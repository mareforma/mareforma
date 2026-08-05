"""Tests for the public :mod:`mareforma.canonicalize` registry.

Distinct from :mod:`mareforma._canonical` (the internal envelope
canonicalizer): the public registry lets adapters register specialty
forms (SMILES, FASTA, PDB) that downstream replay can pick by name.

Conceptual clusters:

- :class:`TestDefaultCanonicalizer` — ``json-c14n-v1`` behaviour and
  non-finite rejection.
- :class:`TestRegistry` — register / lookup / errors / cleanup.
- :class:`TestUtilities` — ``digest_bytes`` + ``fingerprint_tool_config``.
- :class:`TestSpecialtyForms` — RDKit / FASTA / PDB canonicalizers.
- :class:`TestSpecialtyAutoImport` — docstring contract: importing the
  parent package registers the specialty forms.
- :class:`TestDsseJcsNfcV1` — the DSSE envelope canonicaliser
  exposed under a registered name, NFC normalising.
"""

from __future__ import annotations

import math

import pytest

from mareforma.canonicalize import (
    CanonicalizationError,
    DEFAULT_CANONICALIZER,
    DSSE_JCS_NFC_V1,
    canonicalize,
    canonicalize_default,
    digest_bytes,
    fingerprint_tool_config,
    register_canonicalizer,
    registered_canonicalizers,
)


class TestDefaultCanonicalizer:
    def test_default_canonicalizer_registered(self):
        assert DEFAULT_CANONICALIZER in registered_canonicalizers()

    def test_byte_stable_under_key_reorder(self):
        a = canonicalize({"x": 1, "y": 2})
        b = canonicalize({"y": 2, "x": 1})
        assert a == b

    def test_rejects_nan(self):
        with pytest.raises(CanonicalizationError):
            canonicalize_default(float("nan"))

    def test_rejects_inf_in_nested(self):
        with pytest.raises(CanonicalizationError):
            canonicalize_default({"a": [1, math.inf]})


class TestRegistry:
    def test_unknown_form_raises(self):
        with pytest.raises(CanonicalizationError) as ei:
            canonicalize({"x": 1}, form="no-such-form-v1")
        assert "no-such-form-v1" in str(ei.value)

    def test_register_invalid_name(self):
        with pytest.raises(ValueError):
            register_canonicalizer("", lambda v: b"")
        with pytest.raises(ValueError):
            register_canonicalizer("bad name with spaces", lambda v: b"")

    def test_register_rejects_non_ascii_name(self):
        """The name is persisted in ``result_canonical_form`` and read by
        verifiers that may not be Python, so a Cyrillic homoglyph of a real
        form name must not be registrable."""
        for name in ("формa-v1", "rdkit-cаnonical-smiles-v1", "half-½"):
            with pytest.raises(ValueError):
                register_canonicalizer(name, lambda v: b"")

    def test_register_accepts_ascii_name(self):
        register_canonicalizer("fasta_nfc-probe-v1", lambda v: b"ok")
        try:
            assert canonicalize("x", form="fasta_nfc-probe-v1") == b"ok"
        finally:
            from mareforma.canonicalize import _REGISTRY
            _REGISTRY.pop("fasta_nfc-probe-v1", None)

    def test_reregistering_a_name_is_refused(self):
        """A form name is persisted in ``result_canonical_form`` and
        resolved again at replay, so redefining one silently would make
        the same name mean different bytes in two processes."""
        with pytest.raises(ValueError) as ei:
            register_canonicalizer(DEFAULT_CANONICALIZER, lambda v: b"HIJACKED")
        assert DEFAULT_CANONICALIZER in str(ei.value)
        assert canonicalize({"x": 1}) == b'{"x":1}'

    def test_reregistering_a_name_needs_override(self):
        register_canonicalizer("upper-bytes-v1", lambda v: v.upper().encode())
        try:
            register_canonicalizer(
                "upper-bytes-v1", lambda v: v.lower().encode(), override=True,
            )
            assert canonicalize("AbC", form="upper-bytes-v1") == b"abc"
        finally:
            from mareforma.canonicalize import _REGISTRY
            _REGISTRY.pop("upper-bytes-v1", None)

    def test_register_and_apply_custom_form(self):
        register_canonicalizer("upper-bytes-v1", lambda v: v.upper().encode())
        try:
            assert canonicalize("abc", form="upper-bytes-v1") == b"ABC"
        finally:
            # Module-level registry is shared — clean up to prevent
            # leakage into other tests in the same session.
            from mareforma.canonicalize import _REGISTRY
            _REGISTRY.pop("upper-bytes-v1", None)


class TestUtilities:
    def test_digest_bytes_shape(self):
        h = digest_bytes(b"abc")
        assert len(h) == 64
        assert int(h, 16) >= 0

    def test_fingerprint_tool_config_shape(self):
        fp = fingerprint_tool_config({"model": "x", "temperature": 0.7})
        assert fp.startswith("sha256:")
        assert len(fp) == len("sha256:") + 64


class TestSpecialtyForms:
    def test_specialty_registration_on_import(self):
        """Importing the specialty module must register all three forms."""
        import mareforma.canonicalize.specialty  # noqa: F401
        names = registered_canonicalizers()
        assert "rdkit-canonical-smiles-v1" in names
        assert "fasta-nfc-v1" in names
        assert "pdb-atom-sorted-v1" in names

    def test_fasta_canonicalizer_normalizes(self):
        from mareforma.canonicalize.specialty import canonicalize_fasta_nfc_v1
        assert canonicalize_fasta_nfc_v1("  acgtACGT  \n") == b"ACGTACGT"

    def test_fasta_v2_absorbs_wrap_and_line_endings(self):
        """Column wrap and CRLF carry no sequence semantics, so three copies
        of one record must canonicalize to the same bytes."""
        from mareforma.canonicalize.specialty import canonicalize_fasta_nfc_v2
        seq = "ACGT" * 35
        at60 = ">seq1\n" + "\n".join(
            seq[i:i + 60] for i in range(0, len(seq), 60)
        ) + "\n"
        at70 = ">seq1\n" + "\n".join(
            seq[i:i + 70] for i in range(0, len(seq), 70)
        ) + "\n"
        unwrapped = f">seq1\n{seq}\n"
        assert canonicalize_fasta_nfc_v2(at60) == canonicalize_fasta_nfc_v2(at70)
        assert canonicalize_fasta_nfc_v2(at60) == canonicalize_fasta_nfc_v2(unwrapped)
        assert canonicalize_fasta_nfc_v2(unwrapped) == f">seq1\n{seq}\n".encode()
        assert (
            canonicalize_fasta_nfc_v2(at60.replace("\n", "\r\n"))
            == canonicalize_fasta_nfc_v2(at60)
        )

    def test_fasta_v2_keeps_accession_case(self):
        """Sequence letters are case-insensitive, accessions are not."""
        from mareforma.canonicalize.specialty import canonicalize_fasta_nfc_v2
        assert canonicalize_fasta_nfc_v2(">Seq1\nacgt\n") == b">Seq1\nACGT\n"
        assert canonicalize_fasta_nfc_v2(">seq1\nACGT\n") != b">Seq1\nACGT\n"

    def test_fasta_v1_bytes_unchanged(self):
        """v1 digests are already recorded against these bytes."""
        from mareforma.canonicalize.specialty import canonicalize_fasta_nfc_v1
        assert canonicalize_fasta_nfc_v1(">seq1\nACGT\nACGT\n") == (
            b">SEQ1\nACGT\nACGT"
        )

    def test_pdb_canonicalizer_sorts_atom_block(self):
        from mareforma.canonicalize.specialty import canonicalize_pdb_atom_sorted_v1
        pdb = (
            "HEADER test\n"
            "ATOM      2  CA  ALA A   1\n"
            "ATOM      1  N   ALA A   1\n"
            "END\n"
        )
        out = canonicalize_pdb_atom_sorted_v1(pdb).decode()
        lines = out.strip().split("\n")
        assert lines[0] == "HEADER test"
        assert "ATOM      1" in lines[1]
        assert "ATOM      2" in lines[2]
        assert lines[3] == "END"

    def test_pdb_v1_bytes_unchanged(self):
        """v1 digests are already recorded against these bytes: a serial
        it cannot read sorts as 0 and the line is kept."""
        from mareforma.canonicalize.specialty import canonicalize_pdb_atom_sorted_v1
        overflow = (
            "ATOM  ***** N   GLY A   1\n"
            "ATOM      1  N   GLY A   2\n"
        )
        assert canonicalize_pdb_atom_sorted_v1(overflow) == overflow.encode()
        hybrid36 = (
            "ATOM  A0000  CA  ALA A   2\n"
            "ATOM  99999  N   ALA A   1\n"
        )
        assert canonicalize_pdb_atom_sorted_v1(hybrid36) == hybrid36.encode()

    def test_pdb_v2_reads_hybrid36_serials(self):
        """Serials past 99999 are hybrid-36 and must sort after the decimals."""
        from mareforma.canonicalize.specialty import canonicalize_pdb_atom_sorted_v2
        atoms = [
            "ATOM  99998  CA  ALA A   1",
            "ATOM  99999  N   ALA A   1",
            "ATOM  A0000  CA  ALA A   2",
            "ATOM  A0001  N   ALA A   2",
        ]
        pdb = "".join(line + "\n" for line in atoms)
        out = canonicalize_pdb_atom_sorted_v2(pdb).decode()
        assert out.strip().split("\n") == atoms
        shuffled = "".join(
            line + "\n" for line in [atoms[2], atoms[0], atoms[3], atoms[1]]
        )
        assert canonicalize_pdb_atom_sorted_v2(shuffled) == out.encode()

    def test_pdb_v2_refuses_unreadable_serial(self):
        """An overflow marker is not a serial; refuse instead of sorting it first."""
        from mareforma.canonicalize.specialty import canonicalize_pdb_atom_sorted_v2
        pdb = "ATOM  ***** CA  ALA A   1\nATOM      1  N   ALA A   1\n"
        with pytest.raises(ValueError, match=r"atom serial"):
            canonicalize_pdb_atom_sorted_v2(pdb)

    def test_rdkit_form_refuses_without_rdkit(self):
        """Without rdkit the form refuses instead of running another
        algorithm: one form name must mean one set of bytes on every host."""
        from mareforma.canonicalize.specialty import (
            HAS_RDKIT,
            canonicalize_rdkit_canonical_smiles_v1,
        )
        if HAS_RDKIT:
            pytest.skip("rdkit installed")
        with pytest.raises(CanonicalizationError, match="requires rdkit"):
            canonicalize_rdkit_canonical_smiles_v1("C(C)O")

    def test_rdkit_form_canonicalizes_with_rdkit(self):
        from mareforma.canonicalize.specialty import (
            HAS_RDKIT,
            canonicalize_rdkit_canonical_smiles_v1,
        )
        if not HAS_RDKIT:
            pytest.skip("rdkit not installed")
        assert canonicalize_rdkit_canonical_smiles_v1("C(C)O") == b"CCO"

    def test_smiles_nfc_fallback_form(self):
        """The degraded string form is asked for by name, so the persisted
        result_canonical_form says which function produced the bytes."""
        assert canonicalize(
            "  C(C)O  ", form="smiles-nfc-fallback-v1",
        ) == b"C(C)O"


class TestSpecialtyAutoImport:
    def test_auto_imported_on_package_import(self):
        """Importing mareforma.canonicalize registers the specialty forms.

        The static import in __init__.py drags the specialty submodule in, so
        its forms are registered without the caller discovering the import. We
        assert the live registration directly: reading the source text instead
        would still pass even if the import line were unreachable.
        """
        import mareforma.canonicalize as can
        names = set(can.registered_canonicalizers())
        assert "rdkit-canonical-smiles-v1" in names
        assert "fasta-nfc-v1" in names
        assert "pdb-atom-sorted-v1" in names


class TestDsseJcsNfcV1:
    def test_registered(self):
        """The DSSE envelope canonicaliser is exposed under a registered form."""
        assert DSSE_JCS_NFC_V1 in registered_canonicalizers()
        # And it produces the same bytes as the private envelope canonicaliser.
        from mareforma._canonical import canonicalize as envelope
        payload = {"x": 1, "name": "café"}
        assert canonicalize(payload, form=DSSE_JCS_NFC_V1) == envelope(payload)

    def test_nfc_normalises(self):
        """dsse-jcs-nfc-v1 collapses NFC vs NFD into the same bytes."""
        # 'é' as precomposed U+00E9 vs decomposed e+U+0301.
        a = canonicalize({"k": "café"}, form=DSSE_JCS_NFC_V1)
        b = canonicalize({"k": "café"}, form=DSSE_JCS_NFC_V1)
        assert a == b
