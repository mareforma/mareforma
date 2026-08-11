"""Specialty canonicalizers: domain-specific byte-stable forms.

Importing this module registers ``rdkit-canonical-smiles-v1``,
``smiles-nfc-fallback-v1``, ``fasta-nfc-v1``, ``fasta-nfc-v2``,
``pdb-atom-sorted-v1`` and ``pdb-atom-sorted-v2`` with the central
canonicalize registry. A claim that records its canonical form via
``result_canonical_form`` can then be re-canonicalised at replay time
by the same name.

New FASTA callers want ``fasta-nfc-v2``: ``fasta-nfc-v1`` keeps
internal line breaks, so a re-wrapped copy of one sequence digests
differently. New PDB callers want ``pdb-atom-sorted-v2``, which reads
the hybrid-36 serials of a structure past 99999 atoms that v1 sorts as
0. Both v1 forms stay registered because their bytes are already
recorded in claims.

``smiles-nfc-fallback-v1`` is the string form for hosts without
``rdkit`` (install the ``chem`` extra to get it). It is byte-stable but
NOT chemically canonical: tautomers and equivalent atom orderings of
the same molecule produce different bytes. It is a separate form name
because a name has to determine the bytes on every host.
"""

from __future__ import annotations

import unicodedata

from mareforma.canonicalize import CanonicalizationError, register_canonicalizer


__all__ = [
    "HAS_RDKIT",
    "canonicalize_fasta_nfc_v1",
    "canonicalize_fasta_nfc_v2",
    "canonicalize_pdb_atom_sorted_v1",
    "canonicalize_pdb_atom_sorted_v2",
    "canonicalize_rdkit_canonical_smiles_v1",
    "canonicalize_smiles_nfc_fallback_v1",
    "rdkit_fallback_used",
]


try:  # pragma: no cover, environment-dependent
    from rdkit import Chem  # type: ignore[import-not-found]
    HAS_RDKIT = True
except ImportError:
    HAS_RDKIT = False


def rdkit_fallback_used() -> bool:
    """Return True iff ``rdkit`` is absent, so the RDKit form refuses."""
    return not HAS_RDKIT


def canonicalize_rdkit_canonical_smiles_v1(value: str) -> bytes:
    """Canonical SMILES via RDKit. Refuses when rdkit is unavailable."""
    if not isinstance(value, str):
        raise TypeError("SMILES canonicaliser expects a string")
    if not HAS_RDKIT:
        raise CanonicalizationError(
            "rdkit-canonical-smiles-v1 requires rdkit; install it "
            "(mareforma[chem]) or use smiles-nfc-fallback-v1"
        )
    mol = Chem.MolFromSmiles(value)  # pragma: no cover, needs rdkit
    if mol is None:
        raise ValueError(f"rdkit could not parse SMILES {value!r}")
    return Chem.MolToSmiles(mol, canonical=True).encode("utf-8")


def canonicalize_smiles_nfc_fallback_v1(value: str) -> bytes:
    """NFC + strip of the SMILES string, UTF-8 bytes.

    Byte-stable but not chemically canonical: two spellings of the same
    molecule stay different bytes.
    """
    if not isinstance(value, str):
        raise TypeError("SMILES canonicaliser expects a string")
    return unicodedata.normalize("NFC", value).strip().encode("utf-8")


def canonicalize_fasta_nfc_v1(value: str) -> bytes:
    """NFC + uppercase + strip trailing whitespace, UTF-8 bytes.

    Keeps internal line breaks, so a re-wrapped or CRLF copy of one
    record gives different bytes. Superseded by
    :func:`canonicalize_fasta_nfc_v2`; kept because digests are already
    recorded against these bytes.
    """
    if not isinstance(value, str):
        raise TypeError("FASTA canonicaliser expects a string")
    return unicodedata.normalize("NFC", value).upper().strip().encode("utf-8")


def canonicalize_fasta_nfc_v2(value: str) -> bytes:
    """NFC, sequence lines unwrapped and uppercased, ``\\n`` endings.

    Column wrap and CRLF/CR/LF carry no sequence semantics, so every
    copy of one record canonicalises to the same bytes. Header lines
    (``>`` and ``;``) keep their case, so two accessions differing only
    in case stay distinct.
    """
    if not isinstance(value, str):
        raise TypeError("FASTA canonicaliser expects a string")
    result: list[str] = []
    sequence: list[str] = []

    def _flush_sequence() -> None:
        if sequence:
            result.append("".join(sequence))
            sequence.clear()

    for line in unicodedata.normalize("NFC", value).splitlines():
        if line.startswith((">", ";")):
            _flush_sequence()
            result.append(line.strip())
        else:
            residues = "".join(line.split()).upper()
            if residues:
                sequence.append(residues)
    _flush_sequence()
    return ("\n".join(result) + "\n").encode("utf-8")


_SERIAL_WIDTH = 5
_HY36_DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_HY36_UPPER_OFFSET = 10 ** _SERIAL_WIDTH - 10 * 36 ** (_SERIAL_WIDTH - 1)
_HY36_LOWER_OFFSET = 10 ** _SERIAL_WIDTH + 16 * 36 ** (_SERIAL_WIDTH - 1)


def _decode_atom_serial(field: str) -> int:
    """Decode a PDB serial field, decimal or hybrid-36.

    Files past 99999 atoms encode the serial in hybrid-36: the uppercase
    run starts at ``A0000`` (100000) and the lowercase run continues
    above it. Raises ``ValueError`` on anything else.
    """
    text = field.strip()
    try:
        return int(text)
    except ValueError:
        pass
    if (
        len(text) == _SERIAL_WIDTH
        and text.isascii()
        and text[0].isalpha()
        and (text.isupper() or text.islower())
        and all(char in _HY36_DIGITS for char in text.upper())
    ):
        offset = _HY36_UPPER_OFFSET if text.isupper() else _HY36_LOWER_OFFSET
        return int(text.upper(), 36) + offset
    raise ValueError(f"unreadable PDB atom serial {field!r}")


def _sort_atom_blocks(value: str, serial_of) -> bytes:
    """Sort each ATOM/HETATM block by ``serial_of(field)``; keep the rest.

    Shared by both PDB forms so they differ only in how a serial field is
    read, which is the whole difference between the two names.
    """
    if not isinstance(value, str):
        raise TypeError("PDB canonicaliser expects a string")
    lines = value.splitlines()
    result: list[str] = []
    atom_block: list[tuple[int, int, str]] = []

    def _flush_atom_block() -> None:
        atom_block.sort(key=lambda t: (t[0], t[1]))
        result.extend(line for _, _, line in atom_block)
        atom_block.clear()

    for idx, line in enumerate(lines):
        if line.startswith(("ATOM", "HETATM")):
            try:
                serial = serial_of(line[6:11])
            except ValueError as exc:
                raise ValueError(f"{exc} on line {idx + 1}: {line!r}") from None
            atom_block.append((serial, idx, line))
        else:
            _flush_atom_block()
            result.append(line)
    _flush_atom_block()
    return ("\n".join(result) + "\n").encode("utf-8")


def _decimal_atom_serial(field: str) -> int:
    """Decode a decimal PDB serial field; anything else sorts as 0."""
    try:
        return int(field.strip())
    except ValueError:
        return 0


def canonicalize_pdb_atom_sorted_v1(value: str) -> bytes:
    """Sort each ATOM/HETATM block by decimal serial; preserve other lines.

    PDB serial numbers occupy columns 7-11 (0-indexed slice ``[6:11]``).
    Lines that aren't ATOM/HETATM (HEADER, REMARK, SEQRES, ENDMDL, …)
    keep their relative position. Ties within a block break on input
    order (stable sort).

    A serial this form cannot read as decimal sorts as 0, so the
    hybrid-36 serials a file past 99999 atoms carries all collapse to
    the front of their block. New callers want
    :func:`canonicalize_pdb_atom_sorted_v2`; v1 stays because its bytes
    are already recorded in claims.
    """
    return _sort_atom_blocks(value, _decimal_atom_serial)


def canonicalize_pdb_atom_sorted_v2(value: str) -> bytes:
    """Sort each ATOM/HETATM block by decimal or hybrid-36 serial.

    Same shape as :func:`canonicalize_pdb_atom_sorted_v1`, reading the
    serials past 99999 that v1 collapses to 0. An ATOM/HETATM line whose
    serial is neither decimal nor hybrid-36 raises ``ValueError`` naming
    the line. That includes the ``*****`` overflow marker a writer emits
    past 99999 atoms without hybrid-36: the marker carries no order, and
    a guessed serial would reorder the file.
    """
    return _sort_atom_blocks(value, _decode_atom_serial)


register_canonicalizer(
    "rdkit-canonical-smiles-v1", canonicalize_rdkit_canonical_smiles_v1,
)
register_canonicalizer(
    "smiles-nfc-fallback-v1", canonicalize_smiles_nfc_fallback_v1,
)
register_canonicalizer("fasta-nfc-v1", canonicalize_fasta_nfc_v1)
register_canonicalizer("fasta-nfc-v2", canonicalize_fasta_nfc_v2)
register_canonicalizer("pdb-atom-sorted-v1", canonicalize_pdb_atom_sorted_v1)
register_canonicalizer("pdb-atom-sorted-v2", canonicalize_pdb_atom_sorted_v2)
