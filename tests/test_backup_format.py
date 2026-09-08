"""The format stamp, and what it lets a reader ask of a backup.

The completeness table is the backup's account of itself, and a reader can only
hold a file to that account if the file says one is owed. Without the stamp it
cannot, and the gap is not an edge case: a backup of a healthy project carries
no verdict chain, no attestations and no census, because each is written only
when it has rows. Delete the table from such a file and what remains has the
same sections and the same keys as a backup written before any of it existed.

The stamp is a top-level key, written first, and that placement is load-bearing
rather than tidy. Inside a section it sat seventeen bytes above the table, so
nearly every cut that took the table took it too.

It is a claim the file makes about itself and nothing signs it, so an editor who
removes it along with the table puts the file back where it was. What it closes
is the edit made in one place. The tests below pin both halves.
"""

from __future__ import annotations

import re
import shutil
import warnings
from pathlib import Path

import pytest

import mareforma
from mareforma import restore
from mareforma.db.core import (
    _BACKUP_FORMAT, tables_below_completeness, verify_completeness_digest,
)
from tests._helpers import _bootstrap_key, _enroll_key

try:
    import tomllib          # 3.11+ stdlib
except ModuleNotFoundError:  # 3.10, where it is the tomli backport
    import tomli as tomllib  # type: ignore[no-redef]


def _healthy_project(root: Path) -> Path:
    """The modal project: claims, one signer, no verdict, no guard ever gone.

    Deliberately the plainest graph there is. A fixture carrying a verdict or a
    census entry writes sections that survive a deleted table and would let
    these tests pass on the strength of those instead of the stamp.
    """
    key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=key) as graph:
        for name in ("first", "second", "third"):
            graph.assert_claim(f"the {name} finding", generated_by=name)
    return root / "claims.toml"


def _project_with_a_verdict(root: Path) -> Path:
    """A graph whose backup carries a verdict chain.

    The healthy fixture has none of the sections this format added, which is
    what makes the stamp the only thing that can speak for a deleted table
    there. This one has one, so the other arm gets exercised too: a section
    written ahead of the table, surviving without it.
    """
    root_key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=root_key) as graph:
        older = graph.assert_claim("the older claim", generated_by="run1")
        newer = graph.assert_claim("the newer claim", generated_by="run2")
    witness = _bootstrap_key(root, "witness.key")
    _enroll_key(root, root_key, witness, identity="witness@example.org")
    with mareforma.open(root, key_path=witness) as graph:
        graph.record_contradiction_verdict(
            verdict_id="v1", member_claim_id=newer, other_claim_id=older,
        )
    return root / "claims.toml"


def _restore_into(source: Path, target: Path) -> tuple[dict, list[str]]:
    """Restore *source* byte for byte and return the report and what was said.

    Copied rather than re-serialized: re-serializing changes the body bytes and
    breaks the digest on its own, which would make every case here look caught
    for the wrong reason.
    """
    target.mkdir(parents=True, exist_ok=True)
    shutil.copy(source, target / "claims.toml")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        report = restore(target)
    return report, [
        str(w.message) for w in caught if "claims.toml at" in str(w.message)
    ]


def _restamped(raw: str, value: int) -> str:
    """*raw* with a different format stamp and a digest that still reproduces.

    The stamp sits inside the bytes the digest covers, so changing it breaks
    the digest by construction. A file genuinely written by a later format
    carries a digest over its own body, and a test that skips this step ends up
    asserting about a corrupt file rather than a newer one.
    """
    import hashlib

    body, sep, tail = raw.partition("\n[completeness]\n")
    body = body.replace(
        f"backup_format = {_BACKUP_FORMAT}\n", f"backup_format = {value}\n", 1,
    )
    digest = hashlib.sha256((body + "\n").encode("utf-8")).hexdigest()
    tail = re.sub(r'digest = "[0-9a-f]+"', f'digest = "{digest}"', tail, count=1)
    return body + sep + tail


def _reasons_for(path: Path) -> tuple[str, ...]:
    """The stable reasons the disclosure returns for *path*.

    Module level because several tests below assert on the reason rather than
    on something merely having been said. A test that only checks a warning
    appeared passes whichever branch produced it, which is how a check can be
    removed with every test staying green.
    """
    from mareforma.db.restore import _disclose_a_file_that_disagrees_with_itself

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return _disclose_a_file_that_disagrees_with_itself(
            path, tomllib.loads(path.read_text()),
        )


def _chain_reasons_for(target: Path) -> tuple[str, ...]:
    """The reasons the chain disclosure returns for a restored project.

    A sibling of `_reasons_for`, and here for the same reason: the two tests
    below distinguished their cases by matching sentences out of the warning,
    which is the practice the disclosure's own docstring condemns. Collapsing
    the two reasons into one left every test green.
    """
    import sqlite3

    from mareforma.db.restore import _disclose_a_rotated_copy_worth_trying

    conn = sqlite3.connect(target / ".mareforma" / "graph.db")
    conn.row_factory = sqlite3.Row
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return _disclose_a_rotated_copy_worth_trying(
                conn, target / "claims.toml",
            )
    finally:
        conn.close()


class TestTheStampIsWritten:

    def test_a_healthy_backup_carries_it(self, tmp_path: Path) -> None:
        """Unconditional, because a stamp that is sometimes absent says nothing
        when it is absent."""
        doc = tomllib.loads(_healthy_project(tmp_path).read_text())
        assert doc["backup_format"] == _BACKUP_FORMAT

    def test_it_is_the_first_thing_in_the_file(self, tmp_path: Path) -> None:
        """The placement is the whole of its truncation value.

        A stamp below the data goes with the bytes a cut takes. On line one it
        survives every cut that leaves a parseable file. Asserted against the
        first section rather than against the table, so moving the stamp
        anywhere below the data is a red test even if it stays above the table.
        """
        raw = _healthy_project(tmp_path).read_text()
        assert raw.startswith(f"backup_format = {_BACKUP_FORMAT}\n")
        assert raw.index("backup_format") < raw.index("[validators")

    def test_the_counter_section_is_left_as_it_was(self, tmp_path: Path) -> None:
        """The stamp went to the top rather than into ``[graph_meta]``.

        Held to the exact set on purpose. Releases that predate the stamp read
        that section for one named key, so anything landing in it needs the
        compatibility question asked, and an equality check is what makes
        someone ask.
        """
        doc = tomllib.loads(_healthy_project(tmp_path).read_text())
        assert set(doc["graph_meta"]) == {"supports_revision"}

    def test_it_is_not_counted_as_a_section(self, tmp_path: Path) -> None:
        """A top-level scalar is not a table, so the row counts are unmoved by
        it and a reader comparing them against an older file still agrees."""
        doc = tomllib.loads(_healthy_project(tmp_path).read_text())
        assert "backup_format" not in doc["completeness"]["sections"]


class TestWhatItCatches:

    def test_a_deleted_completeness_table_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """The case that was silent before the stamp, on the graph shape that
        makes it silent: nothing else in the file survives to report it."""
        source = _healthy_project(tmp_path)
        raw = source.read_text()
        stripped = tmp_path / "stripped.toml"
        stripped.write_text(raw[: raw.rfind("\n[completeness]\n") + 1])

        # The same sections and the same claim count as an older backup, which
        # is the whole difficulty. Only the stamp separates them.
        assert set(tomllib.loads(stripped.read_text())) == {
            "backup_format", "validators", "claims", "graph_meta",
        }

        report, said = _restore_into(stripped, tmp_path / "recovered")
        assert report["claims_restored"] == 3
        assert said, "a deleted completeness table restored without a word"
        assert "completeness table" in said[0]

    def test_a_cut_that_takes_most_of_the_file_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """The case the stamp's placement is for.

        A cut mid-claims takes the table, the counts and the section that used
        to hold the stamp. On line one the stamp is still there, so the file
        still says what it owed, and the recovery says it cannot account for
        the file rather than proceeding in silence. It does not say how short
        the graph is, and cannot: the count that would have said went with the
        bytes.
        """
        source = _healthy_project(tmp_path)
        raw = source.read_text()
        short = tmp_path / "short.toml"
        short.write_text(raw[: raw.rindex("[claims.")])

        survivors = tomllib.loads(short.read_text())
        assert "completeness" not in survivors
        assert "graph_meta" not in survivors
        assert survivors["backup_format"] == _BACKUP_FORMAT

        report, said = _restore_into(short, tmp_path / "recovered")
        assert report["claims_restored"] == 2
        assert said, "a backup cut down to two claims restored without a word"

    def test_a_cut_landing_inside_the_table_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """A partial table keeps its header, so the stamp is not what answers
        here. The digest is.

        Asserted on the reason rather than on something having been said. A cut
        inside the table also takes the row counts with it, so this stayed green
        with the digest check removed entirely, reported by the other branch.
        That is the same thing as not testing the digest at all.
        """
        source = _healthy_project(tmp_path)
        head, _, table = source.read_text().partition("\n[completeness]\n")
        kept = "\n".join(table.splitlines()[:2])
        cut = tmp_path / "cut.toml"
        cut.write_text(f"{head}\n[completeness]\n{kept}\n")

        _, said = _restore_into(cut, tmp_path / "recovered")
        assert said, "a half-written completeness table restored without a word"
        assert "digest_mismatch" in _reasons_for(cut)

    def test_a_section_written_ahead_of_the_table_still_answers_for_it(
        self, tmp_path: Path,
    ) -> None:
        """The other arm, on a graph that has something in it.

        A backup carrying a verdict chain can report a deleted table without
        the stamp, because a release with no table to write had no chain to
        write either. The stamp extends that to a graph with nothing in it.
        Both arms are exercised, so removing either is a red test.
        """
        source = _project_with_a_verdict(tmp_path)
        doc = tomllib.loads(source.read_text())
        assert doc.get("verdict_chain"), "the fixture wrote no chain"
        doc.pop("completeness")
        doc.pop("backup_format", None)

        import tomli_w
        stripped = tmp_path / "stripped.toml"
        stripped.write_text(tomli_w.dumps(doc))

        _, said = _restore_into(stripped, tmp_path / "recovered")
        assert said, "a surviving chain did not answer for the deleted table"

    def test_a_section_appended_below_the_table_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """The digest covers the bytes ABOVE the table, so what sits below it
        is outside the digest, and the row counts walk only the names the table
        declares, so a section the file never had is outside those too.

        Measured before it was closed: a well-formed transparency-log entry put
        there restored into the graph and marked a real claim logged, with the
        digest still verifying and nothing said.
        """
        source = _healthy_project(tmp_path)
        raw = source.read_text()
        claim_id = sorted(tomllib.loads(raw)["claims"])[0]
        forged = tmp_path / "forged.toml"
        forged.write_text(
            raw
            + f'\n[rekor_inclusions."{claim_id}"]\n'
            'uuid = "an-entry-that-was-never-submitted"\n'
            'raw_response_b64 = "eyJhIjp7ImJvZHkiOiJ4In19"\n'
            "log_index = 99\n"
            "integrated_time = 1\n"
            'recorded_at = "2026-08-31T00:00:00+00:00"\n'
        )
        # The digest still reproduces, which is the point: it cannot reach here.
        assert verify_completeness_digest(forged)
        # Reported by table name. The row key is not surfaced: the tail is
        # parsed, and what a parse returns is the table, not the header text.
        assert tables_below_completeness(forged) == ("rekor_inclusions",)

        _, said = _restore_into(forged, tmp_path / "recovered")
        assert said, "a section bolted on below the table restored in silence"
        assert "rekor_inclusions" in said[0]
        assert "content_below_table" in _reasons_for(forged)

    def test_a_table_stripped_of_its_row_counts_is_reported(
        self, tmp_path: Path,
    ) -> None:
        """The counts are a sub-table written below the digest boundary, so
        they can be lifted out on their own while the digest still reproduces.

        Every file in this format has them, so their absence in a stamped file
        is the deleted table again, one level down.
        """
        source = _healthy_project(tmp_path)
        head, _, table = source.read_text().partition("\n[completeness]\n")
        without = tmp_path / "without.toml"
        without.write_text(
            f"{head}\n[completeness]\n{table.split('[completeness.sections]')[0]}"
        )
        assert verify_completeness_digest(without)
        assert "sections" not in tomllib.loads(without.read_text())["completeness"]

        _, said = _restore_into(without, tmp_path / "recovered")
        assert said, "a table with no row counts restored without a word"
        assert "row counts" in said[0]


class TestTheTailIsParsedRatherThanScanned:
    """A header is whatever TOML says it is, not whatever a line looks like."""

    @pytest.mark.parametrize(
        "header",
        [
            '[rekor_inclusions."{cid}"]',
            '[rekor_inclusions."{cid}"] # a note',
            '[rekor_inclusions."{cid}"]   ',
            '   [rekor_inclusions."{cid}"]',
            '# a leading comment\n[rekor_inclusions."{cid}"]',
        ],
        ids=["plain", "trailing-comment", "trailing-space", "indented",
             "comment-first"],
    )
    def test_every_shape_of_header_is_seen(
        self, tmp_path: Path, header: str,
    ) -> None:
        """The trailing-comment case is why this reads the tail with the parser.

        A line-shaped check that wanted a bracket at each end walked straight
        past one comment, and the same forged entry landed again in silence.
        Measured before and after.
        """
        source = _healthy_project(tmp_path)
        raw = source.read_text()
        claim_id = sorted(tomllib.loads(raw)["claims"])[0]
        forged = tmp_path / "forged.toml"
        forged.write_text(
            raw + "\n" + header.format(cid=claim_id) + "\n"
            'uuid = "an-entry-that-was-never-submitted"\n'
            'raw_response_b64 = "eyJhIjp7ImJvZHkiOiJ4In19"\n'
            "log_index = 99\nintegrated_time = 1\n"
            'recorded_at = "2026-08-31T00:00:00+00:00"\n'
        )
        assert verify_completeness_digest(forged)
        assert tables_below_completeness(forged) == ("rekor_inclusions",)
        assert "content_below_table" in _reasons_for(forged)


class TestAStampCannotBuySilence:
    """A file's claim about its own format never excuses what it is missing.

    The reason this class exists: putting ``completeness_absent`` in an else
    arm let one character reroute a truncated backup to the benign token, so a
    file that had just lost a signed verdict told the operator to read it with
    a newer release instead of to take the backup again. Measured, and worse
    than the silence it replaced, because silence does not misdirect.
    """

    def _truncated(self, tmp_path: Path, stamp: int) -> Path:
        home = tmp_path / f"src-{stamp}"
        home.mkdir()
        raw = _restamped(_project_with_a_verdict(home).read_text(), stamp)
        out = tmp_path / f"cut-{stamp}.toml"
        out.write_text(raw[: raw.rindex("[claims.")])
        return out

    def test_a_truncated_backup_reports_the_loss_whatever_it_claims(
        self, tmp_path: Path,
    ) -> None:
        honest = self._truncated(tmp_path, _BACKUP_FORMAT)
        bumped = self._truncated(tmp_path, _BACKUP_FORMAT + 1)

        assert "completeness_absent" in _reasons_for(honest)
        assert "completeness_absent" in _reasons_for(bumped), (
            "bumping the stamp dropped the reason the truncation earned"
        )

    def test_the_later_format_note_is_added_not_substituted(
        self, tmp_path: Path,
    ) -> None:
        bumped = self._truncated(tmp_path, _BACKUP_FORMAT + 1)
        assert set(_reasons_for(bumped)) == {"format_ahead", "completeness_absent"}

    def test_the_operator_is_still_told_to_take_the_backup_again(
        self, tmp_path: Path,
    ) -> None:
        """The half that made this worse than silence: the wording."""
        bumped = self._truncated(tmp_path, _BACKUP_FORMAT + 1)
        _, said = _restore_into(bumped, tmp_path / "recovered")
        assert said
        assert "Take the backup again" in said[0]


class TestLineEndingsDoNotHideAnything:

    def test_a_forgery_in_a_crlf_file_is_still_seen(
        self, tmp_path: Path,
    ) -> None:
        """A file through a Windows editor is an ordinary file, and a section
        appended to one is still a section.

        Looking for the boundary as a byte pattern that needed a bare newline
        found nothing in such a file, so it looked like it had no table below
        it and a forgery could sit there behind the digest complaint the
        conversion produced on its own. The honest half of this is asserted
        where the two readers are compared, not here.
        """
        source = _healthy_project(tmp_path)
        raw = source.read_text()
        claim_id = sorted(tomllib.loads(raw)["claims"])[0]
        forged = tmp_path / "crlf.toml"
        forged.write_bytes(
            (
                raw + f'\n[rekor_inclusions."{claim_id}"]\n'
                'uuid = "an-entry-that-was-never-submitted"\n'
                'raw_response_b64 = "eyJhIjp7ImJvZHkiOiJ4In19"\n'
                "log_index = 99\nintegrated_time = 1\n"
                'recorded_at = "2026-08-31T00:00:00+00:00"\n'
            ).replace("\n", "\r\n").encode("utf-8")
        )
        assert tables_below_completeness(forged) == ("rekor_inclusions",)


class TestTheFunctionSaysWhenItCannotTell:

    def test_an_unparseable_tail_is_not_reported_as_nothing_there(
        self, tmp_path: Path,
    ) -> None:
        """"Nothing below the table" and "I could not read below the table"
        are different answers, and one of them is dangerous to give."""
        source = _healthy_project(tmp_path)
        moved = tmp_path / "moved.toml"
        moved.write_text(
            source.read_text()
            + '\n[rekor_inclusions."x"]\n'
            + 'note = """\n[completeness]\nstill inside the string\n"""\n'
        )
        with pytest.raises(ValueError, match="not a table this format writes"):
            tables_below_completeness(moved)

    def test_restore_turns_that_into_its_own_reason(
        self, tmp_path: Path,
    ) -> None:
        source = _healthy_project(tmp_path)
        moved = tmp_path / "moved.toml"
        moved.write_text(
            source.read_text()
            + '\n[rekor_inclusions."x"]\n'
            + 'note = """\n[completeness]\nstill inside the string\n"""\n'
        )
        assert "tail_unparseable" in _reasons_for(moved)


class TestWhatItMustNotCatch:

    def test_an_untouched_backup_passes_without_a_word(
        self, tmp_path: Path,
    ) -> None:
        source = _healthy_project(tmp_path)
        report, said = _restore_into(source, tmp_path / "recovered")
        assert report["claims_restored"] == 3
        assert said == []

    def test_a_backup_written_before_the_stamp_passes_without_a_word(
        self, tmp_path: Path,
    ) -> None:
        """The shape every earlier release wrote: no table, no stamp.

        This is why the check keys on the stamp rather than on the table's
        absence. Keyed on absence it would fire on every backup anyone holds,
        and the recovery path is where a false alarm costs the most.
        """
        source = _healthy_project(tmp_path)
        doc = tomllib.loads(source.read_text())
        doc.pop("backup_format", None)
        doc.pop("completeness")

        import tomli_w
        legacy = tmp_path / "legacy.toml"
        legacy.write_text(tomli_w.dumps(doc))

        report, said = _restore_into(legacy, tmp_path / "recovered")
        assert report["claims_restored"] == 3
        assert said == [], "an older backup was reported as tampered with"


class TestAFormatThisReleaseDoesNotKnow:

    def test_a_later_stamp_is_read_even_when_the_table_is_there(
        self, tmp_path: Path,
    ) -> None:
        """The realistic later-format file, which keeps its table.

        A format after this one still writes a completeness table, so a check
        that only reads the stamp when the table is missing never meets the
        case it was written for. Measured before this was fixed: a file stamped
        ten, table present and digest reproducing, restored without a word.
        """
        source = _healthy_project(tmp_path)
        ahead = tmp_path / "ahead.toml"
        ahead.write_text(_restamped(source.read_text(), _BACKUP_FORMAT + 9))

        # A genuine later-format file carries a digest over its own body, so
        # this repairs it. Without that the file also reports digest_mismatch
        # and the case under test hides behind it.
        assert verify_completeness_digest(ahead)
        assert "completeness" in tomllib.loads(ahead.read_text())
        assert _reasons_for(ahead) == ("format_ahead",)
        _, said = _restore_into(ahead, tmp_path / "recovered")
        assert said, "a file from a later format restored without a word"
        assert "disagrees with itself" not in said[0]

    def test_a_later_stamp_is_not_called_tampering(self, tmp_path: Path) -> None:
        """A number above this release's own is not evidence of an edit.

        Saying it were would turn every honest file from a later format into an
        accusation, on a reader already shipped and unable to be corrected. So
        the reader says what is true: it cannot check what that file owes.
        """
        source = _healthy_project(tmp_path)
        doc = tomllib.loads(source.read_text())
        doc["backup_format"] = _BACKUP_FORMAT + 1
        doc.pop("completeness")

        import tomli_w
        ahead = tmp_path / "ahead.toml"
        ahead.write_text(tomli_w.dumps(doc))

        report, said = _restore_into(ahead, tmp_path / "recovered")
        assert report["claims_restored"] == 3
        assert said, "a file from an unknown format restored without a word"
        assert "later than the format" in said[0]
        # The missing table is still reported. Saying only "this is newer" about
        # a file that has lost its own account of itself is a misdirection, and
        # the reasons carry both so a caller can tell them apart.
        assert set(_reasons_for(ahead)) == {"format_ahead", "completeness_absent"}

    @pytest.mark.parametrize("value", [0, "1", 1.0, True, []])
    def test_a_stamp_of_any_other_shape_still_owes_a_table(
        self, tmp_path: Path, value: object,
    ) -> None:
        """The stamp is read off a file an attacker may have written, so every
        shape it can arrive in has to land somewhere named.

        Anything that is not a recognisable later version is held to the table,
        which is the fail-closed direction: a shape nobody writes does not buy
        silence.
        """
        source = _healthy_project(tmp_path)
        doc = tomllib.loads(source.read_text())
        doc["backup_format"] = value
        doc.pop("completeness")

        import tomli_w
        edited = tmp_path / "edited.toml"
        edited.write_text(tomli_w.dumps(doc))

        report, said = _restore_into(edited, tmp_path / "recovered")
        assert report["claims_restored"] == 3
        assert said, f"a stamp of {value!r} bought silence"


class TestTheResidual:

    def test_removing_the_stamp_with_the_table_is_not_detectable(
        self, tmp_path: Path,
    ) -> None:
        """Pinned so the guarantee is never read wider than it is.

        Nothing signs the stamp. An editor who takes it along with the table
        hands back a file shaped exactly like an older one, and no check inside
        a single file can say otherwise. Closing this needs a copy the editor
        does not hold.
        """
        source = _healthy_project(tmp_path)
        doc = tomllib.loads(source.read_text())
        doc.pop("backup_format", None)
        doc.pop("completeness")

        import tomli_w
        laundered = tmp_path / "laundered.toml"
        laundered.write_text(tomli_w.dumps(doc))

        report, said = _restore_into(laundered, tmp_path / "recovered")
        assert report["claims_restored"] == 3
        assert said == [], "the check claimed a reach it does not have"


class TestItSurvivesRecovery:

    def test_a_restored_graph_stamps_its_own_next_backup(
        self, tmp_path: Path,
    ) -> None:
        """Restore copies the file in and nothing rewrites it until a mutation
        does, so reading the file straight after a restore proves nothing about
        the writer. One claim makes the recovered graph write its own.
        """
        source = _healthy_project(tmp_path)
        target = tmp_path / "recovered"
        _restore_into(source, target)

        with mareforma.open(target, key_path=tmp_path / "root.key") as graph:
            assert len(graph.query()) == 3
            graph.assert_claim("a finding made after recovery",
                               generated_by="after")

        written = tomllib.loads((target / "claims.toml").read_text())
        assert written["backup_format"] == _BACKUP_FORMAT
        assert len(written["claims"]) == 4
        assert verify_completeness_digest(target / "claims.toml")


class TestTheReasonsAreTyped:
    """What a caller that refuses would have to select on.

    Not every complaint should be fatal. A hand-repaired file has to stay
    recoverable, and a file from a later format is this reader admitting a
    limit rather than the file admitting an edit. Selecting on the sentences
    is how that distinction gets lost, so the sentences go in the warning and
    the reasons come back.
    """

    def test_a_clean_file_says_nothing(self, tmp_path: Path) -> None:
        assert _reasons_for(_healthy_project(tmp_path)) == ()

    def test_each_thing_found_has_its_own_reason(self, tmp_path: Path) -> None:
        source = _healthy_project(tmp_path)
        raw = source.read_text()

        gone = tmp_path / "gone.toml"
        gone.write_text(raw[: raw.rfind("\n[completeness]\n") + 1])
        assert _reasons_for(gone) == ("completeness_absent",)

        below = tmp_path / "below.toml"
        below.write_text(raw + '\n[grounding_attestations."x"]\nclaim_id = "x"\n')
        assert "content_below_table" in _reasons_for(below)

        head, _, table = raw.partition("\n[completeness]\n")
        bare = tmp_path / "bare.toml"
        bare.write_text(
            f"{head}\n[completeness]\n{table.split('[completeness.sections]')[0]}"
        )
        assert _reasons_for(bare) == ("row_counts_absent",)

    def test_a_body_that_does_not_match_the_digest_is_its_own_reason(
        self, tmp_path: Path,
    ) -> None:
        """Reached by editing the body and leaving the table alone, so the row
        counts still agree and the digest is the only thing that can object."""
        source = _healthy_project(tmp_path)
        raw = source.read_text()
        edited = tmp_path / "edited.toml"
        edited.write_text(raw.replace("the first finding", "the FIRST finding"))
        assert _reasons_for(edited) == ("digest_mismatch",)

    def test_a_section_short_of_its_declared_count_is_its_own_reason(
        self, tmp_path: Path,
    ) -> None:
        """One claim removed and the table left saying three."""
        import tomli_w
        source = _healthy_project(tmp_path)
        doc = tomllib.loads(source.read_text())
        doc["claims"].pop(sorted(doc["claims"])[0])
        short = tmp_path / "short.toml"
        short.write_text(tomli_w.dumps(doc))
        assert "section_count_mismatch" in _reasons_for(short)

    def test_a_later_format_is_its_own_reason(self, tmp_path: Path) -> None:
        """Separated from the tampering reasons so a caller can refuse one and
        not the other, and reported beside them rather than instead of them.

        On a file that still has its table, which is what a later format
        writes, this is the only reason. That is the case the token exists for.
        """
        source = _healthy_project(tmp_path)
        ahead = tmp_path / "ahead.toml"
        ahead.write_text(_restamped(source.read_text(), _BACKUP_FORMAT + 1))
        assert verify_completeness_digest(ahead)
        assert _reasons_for(ahead) == ("format_ahead",)


class TestARotatedCopyIsNamedWhenItCouldHelp:
    """The recovery the accepted risk rested on, made real.

    Two tables take rows and refuse to give them up, so one row with a
    signature that does not verify makes every claim in the graph read
    tampered and nothing can remove it. That was accepted as a denial of
    service rather than a way to make a false claim read true, because
    recovery through the backup exists.

    It did not survive contact. The next backup after the row is planted
    copies it into claims.toml, so restoring from that file replays it. The
    generation the writer rotates aside is clean when exactly one mutation
    followed the plant, and nothing ever read it. After two it carries the
    row too, which is why the disclosure names that file rather than
    recommending it.
    """

    def _poisoned(self, tmp_path: Path) -> Path:
        import sqlite3

        home = tmp_path / "src"
        home.mkdir()
        _project_with_a_verdict(home)

        raw = sqlite3.connect(home / ".mareforma" / "graph.db")
        raw.execute(
            "INSERT INTO verdict_chain(seq, prev_tip, tip, verdict_kind, "
            "verdict_id, verdict_digest, issuer_keyid, signature, created_at) "
            "VALUES (99, 'x', 'junk', 'contradiction', 'nope', 'nope', "
            "'nokey', X'00', '2026-09-03T00:00:00+00:00')"
        )
        raw.commit()
        raw.close()

        # One honest mutation, which is all it takes for the backup to carry
        # the planted row into the recovery artifact.
        with mareforma.open(home, key_path=home / "root.key") as graph:
            graph.assert_claim("written after the plant", generated_by="after")
        assert (home / "claims.toml.prev").is_file()
        return home

    def test_the_previous_copy_is_named(self, tmp_path: Path) -> None:
        home = self._poisoned(tmp_path)
        target = tmp_path / "recovered"
        target.mkdir()
        shutil.copy(home / "claims.toml", target / "claims.toml")
        shutil.copy(home / "claims.toml.prev", target / "claims.toml.prev")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            restore(target)
        said = [
            str(w.message) for w in caught
            if "verdict chain restored" in str(w.message)
        ]
        assert said, "a poisoned chain restored without naming the clean copy"
        assert "claims.toml.prev" in said[0]
        assert _chain_reasons_for(target) == (
            "chain_damaged_previous_copy_exists",
        )

    def test_the_damage_is_reported_with_no_copy_to_name(
        self, tmp_path: Path,
    ) -> None:
        """The chain is broken whether or not a second file exists.

        Gating the whole sentence on the rotated copy meant restoring a damaged
        backup into an empty project, which is the documented recovery and by
        construction has no copy beside it, returned a plain success and said
        nothing. Deleting that file is free, and it is the first thing worth
        deleting.
        """
        home = self._poisoned(tmp_path)
        target = tmp_path / "recovered"
        target.mkdir()
        shutil.copy(home / "claims.toml", target / "claims.toml")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            restore(target)
        said = [
            str(w.message) for w in caught
            if "verdict chain restored" in str(w.message)
        ]
        assert said, "a damaged chain restored in silence"
        # Reported, but no file is pointed at, because there is none to point
        # at. Naming one that is not there is worse than naming none. Held to
        # the reason as well as the wording: the two cases returned different
        # reasons that nothing asserted, so collapsing them changed nothing
        # any test could see.
        assert "rotates aside" not in said[0]
        assert _chain_reasons_for(target) == ("chain_damaged",)

    def test_an_honest_restore_says_nothing(self, tmp_path: Path) -> None:
        """The direction that matters more: this must not fire on a good file.

        A healthy project restored beside its own rotated copy is the ordinary
        recovery, and telling that operator their chain is damaged would send
        them chasing a second file for no reason.
        """
        home = tmp_path / "honest"
        home.mkdir()
        _project_with_a_verdict(home)
        with mareforma.open(home, key_path=home / "root.key") as graph:
            graph.assert_claim("an ordinary later claim", generated_by="after")

        target = tmp_path / "recovered"
        target.mkdir()
        shutil.copy(home / "claims.toml", target / "claims.toml")
        shutil.copy(home / "claims.toml.prev", target / "claims.toml.prev")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            restore(target)
        assert not [
            w for w in caught if "verdict chain restored" in str(w.message)
        ]


class TestARetypedCountIsStillACount:
    """The careful edit must not be quieter than the clumsy one.

    Lifting the whole table of row counts out is reported. Retyping one of
    them from 3 to "3" was not, and a claim went missing either way, so the
    smaller edit bought silence the larger one did not. That is the same shape
    as reporting one reason instead of another, one level further down.
    """

    def _short_by_one(self, tmp_path: Path, declared: str) -> Path:
        """A backup missing a claim, its digest repaired, its count retyped.

        The digest is recomputed so it is not what answers here. Without that
        this would pass on `digest_mismatch` and prove nothing about counts.
        """
        import hashlib

        raw = _healthy_project(tmp_path).read_text()
        first = raw.index("[claims.")
        second = raw.index("[claims.", first + 1)
        third = raw.index("[claims.", second + 1)
        short = (raw[:second] + raw[third:]).replace(
            "claims = 3", f"claims = {declared}", 1,
        )
        body, sep, tail = short.partition("\n[completeness]\n")
        digest = hashlib.sha256((body + "\n").encode("utf-8")).hexdigest()
        label = declared.strip('"[]')
        out = tmp_path / f"short-{label}.toml"
        out.write_text(
            body + sep + re.sub(
                r'digest = "[0-9a-f]+"', f'digest = "{digest}"', tail, count=1,
            )
        )
        return out

    def test_an_honest_count_reports_the_missing_claim(
        self, tmp_path: Path,
    ) -> None:
        """The control. Without it the cases below prove nothing."""
        source = self._short_by_one(tmp_path, "3")
        # The claim the fixture rests on. Without it these cases could be
        # passing on a broken digest and proving nothing about counts.
        assert verify_completeness_digest(source)
        report, said = _restore_into(source, tmp_path / "recovered")
        assert report["claims_restored"] == 2
        assert said
        assert "section_count_mismatch" in _reasons_for(source)

    @pytest.mark.parametrize("declared", ['"3"', "3.0", "[3]", "true"])
    def test_a_count_that_is_not_a_number_is_reported(
        self, tmp_path: Path, declared: str,
    ) -> None:
        source = self._short_by_one(tmp_path, declared)
        report, said = _restore_into(source, tmp_path / f"rec-{declared!r}")
        assert report["claims_restored"] == 2, "the fixture lost no claim"
        assert said, f"a count of {declared} restored a short graph in silence"
        assert "row_count_not_a_number" in _reasons_for(source)


class TestTheDisclosureNeverCostsTheRecovery:
    """A message about a damaged backup must not destroy the recovery.

    The rotated-copy note is emitted inside the transaction that builds the
    graph, so under a caller that turns warnings into errors it aborted the
    restore, rolled it back and removed the project directory. Only when the
    rotated copy was present, which is to say only when the note had something
    useful to say. Measured before the fix: with the copy the restore died and
    .mareforma was gone; without it the same backup restored fine.
    """

    def test_a_restore_survives_a_caller_that_raises_on_warnings(
        self, tmp_path: Path,
    ) -> None:
        import sqlite3

        home = tmp_path / "src"
        home.mkdir()
        _project_with_a_verdict(home)
        raw = sqlite3.connect(home / ".mareforma" / "graph.db")
        raw.execute(
            "INSERT INTO verdict_chain(seq, prev_tip, tip, verdict_kind, "
            "verdict_id, verdict_digest, issuer_keyid, signature, created_at) "
            "VALUES (99, 'x', 'junk', 'contradiction', 'nope', 'nope', "
            "'nokey', X'00', '2026-09-03T00:00:00+00:00')"
        )
        raw.commit()
        raw.close()
        with mareforma.open(home, key_path=home / "root.key") as graph:
            graph.assert_claim("after the plant", generated_by="after")

        target = tmp_path / "recovered"
        target.mkdir()
        shutil.copy(home / "claims.toml", target / "claims.toml")
        shutil.copy(home / "claims.toml.prev", target / "claims.toml.prev")

        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            report = restore(target)

        assert report["claims_restored"] == 3
        assert (target / ".mareforma").is_dir(), (
            "the disclosure took the recovered graph with it"
        )

    def test_more_than_one_problem_is_not_reported_as_one(
        self, tmp_path: Path,
    ) -> None:
        """Quoting the first of five drops the more alarming ones."""
        import sqlite3

        home = tmp_path / "src"
        home.mkdir()
        _project_with_a_verdict(home)
        raw = sqlite3.connect(home / ".mareforma" / "graph.db")
        raw.execute(
            "INSERT INTO verdict_chain(seq, prev_tip, tip, verdict_kind, "
            "verdict_id, verdict_digest, issuer_keyid, signature, created_at) "
            "VALUES (99, 'x', 'junk', 'contradiction', 'nope', 'nope', "
            "'nokey', X'00', '2026-09-03T00:00:00+00:00')"
        )
        raw.commit()
        raw.close()
        with mareforma.open(home, key_path=home / "root.key") as graph:
            graph.assert_claim("after the plant", generated_by="after")

        target = tmp_path / "recovered"
        target.mkdir()
        shutil.copy(home / "claims.toml", target / "claims.toml")
        shutil.copy(home / "claims.toml.prev", target / "claims.toml.prev")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            restore(target)
        said = [
            str(w.message) for w in caught
            if "verdict chain restored" in str(w.message)
        ]
        assert said
        assert "more" in said[0], "only one of several problems was named"
        # And it does not recommend the rotated copy, only report that it is
        # there: after two mutations that copy carries the damage as well.
        assert "predates the damage is not something this can tell you" in said[0]


class TestTheCheckDoesNotDependOnWhereBytesSit:
    """Two readers that find a boundary by searching bytes can be made to
    disagree about the same file.

    One normalised line endings and the other did not, so a decoy header
    written with carriage returns was invisible to the digest and visible to
    the tail reader. A forged section between the two boundaries was then
    checked by neither, and the file restored with the digest verifying and
    nothing said at all. Measured on 274 appended bytes, no key, no digest
    recomputation.

    The answer is not a better search. It is to stop asking where the bytes
    are: the row counts name every table the writer emitted, so a table the
    document holds and the counts do not declare was added afterwards,
    wherever it sits.
    """

    def _forged(self, tmp_path: Path, tail: str = "") -> Path:
        raw = _healthy_project(tmp_path).read_text()
        claim_id = sorted(tomllib.loads(raw)["claims"])[0]
        out = tmp_path / f"forged{len(tail)}.toml"
        out.write_text(
            raw
            + f'\n[rekor_inclusions."{claim_id}"]\n'
            'uuid = "an-entry-that-was-never-submitted"\n'
            'raw_response_b64 = "eyJhIjp7ImJvZHkiOiJ4In19"\n'
            "log_index = 99\nintegrated_time = 1\n"
            'recorded_at = "2026-08-31T00:00:00+00:00"\n'
            + tail,
            newline="",
        )
        return out

    def test_a_decoy_header_does_not_buy_silence(self, tmp_path: Path) -> None:
        """The bypass, pinned. A carriage-return header below the forged
        section moved one reader's boundary and not the other's."""
        forged = self._forged(
            tmp_path, 'note = """\r\n[completeness]\r\n# """\n',
        )
        reasons = _reasons_for(forged)
        assert reasons, "a decoy header returned the file to silence"
        assert "section_not_declared" in reasons

        _, said = _restore_into(forged, tmp_path / "recovered")
        assert said
        assert "rekor_inclusions" in said[0]

    def test_the_plain_forgery_is_still_caught(self, tmp_path: Path) -> None:
        """The control, so the case above cannot pass by breaking both."""
        forged = self._forged(tmp_path)
        assert "section_not_declared" in _reasons_for(forged)

    def test_both_readers_find_the_same_boundary(self, tmp_path: Path) -> None:
        """The invariant whose violation made the bypass possible.

        Stated as a property rather than a case: whatever a file's line
        endings, the two readers agree about where its table begins.
        """
        source = _healthy_project(tmp_path)
        crlf = tmp_path / "crlf.toml"
        crlf.write_text(source.read_text().replace("\n", "\r\n"), newline="")

        assert verify_completeness_digest(source)
        assert verify_completeness_digest(crlf), (
            "a line-ending conversion read as an edit"
        )
        assert tables_below_completeness(source) == ()
        assert tables_below_completeness(crlf) == ()
        assert _reasons_for(crlf) == (), (
            "an honest file through a Windows editor was reported as tampered"
        )

        # Asserted against a value only a working reader produces. Comparing
        # against the empty tuple alone was satisfied by a reader that found no
        # boundary at all and returned the same thing, so the half of the
        # divergence that produced the bypass went unpinned.
        with_tail = tmp_path / "crlf-tail.toml"
        with_tail.write_text(
            (source.read_text() + '\n[rekor_inclusions."x"]\nuuid = "z"\n')
            .replace("\n", "\r\n"),
            newline="",
        )
        assert tables_below_completeness(with_tail) == ("rekor_inclusions",), (
            "the tail reader lost the boundary on a file with CRLF endings"
        )
