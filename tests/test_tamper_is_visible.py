"""A tamper value has to look like one, in every view of a map.

Both renderers coloured a property by its tier alone. The tier says where an
answer came from, and every tamper state this release added is COMPUTED, because
it was computed from evidence: a planted trust root, a write guard missing on
open, a contradiction record the signed verdicts do not support, a
transparency-log entry that does not verify. So each of them printed its badge in
the same green a healthy computed axis gets. The word said TAMPERED and the
colour said fine.

Whether an answer is alarming is a different question from where it came from,
and a map has to answer both.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import click
import pytest

import mareforma
from mareforma.cli import _TAMPER_FG, _echo_trust_map
from mareforma.trust_map import TAMPERED_VALUE, is_tamper_value
from mareforma.trust_map_html import _TAMPER_ACCENT, render_html
from tests._helpers import _bootstrap_key


def _tampered_map(root: Path):
    key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=key) as g:
        cid = g.assert_claim("a claim")
    raw = sqlite3.connect(root / ".mareforma" / "graph.db")
    raw.execute("DROP TRIGGER findings_no_delete")
    raw.commit()
    raw.close()
    with mareforma.open(root, key_path=key) as g:
        return g.trust_map(cid)


def _clean_map(root: Path):
    key = _bootstrap_key(root, "root.key")
    with mareforma.open(root, key_path=key) as g:
        cid = g.assert_claim("a claim")
        return g.trust_map(cid)


class TestTheWordIsSpelledOnce:
    def test_the_axes_and_the_renderers_share_it(self, tmp_path: Path) -> None:
        """Two spellings would put the renderers one edit away from silence."""
        tmap = _tampered_map(tmp_path)
        assert tmap.get("trust_root").value == TAMPERED_VALUE
        assert is_tamper_value(TAMPERED_VALUE)

    def test_an_ordinary_value_is_not_a_tamper_value(self, tmp_path: Path) -> None:
        tmap = _clean_map(tmp_path)
        for prop in tmap.properties:
            assert not is_tamper_value(prop.value), prop.name


class TestTheHtmlView:
    def test_a_tamper_row_is_not_painted_like_a_healthy_one(
        self, tmp_path: Path,
    ) -> None:
        html = render_html(_tampered_map(tmp_path))
        assert _TAMPER_ACCENT in html

    def test_a_clean_map_carries_no_alarm(self, tmp_path: Path) -> None:
        """Otherwise the accent means nothing: it has to be absent when the map
        is fine, or a reader learns to ignore it."""
        assert _TAMPER_ACCENT not in render_html(_clean_map(tmp_path))


class TestTheTerminalView:
    def _rendered(self, tmap) -> str:
        import io, contextlib

        buf = io.StringIO()
        # click strips styling when the stream is not a terminal, which is
        # exactly the condition a test runs under, so ask for it explicitly.
        with contextlib.redirect_stdout(buf):
            with click.Context(click.Command("x"), color=True):
                _echo_trust_map(tmap)
        return buf.getvalue()

    def test_the_tamper_value_is_styled(self, tmp_path: Path) -> None:
        out = self._rendered(_tampered_map(tmp_path))
        styled = click.style(TAMPERED_VALUE, fg=_TAMPER_FG, bold=True)
        assert styled in out

    def test_it_stays_aligned_with_every_other_value(
        self, tmp_path: Path,
    ) -> None:
        """The styled branch is a separate code path, which is how a value
        loses its indentation and nobody notices until a screenshot."""
        import re

        # ANSI escapes carry a "[" of their own, so the badge test has to run
        # on the stripped text or it matches the styling instead of the layout.
        plain = re.sub(r"\x1b\[[0-9;]*m", "", self._rendered(_tampered_map(tmp_path)))
        value_lines = [ln for ln in plain.splitlines()
                       if ln.strip() == TAMPERED_VALUE]
        assert value_lines
        for line in value_lines:
            assert line.startswith("      "), repr(line)
