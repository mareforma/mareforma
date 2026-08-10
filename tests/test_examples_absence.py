"""The absence demo, pinned so the example cannot rot.

The two claims ``examples/02_compounding_agents`` makes, held as tests:

- A finding whose analysis step never executed cannot earn GROUNDED. The
  observer sees no cited read and returns UNGROUNDED; the trust map's grounding
  edge names no observed source, so a number with no observed execution has an
  empty provenance edge.
- Two agents on the same model are one line of evidence. Effective independence
  stays 1 across distinct signing keys and datasets, and rises only when a
  genuinely different model checks the result. That count needs a real network
  transport, so it is pinned here rather than in the offline example.
- The shipped script and its README report what the engine printed, never a
  count it refused to compute.

All drive the real ``observe()`` path, without an API key or a network.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import httpx

import mareforma
from mareforma import signing as _signing
from mareforma.observe import ObservedGrounding, observe
from mareforma.trust import (
    Direction,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    Prediction,
    Proposition,
)
from mareforma.trust._store import effective_independence
from tests._helpers import _requires_repo_checkout

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_OPENAI_URL = "https://api.openai.com/v1/chat/completions"
_CLAUDE = "claude-3-5-sonnet-20241022"
_GPT = "gpt-4o-2024-08-06"


def _offline_client() -> httpx.Client:
    """An httpx client whose transport answers locally, no network, no key."""
    return httpx.Client(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json={}))
    )


def _proposition() -> Proposition:
    return Proposition(
        subject="cell type A",
        relation="inhibitory connectivity onto",
        object="cell type B",
        direction=Direction.INCREASES,
        scope={"region": "cortex", "species": "mouse"},
    )


def _plan() -> Prediction:
    from mareforma.trust import TestType

    return Prediction(
        test_type=TestType.SUPERIORITY,
        direction_of_interest=DirectionOfInterest.INCREASE,
        alpha=0.05,
    )


def _estimate(n: int) -> EffectEstimate:
    """A positive SMD with a 90% CI clear of zero, a supporting outcome."""
    return EffectEstimate(
        estimate_value=0.42,
        effect_type=EffectType.SMD,
        ci_lower=0.18,
        ci_upper=0.66,
        ci_level=0.90,
        n_total=n,
    )


def _dataset(tmp_path: Path, name: str) -> Path:
    p = tmp_path / f"dataset_{name}.csv"
    rows = "\n".join(f"{i},{i * 2}" for i in range(1, 21))
    p.write_text(f"pre,post\n{rows}\n")
    return p


def _model_check(client: httpx.Client, url: str, model: str, csv_path: Path):
    """Call the model, then read the cited dataset, under one observe scope."""
    with observe(cites=str(csv_path)) as handle:
        client.post(
            url,
            json={
                "model": model,
                "temperature": 0.0,
                "messages": [{"role": "user", "content": "analyze"}],
            },
        )
        with open(csv_path) as fh:
            fh.read()
    return handle.verdict


def _enroll_agents(graph, tmp_path: Path, names):
    """Enroll each agent as a distinct LLM validator and open its graph handle."""
    graphs = {}
    for name in names:
        kp = tmp_path / f"_{name}_key"
        _signing.bootstrap_key(kp)
        priv = _signing.load_private_key(kp)
        graph.enroll_validator(
            _signing.public_key_to_pem(priv.public_key()),
            identity=name,
            validator_type="llm",
        )
        graphs[name] = mareforma.open(tmp_path, key_path=kp)
    return graphs


def test_absence_catch(tmp_path: Path) -> None:
    """A step that never executed cannot earn GROUNDED; its edge is empty."""
    root_key = tmp_path / "_root_key"
    _signing.bootstrap_key(root_key)
    graph = mareforma.open(tmp_path, key_path=root_key)
    try:
        prop, plan = _proposition(), _plan()
        graph.register_plan(prop, plan, generated_by="protocol")

        # The analysis step never ran: nothing reads the cited dataset.
        delta = _dataset(tmp_path, "delta")
        with observe(cites=str(delta)) as handle:
            pass
        verdict = handle.verdict
        assert verdict.grounding is ObservedGrounding.UNGROUNDED
        # A number with no observed execution has an empty provenance edge.
        assert verdict.grounded_sources == ()

        result = graph.assert_finding(
            prop,
            plan,
            _estimate(651),
            data_id="dataset_delta",
            data_source=str(delta),
            generated_by="agent/absent-run",
            grounding=verdict,
        )
        grounding = graph.trust_map(result["claim_id"]).get("grounding")
        # Never GROUNDED, and the edge names no observed read.
        assert grounding.value in {"UNGROUNDED", "OPAQUE"}
        assert grounding.value != "GROUNDED"
        assert "no cited read observed" in grounding.residual

        # Contrast: the step runs and reads its data -> GROUNDED on that source.
        client = _offline_client()
        try:
            alpha = _dataset(tmp_path, "alpha")
            grounded = _model_check(client, _ANTHROPIC_URL, _CLAUDE, alpha)
        finally:
            client.close()
        assert grounded.grounding is ObservedGrounding.GROUNDED
        assert grounded.grounded_sources  # a non-empty provenance edge
    finally:
        graph.close()


def test_compounding_same_model_not_independent(tmp_path: Path, httpx_mock) -> None:
    """Two same-model agents stay at 1; a different model raises it to 2.

    COMPUTED lineage requires a real network transport (a producer-controlled
    offline transport is a PROXY declaration, not a socket capture), so the model
    call runs through a plain ``httpx.Client`` whose network transport is patched
    by ``httpx_mock``, the SDK path a genuine provider call takes.
    """
    httpx_mock.add_response(url=_ANTHROPIC_URL, json={"content": []}, is_reusable=True)
    httpx_mock.add_response(url=_OPENAI_URL, json={"choices": []})
    root_key = tmp_path / "_root_key"
    _signing.bootstrap_key(root_key)
    graph = mareforma.open(tmp_path, key_path=root_key)
    agent_graphs = _enroll_agents(graph, tmp_path, ("agent-a", "agent-b", "agent-c"))
    client = httpx.Client()
    try:
        prop, plan = _proposition(), _plan()
        graph.register_plan(prop, plan, generated_by="protocol")
        cid = prop.content_id()

        alpha = _dataset(tmp_path, "alpha")
        beta = _dataset(tmp_path, "beta")
        gamma = _dataset(tmp_path, "gamma")

        # Two agents on the SAME model, distinct keys + distinct datasets.
        agent_graphs["agent-a"].assert_finding(
            prop, plan, _estimate(842), data_id="dataset_alpha",
            data_source=str(alpha), generated_by="agent-a",
            grounding=_model_check(client, _ANTHROPIC_URL, _CLAUDE, alpha),
        )
        line_b = agent_graphs["agent-b"].assert_finding(
            prop, plan, _estimate(1104), data_id="dataset_beta",
            data_source=str(beta), generated_by="agent-b",
            grounding=_model_check(client, _ANTHROPIC_URL, _CLAUDE, beta),
        )
        # Distinct in every legacy axis, but one model: still one line.
        assert effective_independence(graph._conn, cid)["number"] == 1
        assert graph.trust_map(line_b["claim_id"]).get("independence").value == "1"

        # A genuinely different model is a second independent line.
        line_c = agent_graphs["agent-c"].assert_finding(
            prop, plan, _estimate(970), data_id="dataset_gamma",
            data_source=str(gamma), generated_by="agent-c",
            grounding=_model_check(client, _OPENAI_URL, _GPT, gamma),
        )
        assert effective_independence(graph._conn, cid)["number"] == 2
        assert graph.trust_map(line_c["claim_id"]).get("independence").value == "2"
    finally:
        client.close()
        for g in agent_graphs.values():
            g.close()
        graph.close()


_EXAMPLE_DIR = Path(__file__).resolve().parents[1] / "examples" / "02_compounding_agents"
_INDEPENDENCE_PRINT = re.compile(r"^ +effective independence +(\S+)$", re.M)
_COUNT_CLAIM = re.compile(
    r"(?:count (?:holds at|moves from)|independence stays) (\d+)"
)


@_requires_repo_checkout
def test_example_02_narration_matches_the_engine(tmp_path: Path) -> None:
    """The shipped script may not narrate a count the engine did not print.

    The example runs on a producer-supplied transport, so the model lineage is a
    declaration and the independence axis reads UNVERIFIABLE. Prose asserting a
    number the trust map refused to compute is the defect this repo exists to
    catch, and the README publishes the same block.
    """
    run = subprocess.run(
        [sys.executable, str(_EXAMPLE_DIR / "02_compounding_agents.py")],
        capture_output=True,
        text=True,
        timeout=300,
        check=True,
        env={**os.environ, "TMPDIR": str(tmp_path)},  # the example's graph, cleaned up
    )
    printed = _INDEPENDENCE_PRINT.findall(run.stdout)
    assert printed, run.stdout
    narrated = set(_COUNT_CLAIM.findall(run.stdout))
    assert narrated <= set(printed), (
        f"narration claims counts {sorted(narrated)}, engine printed {printed}"
    )

    readme = (_EXAMPLE_DIR / "README.md").read_text()
    assert _INDEPENDENCE_PRINT.findall(readme) == printed
