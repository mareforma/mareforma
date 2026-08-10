"""
02_compounding_agents.py, What the instrument refuses to count.

Two failure modes an honest trust layer has to catch, shown end to end.

1. Absence. A finding whose analysis step never ran cannot earn GROUNDED. The
   observer watched the scope, saw no cited read, and returns UNGROUNDED, so
   the trust map shows an empty provenance edge: no observed execution stands
   behind the number. A finding whose step DID run and read its data reads
   GROUNDED on that source.

2. Uncertified convergence. Two agents on the same model that reach the same
   answer are one line of evidence, not two, and distinct signing keys and
   distinct datasets do not change that. This run shows the stricter half of
   the rule: the model calls answer from a transport the producer supplied, so
   the model behind each line is declared rather than observed, and the
   instrument returns UNVERIFIABLE in place of a count.

Run:
    python 02_compounding_agents.py

No API key and no network required. The model calls answer from an in-memory
httpx transport, so nothing leaves the process. That is also why no model call
is certified here. The datasets are small CSV files in a temp directory.
"""

import tempfile
from pathlib import Path

import httpx

import mareforma
from mareforma import signing as _signing
from mareforma.observe import observe
from mareforma.trust import (
    Direction,
    DirectionOfInterest,
    EffectEstimate,
    EffectType,
    Prediction,
    Proposition,
    TestType,
)

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_OPENAI_URL = "https://api.openai.com/v1/chat/completions"


def sep(title: str) -> None:
    print(f"\n{'─' * 62}")
    print(f"  {title}")
    print(f"{'─' * 62}")


def show(label: str, value: object) -> None:
    print(f"  {label:<28} {value}")


def _offline_client() -> httpx.Client:
    """An httpx client whose transport answers locally, no network, no key.

    The observer wraps ``httpx.Client.post`` and parses the request body for the
    model field, but the 200 comes from a transport this script supplied, so it
    certifies no model call. The lineage is recorded as a producer declaration
    (PROXY), which is what leaves the independence axis UNVERIFIABLE below.
    """
    return httpx.Client(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json={}))
    )


def model_check(client: httpx.Client, url: str, model: str, csv_path: Path):
    """Run one analysis: call the model, then read the cited dataset.

    Returns the observer's verdict, which carries both the model lineage
    captured at the POST boundary and the grounding verdict for the file read.
    """
    with observe(cites=str(csv_path)) as handle:
        # The model call. Its body names the model and the observer reads it off
        # the POST boundary, but on this transport the name is only declared.
        client.post(
            url,
            json={
                "model": model,
                "temperature": 0.0,
                "messages": [{"role": "user", "content": "analyze"}],
            },
        )
        # The analysis reads the cited dataset. A non-empty read of the cited
        # source is what grounds the finding.
        with open(csv_path) as fh:
            fh.read()
    return handle.verdict


def absent_check(csv_path: Path):
    """A run whose analysis step never executed: nothing reads the dataset."""
    with observe(cites=str(csv_path)) as handle:
        pass  # the step that would read csv_path never ran
    return handle.verdict


# A positive standardised mean difference with a 90% CI clear of zero, the same
# supporting outcome for every check below, so only the model differs.
def _estimate(n: int) -> EffectEstimate:
    return EffectEstimate(
        estimate_value=0.42,
        effect_type=EffectType.SMD,
        ci_lower=0.18,
        ci_upper=0.66,
        ci_level=0.90,
        n_total=n,
    )


# ---------------------------------------------------------------------------
# Setup: a shared graph, distinct agent keys, and datasets on disk
# ---------------------------------------------------------------------------

tmp = Path(tempfile.mkdtemp())

# Root key opens the graph and enrolls the agent validators. The first key
# against a fresh graph auto-enrolls as the root validator.
root_key = tmp / "_root_key"
_signing.bootstrap_key(root_key)
graph = mareforma.open(tmp, key_path=root_key)

# Three agents, three distinct signing keys. The signing key is the independence
# unit, so distinct keys are the strongest legacy signal of independent work.
agent_keys = {}
agent_graphs = {}
for name in ("agent-a", "agent-b", "agent-c"):
    kp = tmp / f"_{name}_key"
    _signing.bootstrap_key(kp)
    priv = _signing.load_private_key(kp)
    graph.enroll_validator(
        _signing.public_key_to_pem(priv.public_key()),
        identity=name,
        validator_type="llm",
    )
    agent_keys[name] = kp
    agent_graphs[name] = mareforma.open(tmp, key_path=kp)

# Datasets: distinct, non-empty CSV files. Distinct data is the other legacy
# signal of independence, and, like distinct keys, not enough on its own.
datasets = {}
for name in ("alpha", "beta", "gamma", "delta", "epsilon"):
    p = tmp / f"dataset_{name}.csv"
    rows = "\n".join(f"{i},{i * 2}" for i in range(1, 21))
    p.write_text(f"pre,post\n{rows}\n")
    datasets[name] = p

client = _offline_client()

# A pre-registered decision rule, bound to each proposition before any numbers
# are seen. The absence demo and the independence demo use separate propositions
# so neither leaks evidence into the other's count.
plan = Prediction(
    test_type=TestType.SUPERIORITY,
    direction_of_interest=DirectionOfInterest.INCREASE,
    alpha=0.05,
)
prop_absence = Proposition(
    subject="cell type A",
    relation="gap-junction coupling onto",
    object="cell type C",
    direction=Direction.INCREASES,
    scope={"region": "cortex", "species": "mouse"},
)
prop = Proposition(
    subject="cell type A",
    relation="inhibitory connectivity onto",
    object="cell type B",
    direction=Direction.INCREASES,
    scope={"region": "cortex", "species": "mouse"},
)
graph.register_plan(prop_absence, plan, generated_by="protocol")
graph.register_plan(prop, plan, generated_by="protocol")


# ---------------------------------------------------------------------------
# 1. The absence catch, a number with no observed execution
# ---------------------------------------------------------------------------
# An agent reports a result, but the step that reads the data never ran. The
# observer saw the whole scope and no cited read, so the finding cannot earn
# GROUNDED. It reads UNGROUNDED, and its provenance edge is empty.

sep("1. Absence, a finding whose step never executed")

absent_verdict = absent_check(datasets["delta"])
show("observed grounding", absent_verdict.grounding.value)
show("grounded sources", list(absent_verdict.grounded_sources) or "(none)")

absent = agent_graphs["agent-a"].assert_finding(
    prop_absence,
    plan,
    _estimate(651),
    data_id="dataset_delta",
    data_source=str(datasets["delta"]),
    generated_by="agent-a/absent-run",
    grounding=absent_verdict,
)
absent_map = graph.trust_map(absent["claim_id"])
show("trust map grounding", absent_map.get("grounding").value)
print(f"    {absent_map.get('grounding').residual}")

print()
print("  The bearing still computes from the pre-registered rule, but the")
print("  grounding axis reports UNGROUNDED: no observed read stands behind the")
print("  number. Absence is caught, not filled in.")

# The contrast: the same question, this time actually executed.
sep("   Contrast, the step runs and reads its data")

grounded_verdict = model_check(
    client, _ANTHROPIC_URL, "claude-3-5-sonnet-20241022", datasets["epsilon"]
)
show("observed grounding", grounded_verdict.grounding.value)
show("grounded sources", [Path(s).name for s in grounded_verdict.grounded_sources])

contrast = agent_graphs["agent-a"].assert_finding(
    prop_absence,
    plan,
    _estimate(842),
    data_id="dataset_epsilon",
    data_source=str(datasets["epsilon"]),
    generated_by="agent-a",
    grounding=grounded_verdict,
)
grounded_map = graph.trust_map(contrast["claim_id"])
show("trust map grounding", grounded_map.get("grounding").value)
print(f"    {grounded_map.get('grounding').residual}")


# ---------------------------------------------------------------------------
# 2. Same-model convergence does not count twice
# ---------------------------------------------------------------------------
# agent-a runs the first grounded check on the shared question. A second agent,
# distinct key and distinct dataset, reaches the same answer, but on the same
# model. The instrument does not read that as a second independent line.

sep("2. Two agents, same model, no count is certified")

verdict_a = model_check(
    client, _ANTHROPIC_URL, "claude-3-5-sonnet-20241022", datasets["alpha"]
)
agent_graphs["agent-a"].assert_finding(
    prop,
    plan,
    _estimate(842),
    data_id="dataset_alpha",
    data_source=str(datasets["alpha"]),
    generated_by="agent-a",
    grounding=verdict_a,
)

verdict_b = model_check(
    client, _ANTHROPIC_URL, "claude-3-5-sonnet-20241022", datasets["beta"]
)
line_b = agent_graphs["agent-b"].assert_finding(
    prop,
    plan,
    _estimate(1104),
    data_id="dataset_beta",
    data_source=str(datasets["beta"]),
    generated_by="agent-b",
    grounding=verdict_b,
)
map_b = graph.trust_map(line_b["claim_id"])
show("agent-a model", "claude-3-5-sonnet")
show("agent-b model", "claude-3-5-sonnet")
show("distinct keys", "yes")
show("distinct datasets", "yes")
show("effective independence", map_b.get("independence").value)
print(f"    {map_b.get('independence').residual}")

print()
print("  Distinct in every legacy axis, signer and dataset, but one model, and")
print("  that model was never observed. Two same-model checks would be one line")
print("  of evidence; here the instrument cannot even certify that much, so it")
print("  reports UNVERIFIABLE instead of a number.")


# ---------------------------------------------------------------------------
# 3. A different model name does not raise the count either
# ---------------------------------------------------------------------------

sep("3. A different model name, still nothing to count")

verdict_c = model_check(
    client, _OPENAI_URL, "gpt-4o-2024-08-06", datasets["gamma"]
)
line_c = agent_graphs["agent-c"].assert_finding(
    prop,
    plan,
    _estimate(970),
    data_id="dataset_gamma",
    data_source=str(datasets["gamma"]),
    generated_by="agent-c",
    grounding=verdict_c,
)
map_c = graph.trust_map(line_c["claim_id"])
show("agent-c model", "gpt-4o")
show("effective independence", map_c.get("independence").value)
print(f"    {map_c.get('independence').residual}")

print()
print("  A genuinely different model would be a second line, but a name in a")
print("  request body this script answered itself proves nothing. The axis that")
print("  matters is the model, not the key that signed it, and it is counted")
print("  only when the call is observed against a recognized provider host.")
print("  tests/test_examples_absence.py pins that path, where the count is 2.")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

client.close()
for g in agent_graphs.values():
    g.close()
graph.close()
print(f"\n{'─' * 62}")
print("  Done. Graph written to:", tmp / ".mareforma" / "graph.db")
