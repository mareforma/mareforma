<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/mareforma/mareforma/main/assets/mareforma-wordmark-dark.svg">
    <img alt="Mareforma" src="https://raw.githubusercontent.com/mareforma/mareforma/main/assets/mareforma-wordmark.svg" width="420">
  </picture>
</p>

<p align="center">
  <strong>Where AI discovery becomes science</strong>
  <br>
  <sub>trust earned, not declared · uses real data · effectively independent · publicly verifiable · local-first</sub>
</p>

<p align="center">
  <a href="https://docs.mareforma.com/introduction/quickstart">Quickstart</a> &nbsp;·&nbsp;
  <a href="https://docs.mareforma.com">Docs</a> &nbsp;·&nbsp;
  <a href="https://github.com/mareforma/mareforma/tree/main/examples/">Examples</a>
</p>

<p align="center">
  <a href="https://pypi.org/project/mareforma/"><img src="https://img.shields.io/pypi/v/mareforma" alt="PyPI"></a>
  <a href="https://pypi.org/project/mareforma/"><img src="https://img.shields.io/pypi/pyversions/mareforma" alt="Python"></a>
  <a href="https://github.com/mareforma/mareforma/actions/workflows/tests.yml"><img src="https://github.com/mareforma/mareforma/actions/workflows/tests.yml/badge.svg" alt="Tests"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License: MIT"></a>
</p>

<hr>

Mareforma is a local-first library where AI scientists record their findings as claims. Each claim cites the claims it builds on and can contradict others, forming a knowledge graph. Trust is read from that graph, not from the agents' self-reported confidence.

## The silent failure it catches

The most dangerous result an AI scientist returns is the one that looks right but never touched the data. A step fails quietly, the model fills the gap from memory, and the answer reads like a real one.

Catch one yourself. You need `uv`, a local model, and a minute. Nothing to install: `uvx` fetches mareforma, runs it, and throws it away.

```bash
# a small model (0.5 GB), runs locally
ollama pull qwen3:0.6b

# the note the pipeline is supposed to read
mkdir -p notes && cat > notes/cell_counts.md <<'EOF'
# Note: cellular composition of the human brain

Isotropic fractionator counts (Azevedo et al., J Comp Neurol, 2009;
Herculano-Houzel, Front Hum Neurosci, 2009) put the adult human brain at
about 86 billion neurons, revising the long-cited "100 billion" figure.
The cerebral cortex holds about 16 billion of those neurons; the cerebellum
holds about 69 billion, despite its far smaller mass.
EOF

# a short RAG pipeline: read the notes, ask the model, print the answer
cat > ask.py <<'EOF'
import glob
import httpx

docs = [open(p).read() for p in sorted(glob.glob("notes/*.md"))]
context = "\n\n".join(docs)
question = "How many neurons are in the human brain?"

reply = httpx.post(
    "http://localhost:11434/api/chat",
    json={
        "model": "qwen3:0.6b",
        "messages": [{"role": "user", "content":
            f"Answer the question using the notes below, and cite them.\n\n"
            f"NOTES:\n{context}\n\nQUESTION: {question}"}],
        "stream": False, "think": False,
    },
    timeout=120,
).json()
print("FINDING:", reply["message"]["content"].strip())
EOF
```

Run it under the observer. `diagnose` runs your program and records every file it opens and every network call it makes; `--cites` names the file the answer should come from:

```bash
uvx mareforma diagnose --cites notes/cell_counts.md -- python ask.py
```

Mareforma prints your pipeline's answer, then its own report below it:

<pre>
FINDING: The human brain has about 86 billion neurons, as stated in the notes. [...]

  reads: 1
    [file] notes/cell_counts.md
  Grounding: GROUNDED
</pre>

It watched the process open the note and read from it, so the finding is `GROUNDED`.

Now rename the folder, which simulates the corpus not having synced, and run the exact same command as before:

```bash
mv notes notes.not-synced
uvx mareforma diagnose --cites notes/cell_counts.md -- python ask.py
```

<pre>
FINDING: The human brain contains approximately 86 billion neurons. [...] (Cite:
Russell, George P. "Neuroscience: An Introduction to the Study of the Brain," 1983)

  reads: 0
  Grounding: UNGROUNDED
</pre>

The script found no files, so it sent the model an empty blob of notes. The model answered from memory, invented a citation, and got the number right. Mareforma marks it `UNGROUNDED`.

Same command, same confident answer, both times correct. The only visible difference is `reads: 1` against `reads: 0`. Nothing else in your stack catches it: no exception, no differing log line, and re-running reproduces it. Ask the model and it says it read the notes. The verdict does not move, because it is not a guess about the text. It records what the process did at the I/O boundary.

The observer sits at the `requests`, `httpx`, `aiohttp`, `io.open`, `pandas`, and `polars` seams. The model call shows up as a socket, which buys the finding no credit: a socket cannot deliver a local file read, so absence stays trustworthy. Where it cannot see, such as duckdb running its own I/O, it returns `OPAQUE` instead of guessing.

That verdict is meant to travel with the finding. Sync the notes again, then record the claim with its computed grounding, not a label you chose:

```bash
mv notes.not-synced notes
cat > record.py <<'EOF'
import glob, httpx, mareforma
from mareforma.observe import observe

with observe(cites="notes/cell_counts.md") as obs:
    context = "\n\n".join(open(p).read() for p in sorted(glob.glob("notes/*.md")))
    reply = httpx.post("http://localhost:11434/api/chat", json={
        "model": "qwen3:0.6b", "stream": False, "think": False,
        "messages": [{"role": "user", "content":
            f"Using these notes, how many neurons are in the human brain?\n\n{context}"}],
    }, timeout=120).json()
    finding = reply["message"]["content"].strip()

claim = mareforma.open().assert_claim(
    finding,
    generated_by="agent/lab-a",
    source_name="notes/cell_counts.md",
    observed_grounding=obs.verdict.to_signed_dict(),  # computed, not declared
)
print(f"recorded claim {claim}")
print(f"grounding: {obs.verdict.grounding.value}")
EOF
uv run --with mareforma record.py
```

<pre>
recorded claim 75e4cbe5-f77a-45c0-9c00-57c065d83c90
grounding: GROUNDED
</pre>

The claim now carries `GROUNDED` as a computed fact, not a self-reported label. The same run records which model authored the finding, computed from the request that was actually sent; a local model is identified by the sha256 digest of the weights that served it, not by the name it reports. [Example 02](https://github.com/mareforma/mareforma/tree/main/examples/02_compounding_agents/) runs this catch end to end.

## Install

```bash
uv add mareforma
mareforma bootstrap   # optional: sign your claims and enable the public log
```

## Capabilities

| What you get | What it does |
|---|---|
| **Signed claims** | Each claim shows who stands behind it and cannot be altered unnoticed. |
| **Grounding check** | Computes whether a finding's step actually accessed the data it cites, or leaned on the model's memory, by observing the run. |
| **Trust map** | `mareforma map <claim>` shows every trust property (grounding, independence, contestation, witnessing) with how far it can be trusted, and says plainly what it cannot check. |
| **Verify** | `mareforma verify <claim>` re-checks the signatures, that the grounding verdict matches the data the finding cites, and the support level, with stable exit codes for CI (0 verified, 1 tampered, 2 unverifiable). |
| **Diagnose a run** | `mareforma diagnose -- python run.py` runs a target under the observer and reports what data actually flowed, and where a silent fallback hid. |
| **Audit a pipeline** | `mareforma audit --findings map.json -- python run.py` runs a pipeline that never imports mareforma and signs one grounding receipt per finding, from the observer alone. |
| **Serve a project to an agent** | `mareforma mcp serve` serves one project over the Model Context Protocol, read and verify only. Six tools to query, inspect and verify; no write path. |
| **Optional public log** | Publish a claim to a public, append-only log for an independent, timestamped record. |
| **Local-first** | Runs on local SQLite. Network only for the optional log. |

## Reading trust from the graph

Trust comes from a claim's place in the graph, never from a self-reported score. The lead signal is **effective independence**: how many checks behind a finding differ in model, data, and signer. Two agents on the same model are one line of evidence, not two, so the count holds until a check on a distinct model, or a human check, raises it. `mareforma map <claim>` reports the number and marks it `UNVERIFIABLE` when it cannot tell the models apart. When every signer traces back to one operator who could have made all the keys, it names that single trust root on the count: the number then rests on distinct model or human lines within one trust domain, not independence across operators.

High-trust claims are re-checked against their signatures on every read, so a tampered claim in a shared graph is caught when you query, not served.

Classification stays a secondary label the agent declares: `INFERRED` (model reasoning), `ANALYTICAL` (analysis run against real data), `DERIVED` (built on higher-trust claims). It is a declaration, kept honest by the computed grounding verdict above, never the trust signal on its own.

## Examples

| | Example | What it shows |
|---|---|---|
| 01 | [API Walkthrough](https://github.com/mareforma/mareforma/tree/main/examples/01_api_walkthrough/) | The full API in one runnable script |
| 02 | [Compounding Agents](https://github.com/mareforma/mareforma/tree/main/examples/02_compounding_agents/) | The absence catch and computed independence, run end to end |
| 03 | [Documented Contestation](https://github.com/mareforma/mareforma/tree/main/examples/03_documented_contestation/) | An agent challenges established consensus |
| 04 | [Private Data, Public Findings](https://github.com/mareforma/mareforma/tree/main/examples/04_private_data_public_findings/) | Two labs share how they reached a finding without sharing the data |
| 05 | [Drug Target Provenance](https://github.com/mareforma/mareforma/tree/main/examples/05_drug_target_provenance/) | A real research agent that labels which findings come from real data and which from the model's guess |
| 06 | [Verify in CI](https://github.com/mareforma/mareforma/tree/main/examples/06_ci_verify/) | `mareforma verify` as a GitHub Actions gate, keyed on exit codes |
| 07 | [Silent Failure Catch](https://github.com/mareforma/mareforma/tree/main/examples/07_silent_failure_catch/) | Two pipelines print the same number; one read the data. `pip install` plus one command, no model |

<hr>

[`AGENTS.md`](https://github.com/mareforma/mareforma/blob/main/AGENTS.md): execution contract and adapters &nbsp;·&nbsp;
[`ARCHITECTURE.md`](https://github.com/mareforma/mareforma/blob/main/ARCHITECTURE.md): system design &nbsp;·&nbsp;
[`SECURITY.md`](https://github.com/mareforma/mareforma/blob/main/SECURITY.md): threat model &nbsp;·&nbsp;
[`CONTRIBUTING.md`](https://github.com/mareforma/mareforma/blob/main/CONTRIBUTING.md): dev workflow &nbsp;·&nbsp;
[`CHANGELOG.md`](https://github.com/mareforma/mareforma/blob/main/CHANGELOG.md): releases
