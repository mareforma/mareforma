#!/usr/bin/env python3
"""
05_drug_target_provenance.py — one script to set up and run the MEDEA drug target demo.

Usage
-----
    python 05_drug_target_provenance.py            # install + download + run experiment
    python 05_drug_target_provenance.py --install  # install packages only
    python 05_drug_target_provenance.py --data     # download MedeaDB only
    python 05_drug_target_provenance.py --run      # run experiment only (env already set up)

Run from the examples/ai_agent_drug_target/ directory.
Requires uv on PATH. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

HERE      = Path(__file__).parent.resolve()
VENV      = HERE / "medea_env"
VENV_PY   = VENV / "bin" / "python"
VENV_HF   = VENV / "bin" / "hf"
MEDEA_SRC = HERE / "Medea"
MARE_SRC  = HERE / "../.."
DATA_DIR  = HERE / "data" / "medeadb" / "raw"
ENV_FILE  = HERE / ".env"

PYTHON_VERSION = "3.10"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd, **kwargs):
    """Run a command, streaming output, exit on failure."""
    print(f"\n>>> {' '.join(str(c) for c in cmd)}\n")
    result = subprocess.run(cmd, **kwargs)
    if result.returncode != 0:
        sys.exit(result.returncode)


def find_uv() -> Path:
    uv = shutil.which("uv")
    if uv:
        return Path(uv)
    # common location when uv was just installed
    candidates = [
        Path.home() / ".local" / "bin" / "uv",
        Path.home() / ".var" / "app" / "com.visualstudio.code" / "bin" / "uv",
        Path.home() / ".cargo" / "bin" / "uv",
    ]
    for c in candidates:
        if c.exists():
            return c
    print("uv not found. Install it with:\n  curl -LsSf https://astral.sh/uv/install.sh | sh")
    sys.exit(1)


def find_python310(uv: Path) -> Path:
    result = subprocess.run(
        [uv, "python", "find", PYTHON_VERSION],
        capture_output=True, text=True
    )
    if result.returncode == 0 and result.stdout.strip():
        return Path(result.stdout.strip())
    # not installed yet — install it
    print(f"Python {PYTHON_VERSION} not found, installing via uv...")
    run([uv, "python", "install", PYTHON_VERSION])
    result = subprocess.run(
        [uv, "python", "find", PYTHON_VERSION],
        capture_output=True, text=True
    )
    if result.returncode != 0 or not result.stdout.strip():
        print(f"Could not locate Python {PYTHON_VERSION} after install.")
        sys.exit(1)
    return Path(result.stdout.strip())


def uv_pip(uv: Path, *args):
    run([uv, "pip", "install", "--python", VENV_PY, *args])


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------

def stage_install(uv: Path):
    print("=" * 60)
    print("STAGE 1 — Install")
    print("=" * 60)

    # Clone Medea if not already present
    if not MEDEA_SRC.exists():
        run(["git", "clone", "https://github.com/mims-harvard/Medea.git", str(MEDEA_SRC)])
    else:
        print(f"Medea source already at {MEDEA_SRC}, skipping clone.")

    # Create venv
    if VENV_PY.exists():
        print(f"venv already exists at {VENV}, skipping creation.")
    else:
        python310 = find_python310(uv)
        run([python310, "-m", "venv", str(VENV)])

    # ----------------------------------------------------------------
    # Install order matters — see SETUP.md for the full explanation.
    # ----------------------------------------------------------------

    # agentlite-llm hard-pins openai==1.10 in its metadata; install
    # without deps so we can use openai==1.82.1 instead.
    uv_pip(uv, "agentlite-llm", "--no-deps")

    # Medea editable, also without deps (same reason).
    uv_pip(uv, "-e", str(MEDEA_SRC), "--no-deps")

    # mareforma + all real runtime deps.
    # Key version pins:
    #   openai==1.82.1  — required by Medea; agentlite says ==1.10 but
    #                     we bypassed that with --no-deps above
    #   pandas==1.5.3   — binary-compatible with numpy==1.26.4
    #   numpy==1.26.4   — must be <2.0; newer numpy segfaults this pandas
    #   pyarrow==14.0.2 — last build against numpy 1.x
    uv_pip(uv,
        "-e", str(MARE_SRC),
        "openai==1.82.1",
        "pandas==1.5.3",
        "numpy==1.26.4",
        "pyarrow==14.0.2",
        "ollama", "anthropic", "google-generativeai",
        "sentence-transformers", "FlagEmbedding",
        "transformers", "tokenizers", "torch", "torchvision",
        "accelerate", "safetensors",
        "spacy", "spacy-legacy", "spacy-loggers", "nltk", "keybert",
        "scikit-learn", "scipy", "peft", "umap-learn", "einops",
        "datasets>=2.14.0,<3.0.0",
        "h5py", "openpyxl", "dill", "qnorm",
        "biothings-client", "mygene", "gseapy", "comut",
        "requests", "requests-cache", "beautifulsoup4",
        "httpx", "httpx-sse", "aiohttp", "lxml",
        "duckduckgo-search", "wikipedia", "ir-datasets",
        "matplotlib", "seaborn", "plotly",
        "statannotations", "colorcet", "palettable",
        "wandb", "rich", "psutil", "python-dotenv",
        "pydantic", "pydantic-settings",
        "click", "typer", "thefuzz", "tqdm",
        "retry-requests", "tenacity",
        "tidepy", "gdown", "filelock",
        "huggingface-hub>=0.34.0,<1.0", "tiktoken",
        "networkx", "sympy", "joblib", "regex",
        "pyyaml", "certifi",
    )

    # langchain suite compatible with agentlite (needs langsmith==0.0.87,
    # not the >=0.3.x in Medea's requirements.txt).
    uv_pip(uv, "--no-deps",
        "langchain==0.1.3",
        "langchain-community==0.0.20",
        "langchain-core==0.1.23",
        "langchain-openai==0.0.5",
        "langsmith==0.0.87",
    )

    # Transitive deps that langchain-community pulls in.
    uv_pip(uv, "jsonpatch", "jsonpointer", "sqlalchemy")

    print("\nInstall complete.")


def stage_data():
    print("=" * 60)
    print("STAGE 2 — Download MedeaDB (~21 GB)")
    print("=" * 60)

    if not VENV_HF.exists():
        print(f"huggingface-hub CLI not found at {VENV_HF}.")
        print("Run the install stage first.")
        sys.exit(1)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Check if already downloaded (rough check — all 4 dirs present)
    expected = {"compass", "depmap_24q2", "pinnacle_embeds", "transcriptformer_embedding"}
    present = {d.name for d in DATA_DIR.iterdir() if d.is_dir()} if DATA_DIR.exists() else set()
    if expected.issubset(present):
        print("MedeaDB already downloaded, skipping.")
        return

    run([
        str(VENV_HF), "download", "mims-harvard/MedeaDB",
        "--repo-type", "dataset",
        "--local-dir", str(DATA_DIR),
    ])

    print("\nData download complete.")


def stage_run():
    print("=" * 60)
    print("STAGE 3 — Run the experiment")
    print("=" * 60)

    if not VENV_PY.exists():
        print("medea_env not found. Run the install stage first.")
        sys.exit(1)

    if not ENV_FILE.exists():
        print(f".env not found at {ENV_FILE}")
        print("Create it from Medea/env_template.txt and set at minimum:")
        print("  MEDEADB_PATH=data/medeadb/raw")
        print("  BACKBONE_LLM=gpt-4o")
        print("  OPENAI_API_KEY=sk-...")
        sys.exit(1)

    # Activate the venv by putting it first on PATH
    env = os.environ.copy()
    env["PATH"] = str(VENV / "bin") + os.pathsep + env.get("PATH", "")
    env["VIRTUAL_ENV"] = str(VENV)

    # mareforma build runs build_transform.py (both MEDEA forks)
    run(["mareforma", "build"], cwd=HERE, env=env)

    # Compare the two forks artifact-by-artifact
    run(
        ["mareforma", "cross-diff", "ra_cd4.medea_run", "sle_cd4.medea_run"],
        cwd=HERE, env=env,
    )

    print("\nExperiment complete.")
    print(f"Results: {HERE / 'claims.toml'}")
    print(f"Run log: {HERE / '.mareforma' / 'commits' / 'transforms.jsonl'}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install", action="store_true", help="Install packages only")
    parser.add_argument("--data",    action="store_true", help="Download MedeaDB only")
    parser.add_argument("--run",     action="store_true", help="Run experiment only")
    args = parser.parse_args()

    # If no flags, run everything
    run_all = not any([args.install, args.data, args.run])

    uv = find_uv()

    if args.install or run_all:
        stage_install(uv)

    if args.data or run_all:
        stage_data()

    if args.run or run_all:
        stage_run()


if __name__ == "__main__":
    main()
