#!/usr/bin/env bash
set -euo pipefail

# Default: DO NOT change tracked files
: "${SETUP_MUTATE:=0}"

# If running under agents/CI (incl. Codex's /workspace), force non-mutating
if [[ -n "${CI:-}" || -n "${GITHUB_ACTIONS:-}" || "${PWD}" == /workspace/* ]]; then
  SETUP_MUTATE=0
fi

# --- Non-mutating bootstrap ---
python -m venv .venv || true
if [[ -f ".venv/bin/activate" ]]; then
  . .venv/bin/activate
elif [[ -f ".venv/Scripts/activate" ]]; then
  . .venv/Scripts/activate
fi
pip install -r requirements.txt || true

# --- Mutating steps (opt-in only) ---
if [[ "${SETUP_MUTATE}" == "1" ]]; then
  echo "Running mutating steps…"
  ruff check --fix . || true
  ruff format . || true
  # python tools/bump_version.py || true
else
  echo "Skipping mutating steps (SETUP_MUTATE=0)"
fi
