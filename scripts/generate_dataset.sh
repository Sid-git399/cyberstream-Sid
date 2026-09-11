#!/usr/bin/env bash
# Performance-testing dataset generation (Section 60).
# Does NOT run automatically — invoke explicitly with the size you want.
set -euo pipefail

SIZE="${1:-small}"   # small | medium | large
OUT="/data/raw/dataset_${SIZE}.jsonl"

echo "Generating '${SIZE}' preset to ${OUT}"
echo "Expected resource use: small ~seconds/tens of MB, medium ~1-2 min/~500MB, large ~10-20 min/~5GB+"

python3 "$(dirname "$0")/../generator/generator.py" \
  --preset "${SIZE}" --scenario normal --rate 0 --sink jsonl --out "${OUT}"
