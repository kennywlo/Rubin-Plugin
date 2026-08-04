#!/usr/bin/env bash
# Package the rc2_subset_nightlyStep1_export run1 bundle for external handoff:
# copies the manifest/quanta/edges + validator into a versioned dir, validates
# it, checksums it, and tars it up.
set -euo pipefail

RUN_DIR="$PSCRATCH/cgsim-rubin/artifacts/rc2_subset_nightlyStep1_export/run1"
HANDOFF="$PSCRATCH/cgsim-rubin/artifacts/raees-rc2-nightlyStep1-v0.2"
VALIDATOR="$HOME/llm-apps/app/CGSim/rubin-data/validate_bundle.py"

mkdir -p "$HANDOFF"

cp "$RUN_DIR/qgraph_manifest.json" "$HANDOFF/"
cp "$RUN_DIR/quanta.jsonl" "$HANDOFF/"
cp "$RUN_DIR/edges.jsonl" "$HANDOFF/"
cp "$VALIDATOR" "$HANDOFF/"

python3 "$HANDOFF/validate_bundle.py" "$HANDOFF/qgraph_manifest.json" | tee "$HANDOFF/validation.txt"

(
  cd "$HANDOFF"
  sha256sum qgraph_manifest.json quanta.jsonl edges.jsonl validate_bundle.py validation.txt > SHA256SUMS
)

tar -C "$(dirname "$HANDOFF")" -czf "${HANDOFF}.tar.gz" "$(basename "$HANDOFF")"

echo
echo "Bundle dir: $HANDOFF"
echo "Tarball:    ${HANDOFF}.tar.gz"
echo
echo "To verify after transfer:"
echo "  tar xzf $(basename "${HANDOFF}.tar.gz")"
echo "  cd $(basename "$HANDOFF")"
echo "  sha256sum -c SHA256SUMS"
echo "  python3 validate_bundle.py qgraph_manifest.json"
