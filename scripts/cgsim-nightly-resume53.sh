#!/bin/bash
#SBATCH -A m2616
#SBATCH -C cpu
#SBATCH -q preempt
#SBATCH --requeue
#SBATCH -N 1
#SBATCH -t 02:00:00
#SBATCH -J rc2-nightly-resume53
#SBATCH -o /pscratch/sd/k/kennylo/cgsim-rubin/logs/rc2-nightly-resume53-%j.log

set -eo pipefail

source /cvmfs/sw.lsst.eu/almalinux-x86_64/lsst_distrib/w_2026_31/loadLSST.bash
setup lsst_distrib
setup obs_subaru

BASE=/pscratch/sd/k/kennylo/cgsim-rubin
REPO=$BASE/SMALL_HSC_run
CHAIN=u/kennylo/cgsim/nfull
# Pre-built via `pipetask qgraph ... --skip-existing-in "$CHAIN" -c ...`
# (standalone; confirmed correct: exactly 53 quanta remain across patches
# 43/52's cascade, with the relaxed QA thresholds already baked in). This
# sidesteps two bugs found the hard way:
#   1. `pipetask run` with --skip-existing-in inline did NOT apply the filter
#      during its own internal graph build (job 56390267, cancelled after
#      re-attempting ~1700 already-done quanta).
#   2. `pipetask run -g <graph>` (no -p) does not accept `-c` config
#      overrides -- LookupError: no task labeled X in the pipeline (job
#      56399982). Config must be baked in at qgraph-build time instead.
GRAPH=$BASE/artifacts/central_six_nightly_full_resume53_cfg.qgraph

echo "=== executing pre-built, config-baked 53-quantum resume graph $(date) ==="
if ! pipetask run -b "$REPO" -g "$GRAPH" -o "$CHAIN" -j 16 \
        --register-dataset-types; then
    echo "=== -j16 failed; retrying serially ==="
    pipetask run -b "$REPO" -g "$GRAPH" -o "$CHAIN" -j 1 \
        --register-dataset-types --extend-run --skip-existing --clobber-outputs
fi

echo "=== DONE: collection chain $CHAIN $(date) ==="
