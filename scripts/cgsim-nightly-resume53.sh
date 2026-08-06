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
# Pre-built via `pipetask qgraph ... --skip-existing-in "$CHAIN"` (standalone,
# confirmed correct: exactly 53 quanta remain across patches 43/52's cascade,
# vs. `pipetask run` with the identical flags which for unknown reasons did
# NOT apply skip-existing-in during its own internal graph build and started
# re-executing already-completed quanta instead (job 56390267, cancelled).
# Executing this pre-built, already-filtered graph sidesteps that discrepancy
# entirely -- there is nothing left for skip-existing to get wrong.
GRAPH=$BASE/artifacts/central_six_nightly_full_resume53.qgraph
CFG="-c detection:scaleVariance.limit=1000 -c detection:detection.minFractionSources=0.005"

echo "=== executing pre-built 53-quantum resume graph $(date) ==="
if ! pipetask run -b "$REPO" -g "$GRAPH" -o "$CHAIN" -j 16 $CFG \
        --register-dataset-types; then
    echo "=== -j16 failed; retrying serially ==="
    pipetask run -b "$REPO" -g "$GRAPH" -o "$CHAIN" -j 1 $CFG \
        --register-dataset-types --extend-run --skip-existing --clobber-outputs
fi

echo "=== DONE: collection chain $CHAIN $(date) ==="
