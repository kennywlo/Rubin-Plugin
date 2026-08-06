#!/bin/bash
#SBATCH -A m2616
#SBATCH -C cpu
#SBATCH -q preempt
#SBATCH --requeue
#SBATCH -N 1
#SBATCH -t 06:00:00
#SBATCH -J rc2-nightly-resume-step3
#SBATCH -o /pscratch/sd/k/kennylo/cgsim-rubin/logs/rc2-nightly-resume-step3-%j.log

set -eo pipefail

source /cvmfs/sw.lsst.eu/almalinux-x86_64/lsst_distrib/w_2026_31/loadLSST.bash
setup lsst_distrib
setup obs_subaru

BASE=/pscratch/sd/k/kennylo/cgsim-rubin
REPO=$BASE/SMALL_HSC_run
PIPE="$DRP_PIPE_DIR/pipelines/HSC/DRP-RC2_subset.yaml"
CHAIN=u/kennylo/cgsim/nfull
QUERY="detector in (58, 50, 42, 47, 49, 41) AND visit in (29336, 11690, 11698, 29350, 11696, 11704, 11710, 11694, 1220, 1204, 23694, 1206, 23706, 23704, 1214, 23718, 19694, 19680, 30490, 1242, 19684, 30482, 19696, 1248, 1178, 17948, 17950, 17904, 1184, 17906, 17926, 17900, 11738, 358, 11724, 346, 22632, 11740, 22662, 322) AND tract = 9813 AND skymap = 'hsc_rings_v1'"

# step1/2a/2b/2c/2d already completed and committed in $CHAIN (from job 56382801).
# Resume nightlyStep3 only: extend the run it partially completed, clobber the
# one partially-written quantum that blocked the previous retry, skip the rest.
echo "=== resuming nightlyStep3, extending run, clobbering partial outputs $(date) ==="
if ! pipetask run -b "$REPO" -i HSC/RC2_subset/defaults -o "$CHAIN" \
        -p "$PIPE#nightlyStep3" -d "$QUERY" -j 32 \
        --register-dataset-types --extend-run --skip-existing --clobber-outputs; then
    echo "=== -j32 resume failed; retrying serially ==="
    pipetask run -b "$REPO" -i HSC/RC2_subset/defaults -o "$CHAIN" \
        -p "$PIPE#nightlyStep3" -d "$QUERY" -j 1 \
        --register-dataset-types --extend-run --skip-existing --clobber-outputs
fi

echo "=== DONE: collection chain $CHAIN $(date) ==="
