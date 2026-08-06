#!/bin/bash
#SBATCH -A m2616
#SBATCH -C cpu
#SBATCH -q preempt
#SBATCH --requeue
#SBATCH -N 1
#SBATCH -t 06:00:00
#SBATCH -J rc2-nightly-stepwise
#SBATCH -o /pscratch/sd/k/kennylo/cgsim-rubin/logs/rc2-nightly-stepwise-%j.log

# fgcmBuildFromIsolatedStars declares canMultiprocess=False, so a single
# whole-pipeline `pipetask run -j N` hard-fails on it -- run per-subset
# instead. -j>1 against this SQLite-backed repo on Lustre also hits
# `sqlite3.OperationalError: database is locked` under real contention
# (documented LSST-wide issue, not specific to this repo:
# https://community.lsst.org/t/diapipe-database-locking-with-bps-pipetask/8922).
# The serial retry below MUST pass --extend-run (or --skip-existing has
# nothing to compare against and silently redoes the whole subset) AND
# --clobber-outputs (a quantum can partially write before the lock error
# hits, so --skip-existing alone won't skip it but re-running collides
# with the orphaned output).

set -eo pipefail

source /cvmfs/sw.lsst.eu/almalinux-x86_64/lsst_distrib/w_2026_31/loadLSST.bash
setup lsst_distrib
setup obs_subaru

BASE=/pscratch/sd/k/kennylo/cgsim-rubin
REPO=$BASE/SMALL_HSC_run
PIPE="$DRP_PIPE_DIR/pipelines/HSC/DRP-RC2_subset.yaml"
CHAIN=u/kennylo/cgsim/nfull
QUERY="detector in (58, 50, 42, 47, 49, 41) AND visit in (29336, 11690, 11698, 29350, 11696, 11704, 11710, 11694, 1220, 1204, 23694, 1206, 23706, 23704, 1214, 23718, 19694, 19680, 30490, 1242, 19684, 30482, 19696, 1248, 1178, 17948, 17950, 17904, 1184, 17906, 17926, 17900, 11738, 358, 11724, 346, 22632, 11740, 22662, 322) AND tract = 9813 AND skymap = 'hsc_rings_v1'"

SKIP=""
if [ "${SLURM_RESTART_COUNT:-0}" -gt 0 ]; then
    SKIP="--skip-existing"
    echo "=== restart ${SLURM_RESTART_COUNT}: skipping completed quanta ==="
fi

run_subset () {
    local subset=$1 jobs=$2
    echo "=== subset $subset (-j $jobs) $(date) ==="
    if ! pipetask run -b "$REPO" -i HSC/RC2_subset/defaults -o "$CHAIN" \
            -p "$PIPE#$subset" -d "$QUERY" -j "$jobs" \
            --register-dataset-types $SKIP; then
        echo "=== subset $subset failed at -j $jobs; retrying serially, extending same run, skip-existing ==="
        pipetask run -b "$REPO" -i HSC/RC2_subset/defaults -o "$CHAIN" \
            -p "$PIPE#$subset" -d "$QUERY" -j 1 \
            --register-dataset-types --extend-run --skip-existing --clobber-outputs
    fi
}

run_subset nightlyStep1  32
run_subset nightlyStep2a 16
run_subset nightlyStep2b 1
run_subset nightlyStep2c 16
run_subset nightlyStep2d 16
run_subset nightlyStep3  32

echo "=== DONE: collection chain $CHAIN $(date) ==="
