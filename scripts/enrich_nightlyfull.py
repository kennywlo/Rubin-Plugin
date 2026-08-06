#!/usr/bin/env python3
"""Enrich raees-rc2-nightlyFull-v0.1 -> v0.2 with measured costs and file
sizes, querying the multi-run CHAIN u/kennylo/cgsim/nfull. A handful of
quanta (patch 52 / g-band cascade, excluded due to ZeroFootprintError -- a
physical data floor, not a config threshold) never executed; those are left
with cost absent / bytes_est null, same honest-gap convention as v0.2's
originally-null bytes_est entries.
"""
import json
import os
import sys
from collections import defaultdict

from lsst.daf.butler import Butler
from lsst.daf.butler.registry import DatasetTypeError
from lsst.pipe.base import QuantumGraph

V01_DIR = "/pscratch/sd/k/kennylo/cgsim-rubin/artifacts/raees-rc2-nightlyFull-v0.1"
OUT_DIR = "/pscratch/sd/k/kennylo/cgsim-rubin/artifacts/raees-rc2-nightlyFull-v0.2"
GRAPH = "/pscratch/sd/k/kennylo/cgsim-rubin/artifacts/central_six_nightly_full_9813.qgraph"
REPO = "/pscratch/sd/k/kennylo/cgsim-rubin/SMALL_HSC_run"
CHAIN = "u/kennylo/cgsim/nfull"

os.makedirs(OUT_DIR, exist_ok=True)
butler = Butler(REPO, collections=[CHAIN])
graph = QuantumGraph.loadUri(GRAPH)


def qkey(task, data_id):
    d = dict(data_id)
    return (task, tuple(sorted((k, v) for k, v in d.items())))


_resolve_cache = {}


def resolve_size(dstype_name, data_id):
    """Look up the CHAIN's actual dataset for (type, dataId) -- the topology
    graph's own embedded DatasetRef UUIDs were never materialized (each
    subset execution assigned its own fresh UUIDs), so we must re-resolve by
    dataset type + dataId against the real execution chain, not trust the
    graph's ref identity."""
    key = (dstype_name, data_id)
    if key in _resolve_cache:
        return _resolve_cache[key]
    try:
        ref = butler.find_dataset(dstype_name, data_id, collections=CHAIN)
    except Exception:
        ref = None
    size = None
    if ref is not None:
        try:
            size = os.stat(butler.getURI(ref).ospath).st_size
        except Exception:
            pass
    _resolve_cache[key] = (ref, size)
    return ref, size


node_info = {}
n_executed = n_unexecuted = 0
for node in graph:
    task = node.taskDef.label
    data_id = dict(node.quantum.dataId.mapping)
    sizes_in = defaultdict(lambda: [0, 0])
    sizes_out = defaultdict(lambda: [0, 0])
    cost = None
    any_output_exists = False
    for conns, sizes in ((node.quantum.inputs, sizes_in), (node.quantum.outputs, sizes_out)):
        for dstype, refs in conns.items():
            for ref in refs:
                found_ref, size = resolve_size(dstype.name, ref.dataId)
                if found_ref is not None:
                    sizes[dstype.name][0] += 1
                    if size is not None:
                        sizes[dstype.name][1] += size
                    if conns is node.quantum.outputs:
                        any_output_exists = True
    for dstype, refs in node.quantum.outputs.items():
        if dstype.name.endswith("_metadata"):
            for ref in refs:
                found_ref, _ = resolve_size(dstype.name, ref.dataId)
                if found_ref is None:
                    continue
                try:
                    md = json.load(open(butler.getURI(found_ref).ospath))
                    a = md["metadata"]["quantum"]["arrays"]
                    cost = {
                        "cpu_s": round(a["endCpuTime"][-1] - a["prepCpuTime"][0], 3),
                        "max_rss_bytes": int(a["endMaxResidentSetSize"][-1]),
                        "start_utc": a["prepUtc"][0],
                        "end_utc": a["endUtc"][-1],
                    }
                except Exception:
                    pass
    if any_output_exists:
        n_executed += 1
    else:
        n_unexecuted += 1
    node_info[qkey(task, data_id)] = (dict(sizes_in), dict(sizes_out), cost, any_output_exists)

print(f"indexed {len(node_info)} graph nodes: {n_executed} executed, {n_unexecuted} unexecuted", file=sys.stderr)

n_cost = n_bytes = n_missing = 0
unexecuted_examples = []
with open(f"{V01_DIR}/quanta.jsonl") as fin, open(f"{OUT_DIR}/quanta.jsonl", "w") as fout:
    for line in fin:
        rec = json.loads(line)
        rec["schema_version"] = "0.2"
        info = node_info.get(qkey(rec["task"], rec["data_id"]))
        if info is None:
            print(f"WARN no graph node for qid {rec['qid']}", file=sys.stderr)
        else:
            sizes_in, sizes_out, cost, executed = info
            if not executed:
                n_missing += 1
                if len(unexecuted_examples) < 20:
                    unexecuted_examples.append((rec["qid"], rec["task"], rec["data_id"]))
            for direction, sizes in (("inputs", sizes_in), ("outputs", sizes_out)):
                for summary in rec[direction]:
                    hit = sizes.get(summary["dataset_type"])
                    if hit and hit[0] > 0:
                        summary["bytes_est"] = hit[1]
                        n_bytes += 1
            if cost:
                rec["cost"] = cost
                n_cost += 1
        fout.write(json.dumps(rec) + "\n")
print(f"quanta written: cost on {n_cost}, bytes_est filled on {n_bytes} summaries, "
      f"{n_missing} quanta never executed (left without cost)", file=sys.stderr)
print("unexecuted qid/task/dataId sample:", unexecuted_examples, file=sys.stderr)

with open(f"{V01_DIR}/edges.jsonl") as fin, open(f"{OUT_DIR}/edges.jsonl", "w") as fout:
    for line in fin:
        rec = json.loads(line)
        rec["schema_version"] = "0.2"
        fout.write(json.dumps(rec) + "\n")

manifest = json.load(open(f"{V01_DIR}/qgraph_manifest.json"))
manifest["schema_version"] = "0.2"
manifest["provenance"]["timing_run"] = {
    "chain_collection": CHAIN,
    "butler_repo": REPO,
    "qgraph_topology_source": os.path.basename(GRAPH),
    "site": "NERSC Perlmutter CPU node",
    "cpu_model": "AMD EPYC 7763 64-Core Processor (2 sockets, Perlmutter CPU node)",
    "execution_mode": "stepwise per-subset (nightlyStep1, 2a-2d, 3), multiple Slurm jobs chained into one run collection due to SQLite lock contention retries and a QA-threshold resume -- see scripts/cgsim-nightly-stepwise.sh and cgsim-nightly-resume53.sh in this repo for the full recovery sequence",
    "quanta_total": len(node_info),
    "quanta_executed": n_executed,
    "quanta_unexecuted": n_unexecuted,
    "qa_overrides_applied": {
        "detection:scaleVariance.limit": "10.0 -> 1000 (patch 52 measured factor 465)",
        "detection:detection.minFractionSources": "0.02 -> 0.005 (patch 43 measured 10 of required 20 sky sources)",
        "detection:detection.minGoodPixelFraction": "0.005 -> 0.001 (patch 52 measured 0.0037 good-pixel fraction)",
    },
    "known_gap": "patch 52, g-band excluded from the query (NOT (patch=52 AND band='g')) after ZeroFootprintError -- a physical data floor (no detectable sources in that sparse region), not a relaxable threshold. 11 task types that depended solely on that quantum (measure, forcedPhotCoadd, deblend, mergeDetections, deconvolve, fitDeepCoaddPsfGaussians, mergeMeasurements, fitDeblendedObjectsExp, fitDeblendedObjectsSersic, writeObjectTable, detection itself for that dataId) are absent from the executed set; the tract-wide consolidation tasks proceeded without that patch/band's contribution. Affected quanta in this bundle are left without a cost block (bytes_est null), same honest-gap convention used for input n=0 entries.",
    "cost_semantics": "cost.cpu_s = quantum endCpuTime - prepCpuTime (process CPU seconds, single process); flops = cpu_s under the core-seconds convention (site corepower = 1/core)",
}
json.dump(manifest, open(f"{OUT_DIR}/qgraph_manifest.json", "w"), indent=2, sort_keys=True)
print("manifest written", file=sys.stderr)
