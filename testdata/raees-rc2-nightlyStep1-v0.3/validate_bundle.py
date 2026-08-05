#!/usr/bin/env python3
"""
Standalone streaming validator for a rubin_campaign_schema v0.3 QuantumGraph
export bundle: qgraph_manifest.json + quanta.jsonl + edges.jsonl (schema
section 4 in LLM-Interface/rubin_campaign_schema_v0.3.md).

Pure standard library -- does not require an LSST environment. Reads
quanta.jsonl/edges.jsonl one line at a time rather than loading either file
into a full list, so it scales to graphs approaching 10^6 quanta the same
way the exporter does.

Usage:
    python validate_bundle.py /path/to/qgraph_manifest.json

Exit code 0 and "OK: bundle is valid" on success; exit code 1 and a list of
issues (to stderr) otherwise.
"""

import argparse
import json
import os
import sys

SCHEMA_VERSION = "0.3"

REQUIRED_PROVENANCE_KEYS = {
    "qgraph_uuid", "qgraph_path", "butler_repo", "butler_config", "pinned",
}
REQUIRED_PINNED_KEYS = {"lsst_distrib", "pipe_base", "ctrl_bps", "drp_pipe", "obs_subaru"}


def _fail(errors, msg):
    errors.append(msg)


def validate(manifest_path):
    """Return a list of error strings; empty means the bundle is valid."""
    errors = []
    bundle_dir = os.path.dirname(os.path.abspath(manifest_path))

    if not os.path.isfile(manifest_path):
        return [f"manifest does not exist: {manifest_path}"]

    with open(manifest_path) as f:
        try:
            manifest = json.load(f)
        except json.JSONDecodeError as e:
            return [f"manifest is not valid JSON: {e}"]

    if manifest.get("schema_version") != SCHEMA_VERSION:
        _fail(errors, f"manifest schema_version != {SCHEMA_VERSION!r}: {manifest.get('schema_version')!r}")

    provenance = manifest.get("provenance", {})
    missing_prov = REQUIRED_PROVENANCE_KEYS - provenance.keys()
    if missing_prov:
        _fail(errors, f"manifest provenance missing keys: {sorted(missing_prov)}")

    pinned = provenance.get("pinned", {}) or {}
    missing_pinned = REQUIRED_PINNED_KEYS - {k for k, v in pinned.items() if v}
    if missing_pinned:
        _fail(errors, f"manifest provenance.pinned missing/empty keys: {sorted(missing_pinned)}")

    task_labels = set()
    for task in manifest.get("tasks", []):
        label = task.get("label")
        if not label:
            _fail(errors, f"task entry missing label: {task}")
            continue
        if not task.get("class"):
            _fail(errors, f"task {label!r} missing class")
        if not task.get("resource_key"):
            _fail(errors, f"task {label!r} missing resource_key")
        task_labels.add(label)

    quanta_file = manifest.get("quanta_file")
    edges_file = manifest.get("edges_file")
    if not quanta_file:
        _fail(errors, "manifest missing quanta_file")
        return errors
    if not edges_file:
        _fail(errors, "manifest missing edges_file")
        return errors

    quanta_path = os.path.join(bundle_dir, quanta_file)
    edges_path = os.path.join(bundle_dir, edges_file)
    if not os.path.isfile(quanta_path):
        _fail(errors, f"referenced quanta_file does not exist: {quanta_path}")
        return errors
    if not os.path.isfile(edges_path):
        _fail(errors, f"referenced edges_file does not exist: {edges_path}")
        return errors

    qids = set()
    n_quanta = 0
    with open(quanta_path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                _fail(errors, f"quanta.jsonl:{lineno} invalid JSON: {e}")
                continue
            n_quanta += 1

            if rec.get("record_type") != "quantum":
                _fail(errors, f"quanta.jsonl:{lineno} record_type != 'quantum': {rec.get('record_type')!r}")

            qid = rec.get("qid")
            if not isinstance(qid, int) or isinstance(qid, bool):
                _fail(errors, f"quanta.jsonl:{lineno} qid is not an int: {qid!r}")
            elif qid in qids:
                _fail(errors, f"quanta.jsonl:{lineno} duplicate qid {qid}")
            else:
                qids.add(qid)

            task = rec.get("task")
            if not task:
                _fail(errors, f"quanta.jsonl:{lineno} missing task label")
            elif task_labels and task not in task_labels:
                _fail(errors, f"quanta.jsonl:{lineno} task {task!r} not declared in manifest tasks")

            if not isinstance(rec.get("data_id"), dict):
                _fail(errors, f"quanta.jsonl:{lineno} data_id missing or not an object")

            for direction in ("inputs", "outputs"):
                summaries = rec.get(direction)
                if not isinstance(summaries, list):
                    _fail(errors, f"quanta.jsonl:{lineno} {direction} missing or not a list")
                    continue
                for summary in summaries:
                    if "dataset_type" not in summary or "n" not in summary:
                        _fail(errors, f"quanta.jsonl:{lineno} {direction} summary missing dataset_type/n: {summary}")
                    if "bytes_est" not in summary:
                        _fail(errors, f"quanta.jsonl:{lineno} {direction} summary missing bytes_est field: {summary}")

    n_edges = 0
    seen_edge_keys = set()
    with open(edges_path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                _fail(errors, f"edges.jsonl:{lineno} invalid JSON: {e}")
                continue
            n_edges += 1

            if rec.get("record_type") != "edge":
                _fail(errors, f"edges.jsonl:{lineno} record_type != 'edge': {rec.get('record_type')!r}")

            p, c, dt = rec.get("producer_qid"), rec.get("consumer_qid"), rec.get("dataset_type")
            for name, val in (("producer_qid", p), ("consumer_qid", c)):
                if not isinstance(val, int) or isinstance(val, bool):
                    _fail(errors, f"edges.jsonl:{lineno} {name} is not an int: {val!r}")
            if not dt:
                _fail(errors, f"edges.jsonl:{lineno} missing dataset_type")

            if isinstance(p, int) and p not in qids:
                _fail(errors, f"edges.jsonl:{lineno} producer_qid {p} not present in quanta.jsonl")
            if isinstance(c, int) and c not in qids:
                _fail(errors, f"edges.jsonl:{lineno} consumer_qid {c} not present in quanta.jsonl")
            if isinstance(p, int) and isinstance(c, int) and p == c:
                _fail(errors, f"edges.jsonl:{lineno} self-edge on qid {p}")

            key = (p, c, dt)
            if key in seen_edge_keys:
                _fail(errors, f"edges.jsonl:{lineno} duplicate logical edge {key}")
            seen_edge_keys.add(key)

    counts = manifest.get("counts", {})
    if counts.get("quanta") is not None and counts["quanta"] != n_quanta:
        _fail(errors, f"manifest counts.quanta ({counts['quanta']}) != actual line count ({n_quanta})")
    if counts.get("edges") is not None and counts["edges"] != n_edges:
        _fail(errors, f"manifest counts.edges ({counts['edges']}) != actual line count ({n_edges})")

    return errors


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("manifest", help="path to qgraph_manifest.json")
    args = ap.parse_args(argv)

    errors = validate(args.manifest)
    if errors:
        print(f"FAIL: {len(errors)} issue(s)", file=sys.stderr)
        for e in errors[:50]:
            print(f"  - {e}", file=sys.stderr)
        if len(errors) > 50:
            print(f"  ... and {len(errors) - 50} more", file=sys.stderr)
        return 1
    print("OK: bundle is valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
