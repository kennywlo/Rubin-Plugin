#!/usr/bin/env python3
"""
Generate 3 example CGSim/Rubin-Plugin workload files (Rubin-Plugin jobs[] format,
same schema as ../real_quantum_graph.json) structurally derived from the real
LSSTCam DRP.yaml task graph, instead of an RC2/HSC QuantumGraph export.

Source of the DAG shape: ~/llm-apps/app/rubin_drp_pipeline/
  pipeline_drp_pipe_LSSTCam_DRP_stage{1,2,3}-*.pdf (pipetask build reports) and
  pipeline_drp_pipe_LSSTCam_DRP.yaml (the actual pipeline definition).

Each stage graph covers only the compute-critical spine of that stage (the chain
that actually processes images/catalogs) and deliberately excludes the parallel
QA/analysis-tools branch (metric tables, whole-sky plots, fgcm diagnostic plots) —
those fan out to dozens of near-zero-cost tasks per stage that don't meaningfully
change scheduling load.

ASSUMPTIONS (flagged explicitly, same spirit as ../../rubin-data/generate_campaign.py):
  - Campaign scale (2 tracts x 4 patches x 6 visits/tract x 9 detectors x 3 bands)
    is a structural stand-in sized for a readable example, not a DRP1-scale run.
  - flops are order-of-magnitude estimates: task types that also appear in
    ../real_quantum_graph.json (isr, calibrateImage) reuse its measured averages;
    everything else is a reasoned guess, NOT a measurement. Do not treat these
    numbers as real per-task costs.
  - cores are 1 almost everywhere, matching the real corecount distribution found
    in panda_prod_test OpenSearch data (avg ~1.0 across all 4 Rubin PanDA
    facilities); a few global/fan-in-heavy tasks (fgcmFitCycle, deblendCoaddFootprints)
    are bumped to reflect the max-corecount tail seen in that same data.
  - gbdesHealpix3AstrometricFit is modeled per-tract as a stand-in for the real
    healpix3 pixelization, which this generator does not implement.
  - Each stage file is self-contained: inputs that would come from a prior stage
    (or from calibs/skymap) are treated as pre-existing external files available
    at creation_time=0, the same pattern generate_campaign.py uses for raw inputs.
"""

import json
import os

INSTRUMENT = "LSSTCam"
N_TRACTS = 2
PATCHES_PER_TRACT = 4
VISITS_PER_TRACT = 6
DETECTORS_PER_VISIT = 9
BANDS = ["g", "r", "i"]

TRACTS = list(range(N_TRACTS))
PATCHES = list(range(PATCHES_PER_TRACT))
VISITS_OF = {t: list(range(t * VISITS_PER_TRACT, (t + 1) * VISITS_PER_TRACT)) for t in TRACTS}
VISIT_TRACT = {v: t for t, vs in VISITS_OF.items() for v in vs}
VISIT_BAND = {v: BANDS[v % len(BANDS)] for vs in VISITS_OF.values() for v in vs}
DETECTORS = list(range(DETECTORS_PER_VISIT))

# rough per-copy sizes in bytes; same ballpark as real_quantum_graph.json / generate_campaign.py
SIZES = {
    "raw": 18_000_000, "calib": 2_000_000,
    "postISRCCD": 48_000_000, "calexp": 55_000_000, "preSource": 6_000_000,
    "single_visit_star": 4_000_000, "visit_summary": 500_000,
    "preliminary_visit_image": 55_000_000,
    "refit_psf_models": 3_000_000, "recalibrated_star": 5_000_000,
    "fgcm_star_obs": 200_000_000, "fgcm_standard_star": 50_000_000,
    "gbdes_fit": 20_000_000,
    "direct_warp": 60_000_000, "psf_matched_warp": 60_000_000,
    "deep_coadd": 400_000_000, "object_detection": 10_000_000,
    "object_detection_merged": 15_000_000, "deconvolved_coadd": 400_000_000,
    "object_deblend": 30_000_000, "object_measurement": 20_000_000,
    "object_ref_measurement": 25_000_000, "object_forced_measurement": 20_000_000,
    "object_patch": 15_000_000, "object": 40_000_000, "object_parent": 40_000_000,
}


class Builder:
    def __init__(self):
        self.jobs = []
        self.qid = 0
        self.by_key = {}  # arbitrary key -> jobid, for wiring dependencies

    def add(self, key, task, data_id, flops, cores=1, creation_time=None,
            parents=(), extra_inputs=None):
        self.qid += 1
        jid = self.qid
        input_files, input_file_sizes = {}, {}
        if extra_inputs:
            for name, size in extra_inputs:
                input_files[name] = None
                input_file_sizes[name] = size
        parent_jids = []
        for pkey, dstype in parents:
            pjid = self.by_key[pkey]
            parent_jids.append(pjid)
            fname = f"{dstype}_q{pjid}.dat"
            input_files[fname] = None
            input_file_sizes[fname] = SIZES.get(dstype, 10_000_000)
        job = {
            "jobid": jid, "task": task, "resource_key": f"{task}:{INSTRUMENT}",
            "data_id": {"instrument": INSTRUMENT, **data_id},
            "cores": cores, "flops": flops, "creation_time": creation_time,
            "input_files": list(input_files.keys()),
            "input_file_sizes": input_file_sizes,
            "output_files": {},
            "parents": parent_jids, "children": [],
        }
        self.jobs.append(job)
        self.by_key[key] = jid
        return jid

    def set_output(self, key, dstype, size=None):
        jid = self.by_key[key]
        job = self.jobs[jid - 1]
        fname = f"{dstype}_q{jid}.dat"
        job["output_files"][fname] = size if size is not None else SIZES.get(dstype, 10_000_000)

    def link_children(self):
        by_id = {j["jobid"]: j for j in self.jobs}
        for j in self.jobs:
            for pjid in j["parents"]:
                dstype = next(iter(j["output_files"]), "unknown").rsplit("_q", 1)[0] \
                    if False else None
            # derive dataset_type each parent produced that this job consumes,
            # by matching input filenames against the parent's output filenames
            parent_outputs = {}
            for pjid in j["parents"]:
                for fname, size in by_id[pjid]["output_files"].items():
                    parent_outputs[fname] = pjid
            for fname in j["input_files"]:
                if fname in parent_outputs:
                    pjid = parent_outputs[fname]
                    dstype = fname.rsplit("_q", 1)[0]
                    by_id[pjid]["children"].append(
                        {"jobid": j["jobid"], "creation_delay": 0, "dataset_type": dstype})


def build_stage1():
    b = Builder()
    for t in TRACTS:
        for v in VISITS_OF[t]:
            band = VISIT_BAND[v]
            for d in DETECTORS:
                data_id = {"detector": d, "visit": v, "tract": t, "band": band}
                b.add(("isr", v, d), "isr", data_id, flops=12_145_000_000, creation_time=0.0,
                      extra_inputs=[("raw_exp{}_det{}.fits".format(v, d), SIZES["raw"]),
                                    ("calib_bias_det{}.dat".format(d), SIZES["calib"]),
                                    ("calib_flat_det{}.dat".format(d), SIZES["calib"])])
                b.set_output(("isr", v, d), "postISRCCD")

                b.add(("calibrateImage", v, d), "calibrateImage", data_id, flops=41_016_000_000,
                      parents=[(("isr", v, d), "postISRCCD")])
                b.set_output(("calibrateImage", v, d), "calexp")

                b.add(("standardize", v, d), "standardizeSingleVisitStar", data_id, flops=6_000_000_000,
                      parents=[(("calibrateImage", v, d), "calexp")])
                b.set_output(("standardize", v, d), "single_visit_star")

        for v in VISITS_OF[t]:
            band = VISIT_BAND[v]
            b.add(("consolidateVisit", v), "consolidateSingleVisitStar",
                  {"visit": v, "tract": t, "band": band}, flops=3_000_000_000,
                  parents=[(("standardize", v, d), "single_visit_star") for d in DETECTORS])
            b.set_output(("consolidateVisit", v), "single_visit_star_visit")

    all_visits = [v for t in TRACTS for v in VISITS_OF[t]]
    b.add(("visitTable",), "makeInitialVisitTable", {}, flops=1_000_000_000,
          parents=[(("consolidateVisit", v), "single_visit_star_visit") for v in all_visits])
    b.set_output(("visitTable",), "preliminary_visit_table")

    b.add(("visitDetectorTable",), "makeInitialVisitDetectorTable", {}, flops=1_000_000_000,
          parents=[(("consolidateVisit", v), "single_visit_star_visit") for v in all_visits])
    b.set_output(("visitDetectorTable",), "preliminary_visit_detector_table")

    for t in TRACTS:
        b.add(("isolatedStar", t), "associateIsolatedStar", {"tract": t}, flops=15_000_000_000,
              parents=[(("consolidateVisit", v), "single_visit_star_visit") for v in VISITS_OF[t]])
        b.set_output(("isolatedStar", t), "isolated_star_association")

    b.link_children()
    return b.jobs


def build_stage2():
    b = Builder()
    all_visits = [v for t in TRACTS for v in VISITS_OF[t]]
    for v in all_visits:
        t = VISIT_TRACT[v]
        band = VISIT_BAND[v]
        data_id_v = {"visit": v, "tract": t, "band": band}
        b.add(("updateVisitSummary", v), "updateVisitSummary", data_id_v, flops=2_000_000_000,
              creation_time=0.0,
              extra_inputs=[(f"visit_summary_ext_{v}.dat", SIZES["visit_summary"])])
        b.set_output(("updateVisitSummary", v), "visit_summary")

        for d in DETECTORS:
            data_id = {**data_id_v, "detector": d}
            b.add(("refitPsf", v, d), "finalizeCharacterizationDetector", data_id, flops=9_000_000_000,
                  parents=[(("updateVisitSummary", v), "visit_summary")],
                  extra_inputs=[(f"preliminary_visit_image_ext_{v}_{d}.dat",
                                 SIZES["preliminary_visit_image"])])
            b.set_output(("refitPsf", v, d), "refit_psf_models")

            b.add(("standardizeRecal", v, d), "transformSourceTable", data_id, flops=5_000_000_000,
                  parents=[(("refitPsf", v, d), "refit_psf_models")])
            b.set_output(("standardizeRecal", v, d), "recalibrated_star")

        b.add(("consolidateRefit", v), "consolidateFinalizeCharacterization", data_id_v, flops=2_500_000_000,
              parents=[(("refitPsf", v, d), "refit_psf_models") for d in DETECTORS])
        b.set_output(("consolidateRefit", v), "refit_psf_models_visit")

        b.add(("consolidateRecal", v), "consolidateVisitSummary", data_id_v, flops=2_500_000_000,
              parents=[(("standardizeRecal", v, d), "recalibrated_star") for d in DETECTORS])
        b.set_output(("consolidateRecal", v), "recalibrated_star_visit")

        b.add(("writeRecal", v), "writeRecalibratedSourceTable", data_id_v, flops=3_000_000_000,
              parents=[(("consolidateRefit", v), "refit_psf_models_visit"),
                       (("consolidateRecal", v), "recalibrated_star_visit")])
        b.set_output(("writeRecal", v), "recalibrated_star_final")

    b.add(("fgcmBuild",), "fgcmBuildFromIsolatedStars", {}, flops=30_000_000_000,
          parents=[(("consolidateRecal", v), "recalibrated_star_visit") for v in all_visits])
    b.set_output(("fgcmBuild",), "fgcm_star_obs", SIZES["fgcm_star_obs"])

    prev = ("fgcmBuild",)
    prev_dstype = "fgcm_star_obs"
    for cycle in range(3):
        key = ("fgcmFitCycle", cycle)
        b.add(key, "fgcmFitCycle", {"cycle": cycle}, flops=80_000_000_000, cores=4,
              parents=[(prev, prev_dstype)])
        b.set_output(key, f"fgcm_cycle{cycle}")
        prev, prev_dstype = key, f"fgcm_cycle{cycle}"

    b.add(("fgcmOutput",), "fgcmOutputProducts", {}, flops=10_000_000_000,
          parents=[(prev, prev_dstype)])
    b.set_output(("fgcmOutput",), "fgcm_standard_star")

    for t in TRACTS:
        for band in BANDS:
            b.add(("gbdes", t, band), "gbdesAstrometricFit", {"tract": t, "band": band},
                  flops=20_000_000_000,
                  parents=[(("updateVisitSummary", v), "visit_summary")
                           for v in VISITS_OF[t] if VISIT_BAND[v] == band])
            b.set_output(("gbdes", t, band), "gbdes_fit")

    b.link_children()
    return b.jobs


def build_stage3():
    b = Builder()
    for t in TRACTS:
        for p in PATCHES:
            for band in BANDS:
                visits_bp = [v for v in VISITS_OF[t] if VISIT_BAND[v] == band]
                data_id_bp = {"tract": t, "patch": p, "band": band}
                b.add(("selectVisits", t, p, band), "selectDeepCoaddVisits", data_id_bp,
                      flops=1_000_000_000, creation_time=0.0,
                      extra_inputs=[(f"recalibrated_star_ext_{v}.dat", SIZES["recalibrated_star"])
                                    for v in visits_bp])
                b.set_output(("selectVisits", t, p, band), "deep_coadd_visit_selection")

                for v in visits_bp:
                    data_id_pv = {"tract": t, "patch": p, "visit": v, "band": band}
                    b.add(("directWarp", t, p, v), "makeDirectWarp", data_id_pv, flops=25_000_000_000,
                          parents=[(("selectVisits", t, p, band), "deep_coadd_visit_selection")],
                          extra_inputs=[(f"preliminary_visit_image_ext_{v}_{p}.dat",
                                         SIZES["preliminary_visit_image"])])
                    b.set_output(("directWarp", t, p, v), "direct_warp")

                    b.add(("psfWarp", t, p, v), "makePsfMatchedWarp", data_id_pv, flops=25_000_000_000,
                          parents=[(("selectVisits", t, p, band), "deep_coadd_visit_selection")],
                          extra_inputs=[(f"preliminary_visit_image_ext_{v}_{p}.dat",
                                         SIZES["preliminary_visit_image"])])
                    b.set_output(("psfWarp", t, p, v), "psf_matched_warp")

                b.add(("assembleCoadd", t, p, band), "assembleDeepCoadd", data_id_bp,
                      flops=60_000_000_000, cores=2,
                      parents=[(("directWarp", t, p, v), "direct_warp") for v in visits_bp] +
                              [(("psfWarp", t, p, v), "psf_matched_warp") for v in visits_bp])
                b.set_output(("assembleCoadd", t, p, band), "deep_coadd")

                b.add(("detectPeaks", t, p, band), "detection", data_id_bp, flops=8_000_000_000,
                      parents=[(("assembleCoadd", t, p, band), "deep_coadd")])
                b.set_output(("detectPeaks", t, p, band), "object_detection")

            b.add(("mergeDetections", t, p), "mergeDetections", {"tract": t, "patch": p},
                  flops=5_000_000_000,
                  parents=[(("detectPeaks", t, p, band), "object_detection") for band in BANDS])
            b.set_output(("mergeDetections", t, p), "object_detection_merged")

            for band in BANDS:
                data_id_bp = {"tract": t, "patch": p, "band": band}
                b.add(("deconvolve", t, p, band), "deconvolve", data_id_bp, flops=15_000_000_000,
                      parents=[(("assembleCoadd", t, p, band), "deep_coadd"),
                               (("mergeDetections", t, p), "object_detection_merged")])
                b.set_output(("deconvolve", t, p, band), "deconvolved_coadd")

            b.add(("deblend", t, p), "deblend", {"tract": t, "patch": p}, flops=40_000_000_000, cores=2,
                  parents=[(("deconvolve", t, p, band), "deconvolved_coadd") for band in BANDS] +
                          [(("mergeDetections", t, p), "object_detection_merged")])
            b.set_output(("deblend", t, p), "object_deblend")

            for band in BANDS:
                data_id_bp = {"tract": t, "patch": p, "band": band}
                b.add(("measureUnforced", t, p, band), "measure", data_id_bp, flops=12_000_000_000,
                      parents=[(("deblend", t, p), "object_deblend"),
                               (("assembleCoadd", t, p, band), "deep_coadd")])
                b.set_output(("measureUnforced", t, p, band), "object_measurement")

            b.add(("mergeMeasurements", t, p), "mergeMeasurements", {"tract": t, "patch": p},
                  flops=5_000_000_000,
                  parents=[(("measureUnforced", t, p, band), "object_measurement") for band in BANDS])
            b.set_output(("mergeMeasurements", t, p), "object_ref_measurement")

            for band in BANDS:
                data_id_bp = {"tract": t, "patch": p, "band": band}
                b.add(("measureForced", t, p, band), "forcedPhotCoadd", data_id_bp, flops=10_000_000_000,
                      parents=[(("mergeMeasurements", t, p), "object_ref_measurement"),
                               (("assembleCoadd", t, p, band), "deep_coadd")])
                b.set_output(("measureForced", t, p, band), "object_forced_measurement")

            b.add(("standardizeObject", t, p), "transformObjectTable", {"tract": t, "patch": p},
                  flops=4_000_000_000,
                  parents=[(("measureForced", t, p, band), "object_forced_measurement") for band in BANDS])
            b.set_output(("standardizeObject", t, p), "object_patch")

        b.add(("consolidateObject", t), "consolidateObjectTable", {"tract": t}, flops=6_000_000_000,
              parents=[(("standardizeObject", t, p), "object_patch") for p in PATCHES])
        b.set_output(("consolidateObject", t), "object")

        b.add(("consolidateParent", t), "consolidateParentTable", {"tract": t}, flops=3_000_000_000,
              parents=[(("consolidateObject", t), "object")])
        b.set_output(("consolidateParent", t), "object_parent")

    b.link_children()
    return b.jobs


def main():
    out_dir = os.path.dirname(os.path.abspath(__file__))
    for name, builder in [("example_stage1_single_visit.json", build_stage1),
                          ("example_stage2_recalibrate.json", build_stage2),
                          ("example_stage3_coadd.json", build_stage3)]:
        jobs = builder()
        path = os.path.join(out_dir, name)
        with open(path, "w") as f:
            json.dump({"jobs": jobs}, f, indent=2)
        print(f"wrote {path}: {len(jobs)} jobs")


if __name__ == "__main__":
    main()
