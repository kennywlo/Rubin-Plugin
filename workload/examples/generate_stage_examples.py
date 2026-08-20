#!/usr/bin/env python3
"""
Generate 3 example CGSim/Rubin-Plugin workload files (Rubin-Plugin jobs[] format,
same schema as ../real_quantum_graph.json) structurally derived from the real
LSSTCam DRP.yaml task graph, instead of an RC2/HSC QuantumGraph export.

Source of the DAG shape: ~/llm-apps/app/rubin_drp_pipeline/
  pipeline_drp_pipe_LSSTCam_DRP_stage{1,2,3}-*.pdf (pipetask build reports) and
  pipeline_drp_pipe_LSSTCam_DRP.yaml (the actual pipeline definition).

Each stage graph now covers BOTH the compute-critical spine (the chain that
actually processes images/catalogs) AND the QA/analysis-tools branch (metric
tables, whole-sky plots, fgcm diagnostic plots, property maps, RGB/HiPS image
products) for maximum structural realism. QA tasks are modeled as one job per
task invocation (matching the real pipeline's granularity), with that task's many
named plot/metric datasets bundled into a couple of representative output_files
entries rather than exploded one-per-dataset-name — a real invocation of e.g.
makeAnalysisSingleVisitStarAssociationWholeSkyPlot produces ~22 plot datasets in
ONE task run, not 22 separate jobs.

A 4th stage is included: stage4 (step4b-4f in the source PDFs) covers solar-system
ephemerides generation, DIA/variability source association, forced photometry on
detector-level difference/visit images, the "pretty picture" RGB coadd pipeline,
and HiPS tile generation from those RGB images. There is no aggregate "stage4"
PDF in the source directory (WHERE_IS_STEP4A_OR_STAGE4.html is a redirect stub,
and there is no step4a) — this stage was pieced together directly from
step4b-step4f.

ASSUMPTIONS (flagged explicitly, same spirit as ../../rubin-data/generate_campaign.py):
  - Campaign scale (2 tracts x 4 patches x 6 visits/tract x 9 detectors x 3 bands)
    is a structural stand-in sized for a readable example, not a DRP1-scale run.
  - flops are order-of-magnitude estimates: task types that also appear in
    ../real_quantum_graph.json (isr, calibrateImage) reuse its measured averages;
    everything else is a reasoned guess, NOT a measurement. Do not treat these
    numbers as real per-task costs. QA/plot-producing tasks are given deliberately
    low flops relative to compute tasks, since they aggregate/plot existing data
    rather than reprocess images.
  - cores are 1 almost everywhere, matching the real corecount distribution found
    in panda_prod_test OpenSearch data (avg ~1.0 across all 4 Rubin PanDA
    facilities); a few global/fan-in-heavy tasks (fgcmFitCycle, deblendCoaddFootprints)
    are bumped to reflect the max-corecount tail seen in that same data.
  - gbdesHealpix3AstrometricFit, and the stage3/stage4 HiPS tasks
    (makeHighOrderHips*/makeLowOrderHips*), are modeled per-tract or per-band as a
    stand-in for the real healpix3/healpix8 pixelization, which this generator
    does not implement pixel-accurately.
  - Each stage file is self-contained: inputs that would come from a prior stage
    (or from calibs/skymap) are treated as pre-existing external files available
    at creation_time=0, the same pattern generate_campaign.py uses for raw inputs.
  - stage4's forced-photometry and DIA-source tasks assume a "difference_image"
    and "visit_image" already exist (produced by AP/prompt processing, which this
    generator does not model) and treats them as external t=0 inputs.
  - Template-coadd-specific QA (a near-duplicate of deep-coadd QA in step3a/3b) is
    NOT duplicated in stage3 — only the deep-coadd side is modeled, to avoid
    doubling an already-large QA branch with structurally identical tasks.
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
    "qa_metrics": 500_000, "qa_metrics_table": 2_000_000, "qa_plot_bundle": 30_000_000,
    "healsparse_map": 20_000_000, "healsparse_consolidated": 40_000_000,
    "coadd_input_summary": 5_000_000,
    "binned_image": 15_000_000, "whole_tract_image": 60_000_000,
    "rgb_gray_bundle": 80_000_000, "hips_tile": 5_000_000, "hips_allsky": 40_000_000,
    "ephemerides": 30_000_000, "ss_tables": 100_000_000,
    "forced_source": 8_000_000, "dia_source": 6_000_000, "dia_object": 10_000_000,
    "pretty_warp": 60_000_000, "pretty_coadd": 400_000_000, "pretty_rgb": 80_000_000,
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

    # QA/analysis-tools branch (step1c, step1d)
    for t in TRACTS:
        b.add(("analyzeAssoc", t), "analyzeSingleVisitStarAssociation", {"tract": t}, flops=2_000_000_000,
              parents=[(("isolatedStar", t), "isolated_star_association")])
        b.set_output(("analyzeAssoc", t), "qa_metrics")
        b.set_output(("analyzeAssoc", t), "qa_plot_bundle")

    b.add(("assocMetricTable",), "makeAnalysisSingleVisitStarAssociationMetricTable", {}, flops=200_000_000,
          parents=[(("analyzeAssoc", t), "qa_metrics") for t in TRACTS])
    b.set_output(("assocMetricTable",), "qa_metrics_table")

    b.add(("assocWholeSkyPlot",), "makeAnalysisSingleVisitStarAssociationWholeSkyPlot", {}, flops=500_000_000,
          parents=[(("assocMetricTable",), "qa_metrics_table")])
    b.set_output(("assocWholeSkyPlot",), "qa_plot_bundle")

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
        b.set_output(key, "qa_plot_bundle")  # real task also emits ~150 fgcm diagnostic plots
        prev, prev_dstype = key, f"fgcm_cycle{cycle}"

    b.add(("fgcmOutput",), "fgcmOutputProducts", {}, flops=10_000_000_000,
          parents=[(prev, prev_dstype)])
    b.set_output(("fgcmOutput",), "fgcm_standard_star")
    b.set_output(("fgcmOutput",), "qa_plot_bundle")

    for t in TRACTS:
        for band in BANDS:
            b.add(("gbdes", t, band), "gbdesAstrometricFit", {"tract": t, "band": band},
                  flops=20_000_000_000,
                  parents=[(("updateVisitSummary", v), "visit_summary")
                           for v in VISITS_OF[t] if VISIT_BAND[v] == band])
            b.set_output(("gbdes", t, band), "gbdes_fit")

    # QA/analysis-tools branch (step2d, step2e, step2f)
    for v in all_visits:
        b.add(("refMatch", v), "makeAnalysisRecalibratedStarAstrometricRefMatch",
              {"visit": v, "tract": VISIT_TRACT[v]}, flops=500_000_000,
              parents=[(("writeRecal", v), "recalibrated_star_final")])
        b.set_output(("refMatch", v), "ref_match_astrom")

        b.add(("analyzeRefMatch", v), "analyzeRecalibratedStarAstrometricRefMatch",
              {"visit": v, "tract": VISIT_TRACT[v]}, flops=500_000_000,
              parents=[(("refMatch", v), "ref_match_astrom")])
        b.set_output(("analyzeRefMatch", v), "qa_metrics")

    for t in TRACTS:
        b.add(("fitStellarMotion", t), "fitStellarMotion", {"tract": t}, flops=8_000_000_000,
              parents=[(("writeRecal", v), "recalibrated_star_final") for v in VISITS_OF[t]])
        b.set_output(("fitStellarMotion", t), "stellar_motions")

        b.add(("analyzeRecalAssoc", t), "analyzeRecalibratedStarAssociation", {"tract": t},
              flops=2_000_000_000,
              parents=[(("writeRecal", v), "recalibrated_star_final") for v in VISITS_OF[t]])
        b.set_output(("analyzeRecalAssoc", t), "qa_metrics")
        b.set_output(("analyzeRecalAssoc", t), "qa_plot_bundle")

    b.add(("recalMetricTable",), "makeAnalysisRecalibratedStarAssociationMetricTable", {},
          flops=200_000_000,
          parents=[(("analyzeRecalAssoc", t), "qa_metrics") for t in TRACTS])
    b.set_output(("recalMetricTable",), "qa_metrics_table")

    b.add(("recalWholeSkyPlot",), "makeAnalysisRecalibratedStarAssociationWholeSkyPlot", {},
          flops=500_000_000, parents=[(("recalMetricTable",), "qa_metrics_table")])
    b.set_output(("recalWholeSkyPlot",), "qa_plot_bundle")

    b.add(("visitDetectorTable2",), "makeVisitDetectorTable", {}, flops=1_000_000_000,
          parents=[(("updateVisitSummary", v), "visit_summary") for v in all_visits])
    b.set_output(("visitDetectorTable2",), "visit_detector_table")

    b.add(("visitTable2",), "makeVisitTable", {}, flops=1_000_000_000,
          parents=[(("updateVisitSummary", v), "visit_summary") for v in all_visits])
    b.set_output(("visitTable2",), "visit_table")

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

    # healSparse property maps + coadd input summaries (compute-critical, step3a)
    for t in TRACTS:
        for band in BANDS:
            b.add(("healSparse", t, band), "makeHealSparsePropertyMaps", {"tract": t, "band": band},
                  flops=4_000_000_000,
                  parents=[(("assembleCoadd", t, p, band), "deep_coadd") for p in PATCHES])
            b.set_output(("healSparse", t, band), "healsparse_map")

            b.add(("inputSummaryTract", t, band), "makeDeepCoaddInputSummaryTract",
                  {"tract": t, "band": band}, flops=500_000_000,
                  parents=[(("selectVisits", t, p, band), "deep_coadd_visit_selection")
                           for p in PATCHES])
            b.set_output(("inputSummaryTract", t, band), "coadd_input_summary")

    b.add(("inputSummary",), "makeDeepCoaddInputSummary", {}, flops=500_000_000,
          parents=[(("inputSummaryTract", t, band), "coadd_input_summary")
                   for t in TRACTS for band in BANDS])
    b.set_output(("inputSummary",), "coadd_input_summary")

    for band in BANDS:
        b.add(("consolidateHealSparse", band), "consolidateHealSparsePropertyMaps", {"band": band},
              flops=2_000_000_000,
              parents=[(("healSparse", t, band), "healsparse_map") for t in TRACTS])
        b.set_output(("consolidateHealSparse", band), "healsparse_consolidated")

    # QA/analysis-tools branch (step3a tract-level, step3b global, step3c/3d RGB+HiPS)
    for t in TRACTS:
        b.add(("analyzeObjectCore", t), "analyzeObjectTableCore", {"tract": t}, flops=2_000_000_000,
              parents=[(("consolidateObject", t), "object")])
        b.set_output(("analyzeObjectCore", t), "qa_metrics")
        b.set_output(("analyzeObjectCore", t), "qa_plot_bundle")

        b.add(("analyzeObjectParentCore", t), "analyzeObjectParentTableCore", {"tract": t},
              flops=2_000_000_000, parents=[(("consolidateParent", t), "object_parent")])
        b.set_output(("analyzeObjectParentCore", t), "qa_metrics")

        b.add(("refCatObjectTract", t), "refCatObjectTract", {"tract": t}, flops=1_500_000_000,
              parents=[(("consolidateObject", t), "object")])
        b.set_output(("refCatObjectTract", t), "qa_metrics")

        b.add(("validateObjectCore", t), "validateObjectTableCore", {"tract": t}, flops=1_000_000_000,
              parents=[(("consolidateObject", t), "object")])
        b.set_output(("validateObjectCore", t), "qa_metrics")

        for band in BANDS:
            b.add(("propertyMapTract", t, band), "plotPropertyMapTract", {"tract": t, "band": band},
                  flops=800_000_000, parents=[(("healSparse", t, band), "healsparse_map")])
            b.set_output(("propertyMapTract", t, band), "qa_plot_bundle")

            for p in PATCHES:
                b.add(("binnedImage", t, p, band), "makeBinnedDeepCoaddImage",
                      {"tract": t, "patch": p, "band": band}, flops=600_000_000,
                      parents=[(("assembleCoadd", t, p, band), "deep_coadd")])
                b.set_output(("binnedImage", t, p, band), "binned_image")

            b.add(("wholeTractImage", t, band), "makeWholeTractDeepCoaddImage",
                  {"tract": t, "band": band}, flops=2_000_000_000,
                  parents=[(("binnedImage", t, p, band), "binned_image") for p in PATCHES])
            b.set_output(("wholeTractImage", t, band), "whole_tract_image")

            b.add(("maskFractions", t, band), "aggregateDeepCoaddMaskFractions",
                  {"tract": t, "band": band}, flops=500_000_000,
                  parents=[(("wholeTractImage", t, band), "whole_tract_image")])
            b.set_output(("maskFractions", t, band), "qa_metrics")

    b.add(("surveyCore",), "analyzeObjectTableSurveyCore", {}, flops=3_000_000_000,
          parents=[(("consolidateObject", t), "object") for t in TRACTS])
    b.set_output(("surveyCore",), "qa_plot_bundle")

    b.add(("objectMetricTable",), "makeMetricTableObjectTableCore", {}, flops=300_000_000,
          parents=[(("analyzeObjectCore", t), "qa_metrics") for t in TRACTS])
    b.set_output(("objectMetricTable",), "qa_metrics_table")
    b.add(("objectWholeSkyPlot",), "objectTableCoreWholeSkyPlot", {}, flops=500_000_000,
          parents=[(("objectMetricTable",), "qa_metrics_table")])
    b.set_output(("objectWholeSkyPlot",), "qa_plot_bundle")

    b.add(("objectParentMetricTable",), "makeMetricTableObjectParentTableCore", {}, flops=300_000_000,
          parents=[(("analyzeObjectParentCore", t), "qa_metrics") for t in TRACTS])
    b.set_output(("objectParentMetricTable",), "qa_metrics_table")
    b.add(("objectParentWholeSkyPlot",), "objectParentTableCoreWholeSkyPlot", {}, flops=500_000_000,
          parents=[(("objectParentMetricTable",), "qa_metrics_table")])
    b.set_output(("objectParentWholeSkyPlot",), "qa_plot_bundle")

    b.add(("refCatMatchMetricTable",), "makeMetricTableObjectTableCoreRefCatMatch", {},
          flops=300_000_000, parents=[(("refCatObjectTract", t), "qa_metrics") for t in TRACTS])
    b.set_output(("refCatMatchMetricTable",), "qa_metrics_table")
    b.add(("refCatMatchWholeSkyPlot",), "objectTableCoreRefCatMatchWholeSkyPlot", {},
          flops=500_000_000, parents=[(("refCatMatchMetricTable",), "qa_metrics_table")])
    b.set_output(("refCatMatchWholeSkyPlot",), "qa_plot_bundle")

    for band in BANDS:
        b.add(("aggMaskFracTable", band), "makeAggregatedDeepCoaddMaskFractionsTable", {"band": band},
              flops=300_000_000,
              parents=[(("maskFractions", t, band), "qa_metrics") for t in TRACTS])
        b.set_output(("aggMaskFracTable", band), "qa_metrics_table")
        b.add(("aggMaskFracPlot", band), "aggregatedDeepCoaddMaskFractionsWholeSkyPlot", {"band": band},
              flops=400_000_000, parents=[(("aggMaskFracTable", band), "qa_metrics_table")])
        b.set_output(("aggMaskFracPlot", band), "qa_plot_bundle")

        b.add(("propertyMapSurvey", band), "plotPropertyMapSurvey", {"band": band}, flops=800_000_000,
              parents=[(("consolidateHealSparse", band), "healsparse_consolidated")])
        b.set_output(("propertyMapSurvey", band), "qa_plot_bundle")

    # step3c: per-patch single-band RGB gray images (real pipeline has 6 separate
    # makeSingleBandRGBCoadd{Z,Y,U,R,I,G} tasks; collapsed to one job per patch
    # covering this generator's BANDS to avoid a 6x blowup for bands not modeled)
    for t in TRACTS:
        for p in PATCHES:
            b.add(("rgbGray", t, p), "makeSingleBandRGBCoadd", {"tract": t, "patch": p},
                  flops=3_000_000_000,
                  parents=[(("assembleCoadd", t, p, band), "deep_coadd") for band in BANDS])
            b.set_output(("rgbGray", t, p), "rgb_gray_bundle")

    # step3d: HiPS pyramid per band, per-patch gray images rolled up through
    # healpix8 -> healpix3 -> allsky info (modeled as one job per level per band,
    # fanning in over all patches, as a stand-in for real per-healpix-pixel tasks)
    for band in BANDS:
        b.add(("hipsHigh", band), f"makeHighOrderHips{band.upper()}", {"band": band}, flops=1_500_000_000,
              parents=[(("rgbGray", t, p), "rgb_gray_bundle") for t in TRACTS for p in PATCHES])
        b.set_output(("hipsHigh", band), "hips_tile")

        b.add(("hipsLow", band), f"makeLowOrderHips{band.upper()}", {"band": band}, flops=500_000_000,
              parents=[(("hipsHigh", band), "hips_tile")])
        b.set_output(("hipsLow", band), "hips_tile")

        b.add(("hipsAllSky", band), f"writeAllSkyHipsInfo{band.upper()}", {"band": band}, flops=100_000_000,
              parents=[(("hipsLow", band), "hips_tile")])
        b.set_output(("hipsAllSky", band), "hips_allsky")

    b.link_children()
    return b.jobs


def build_stage4():
    """
    Stage 4 (step4b-step4f): solar-system ephemerides, DIA/variability source
    association, forced photometry, the "pretty picture" RGB coadd pipeline, and
    HiPS tiles from those RGB images. Assumes stage1-3 products (visit_summary,
    preliminary_visit_table, recalibrated products) and AP/prompt-processing
    products (difference_image, visit_image) are pre-existing external inputs.
    """
    b = Builder()
    all_visits = [v for t in TRACTS for v in VISITS_OF[t]]

    b.add(("ephemerides",), "generateEphemerides", {}, flops=5_000_000_000, creation_time=0.0,
          extra_inputs=[("mpcorb_ext.dat", 50_000_000),
                        ("sspAux_ext.dat", 10_000_000)] +
                       [(f"visit_summary_ext_{v}.dat", SIZES["visit_summary"]) for v in all_visits])
    b.set_output(("ephemerides",), "ephemerides")

    # DIA/variability chain (step4c), per patch and per tract
    for t in TRACTS:
        for p in PATCHES:
            b.add(("associateDia", t, p), "associateDiaSource", {"tract": t, "patch": p},
                  flops=6_000_000_000, creation_time=0.0,
                  extra_inputs=[(f"difference_image_ext_{t}_{p}.dat", 55_000_000)])
            b.set_output(("associateDia", t, p), "dia_source")

            b.add(("calcDiaObject", t, p), "calculateDiaObject", {"tract": t, "patch": p},
                  flops=6_000_000_000, parents=[(("associateDia", t, p), "dia_source")])
            b.set_output(("calcDiaObject", t, p), "dia_object")

        b.add(("consolidateDiaSource", t), "consolidateDiaSource", {"tract": t}, flops=2_000_000_000,
              parents=[(("associateDia", t, p), "dia_source") for p in PATCHES])
        b.set_output(("consolidateDiaSource", t), "dia_source")

        b.add(("consolidateDiaObject", t), "consolidateDiaObject", {"tract": t}, flops=2_000_000_000,
              parents=[(("calcDiaObject", t, p), "dia_object") for p in PATCHES])
        b.set_output(("consolidateDiaObject", t), "dia_object")

        b.add(("analyzeDiaTract", t), "analyzeDiaSourceTableTract", {"tract": t}, flops=1_500_000_000,
              parents=[(("consolidateDiaSource", t), "dia_source")])
        b.set_output(("analyzeDiaTract", t), "qa_metrics")

        b.add(("associateAnalysisSource", t), "associateAnalysisSource", {"tract": t},
              flops=3_000_000_000, creation_time=0.0,
              extra_inputs=[(f"source_all_ext_{v}.dat", 4_000_000) for v in VISITS_OF[t]])
        b.set_output(("associateAnalysisSource", t), "qa_analysis_source")

        b.add(("analyzeSourceAssoc", t), "analyzeSourceAssociation", {"tract": t}, flops=2_000_000_000,
              parents=[(("associateAnalysisSource", t), "qa_analysis_source")])
        b.set_output(("analyzeSourceAssoc", t), "qa_metrics")
        b.set_output(("analyzeSourceAssoc", t), "qa_plot_bundle")

    # forced photometry on detector-level difference/visit images (per detector,visit)
    for v in all_visits:
        t = VISIT_TRACT[v]
        for d in DETECTORS:
            data_id = {"detector": d, "visit": v, "tract": t}
            b.add(("forcedObjDet", v, d), "forcedPhotObjectDetector", data_id, flops=4_000_000_000,
                  creation_time=0.0,
                  extra_inputs=[(f"visit_image_ext_{v}_{d}.dat", SIZES["preliminary_visit_image"])])
            b.set_output(("forcedObjDet", v, d), "forced_source")

            b.add(("forcedDiaObjDet", v, d), "forcedPhotDiaObjectDetector", data_id, flops=4_000_000_000,
                  creation_time=0.0,
                  extra_inputs=[(f"difference_image_ext_{v}_{d}.dat", SIZES["preliminary_visit_image"])])
            b.set_output(("forcedDiaObjDet", v, d), "forced_source")

    for t in TRACTS:
        vs = VISITS_OF[t]
        for p in PATCHES:
            b.add(("standardizeObjForced", t, p), "standardizeObjectForcedSource",
                  {"tract": t, "patch": p}, flops=3_000_000_000,
                  parents=[(("forcedObjDet", v, d), "forced_source") for v in vs for d in DETECTORS])
            b.set_output(("standardizeObjForced", t, p), "forced_source")

            b.add(("standardizeDiaObjForced", t, p), "standardizeDiaObjectForcedSource",
                  {"tract": t, "patch": p}, flops=3_000_000_000,
                  parents=[(("forcedDiaObjDet", v, d), "forced_source") for v in vs for d in DETECTORS])
            b.set_output(("standardizeDiaObjForced", t, p), "forced_source")

            b.add(("splitPrimaryForced", t, p), "splitPrimaryObjectForcedSource",
                  {"tract": t, "patch": p}, flops=1_000_000_000,
                  parents=[(("standardizeObjForced", t, p), "forced_source")])
            b.set_output(("splitPrimaryForced", t, p), "object_forced_source")

    b.add(("consolidateSs",), "consolidateSsTables", {}, flops=2_000_000_000, creation_time=0.0,
          extra_inputs=[("ss_source_associated_ext.dat", 20_000_000), ("mpcorb_ext2.dat", 50_000_000)])
    b.set_output(("consolidateSs",), "ss_tables")

    b.add(("assocMetricTable4",), "makeAnalysisSourceAssociationMetricTable", {}, flops=200_000_000,
          parents=[(("analyzeSourceAssoc", t), "qa_metrics") for t in TRACTS])
    b.set_output(("assocMetricTable4",), "qa_metrics_table")
    b.add(("assocWholeSkyPlot4",), "makeAnalysisSourceAssociationWholeSkyPlot", {}, flops=500_000_000,
          parents=[(("assocMetricTable4",), "qa_metrics_table")])
    b.set_output(("assocWholeSkyPlot4",), "qa_plot_bundle")

    # "pretty picture" RGB coadd pipeline (step4c warps/coadd, step4e fix+RGB, step4f HiPS)
    for t in TRACTS:
        for p in PATCHES:
            for band in BANDS:
                visits_bp = [v for v in VISITS_OF[t] if VISIT_BAND[v] == band]
                data_id_bp = {"tract": t, "patch": p, "band": band}
                for v in visits_bp:
                    data_id_pv = {"tract": t, "patch": p, "visit": v, "band": band}
                    b.add(("prettyDirectWarp", t, p, v), "makePrettyDirectWarp", data_id_pv,
                          flops=25_000_000_000, creation_time=0.0,
                          extra_inputs=[(f"preliminary_visit_image_ext_{v}_{p}.dat",
                                         SIZES["preliminary_visit_image"])])
                    b.set_output(("prettyDirectWarp", t, p, v), "pretty_warp")

                    b.add(("prettyPsfWarp", t, p, v), "makePrettyPsfMatchedWarp", data_id_pv,
                          flops=25_000_000_000, creation_time=0.0,
                          extra_inputs=[(f"preliminary_visit_image_ext_{v}_{p}.dat",
                                         SIZES["preliminary_visit_image"])])
                    b.set_output(("prettyPsfWarp", t, p, v), "pretty_warp")

                b.add(("assemblePretty", t, p, band), "assemblePrettyCoadd", data_id_bp,
                      flops=60_000_000_000, cores=2,
                      parents=[(("prettyDirectWarp", t, p, v), "pretty_warp") for v in visits_bp] +
                              [(("prettyPsfWarp", t, p, v), "pretty_warp") for v in visits_bp])
                b.set_output(("assemblePretty", t, p, band), "pretty_coadd")

                b.add(("binnedPretty", t, p, band), "makeBinnedPrettyCoaddImage", data_id_bp,
                      flops=600_000_000, parents=[(("assemblePretty", t, p, band), "pretty_coadd")])
                b.set_output(("binnedPretty", t, p, band), "binned_image")

                b.add(("fixBackground", t, p, band), "fixBackgroundPrettyCoadd", data_id_bp,
                      flops=3_000_000_000, parents=[(("assemblePretty", t, p, band), "pretty_coadd")])
                b.set_output(("fixBackground", t, p, band), "pretty_coadd")

            b.add(("wholeTractPretty", t, p), "makeWholeTractPrettyCoaddImage", {"tract": t, "patch": p},
                  flops=2_000_000_000,
                  parents=[(("binnedPretty", t, p, band), "binned_image") for band in BANDS])
            b.set_output(("wholeTractPretty", t, p), "whole_tract_image")

            b.add(("aggMaskFracPretty", t, p), "aggregatePrettyCoaddMaskFractions",
                  {"tract": t, "patch": p}, flops=500_000_000,
                  parents=[(("wholeTractPretty", t, p), "whole_tract_image")])
            b.set_output(("aggMaskFracPretty", t, p), "qa_metrics")

            b.add(("fixStars", t, p), "fixStarsPrettyCoadd", {"tract": t, "patch": p}, flops=3_000_000_000,
                  parents=[(("fixBackground", t, p, band), "pretty_coadd") for band in BANDS])
            b.set_output(("fixStars", t, p), "pretty_coadd")

            b.add(("rgbPicture", t, p), "makeRGBPicturePrettyCoaddGRI", {"tract": t, "patch": p},
                  flops=4_000_000_000, parents=[(("fixStars", t, p), "pretty_coadd")])
            b.set_output(("rgbPicture", t, p), "pretty_rgb")

    for label in ["GRI", "EpoGRI"]:
        b.add(("hipsHighPretty", label), f"makeHighOrderHips{label}", {}, flops=1_500_000_000,
              parents=[(("rgbPicture", t, p), "pretty_rgb") for t in TRACTS for p in PATCHES])
        b.set_output(("hipsHighPretty", label), "hips_tile")

        b.add(("hipsLowPretty", label), f"makeLowOrderHips{label}", {}, flops=500_000_000,
              parents=[(("hipsHighPretty", label), "hips_tile")])
        b.set_output(("hipsLowPretty", label), "hips_tile")

        b.add(("hipsAllSkyPretty", label), f"writeAllSkyHipsInfo{label}", {}, flops=100_000_000,
              parents=[(("hipsLowPretty", label), "hips_tile")])
        b.set_output(("hipsAllSkyPretty", label), "hips_allsky")

    b.link_children()
    return b.jobs


def main():
    out_dir = os.path.dirname(os.path.abspath(__file__))
    for name, builder in [("example_stage1_single_visit.json", build_stage1),
                          ("example_stage2_recalibrate.json", build_stage2),
                          ("example_stage3_coadd.json", build_stage3),
                          ("example_stage4_variability.json", build_stage4)]:
        jobs = builder()
        path = os.path.join(out_dir, name)
        with open(path, "w") as f:
            json.dump({"jobs": jobs}, f, indent=2)
        print(f"wrote {path}: {len(jobs)} jobs")


if __name__ == "__main__":
    main()
