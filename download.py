import logging
from pathlib import Path
import time
import argparse

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

bids_schema = {
    "_id": {"type": "string", "required": True},
    # BIDS identification bits
    "modality": {"type": "string", "required": True},
    "subject_id": {"type": "string", "required": True},
    "session_id": {"type": "string"},
    "run_id": {"type": "string"},
    "acq_id": {"type": "string"},
    "task_id": {"type": "string"},
    # BIDS metadata
    "AccelNumReferenceLines": {"type": "integer"},
    "AccelerationFactorPE": {"type": "integer"},
    "AcquisitionMatrix": {"type": "string"},
    "CogAtlasID": {"type": "string"},
    "CogPOID": {"type": "string"},
    "CoilCombinationMethod": {"type": "string"},
    "ContrastBolusIngredient": {"type": "string"},
    "ConversionSoftware": {"type": "string"},
    "ConversionSoftwareVersion": {"type": "string"},
    "DelayTime": {"type": "float"},
    "DeviceSerialNumber": {"type": "string"},
    "EchoTime": {"type": "float"},
    "EchoTrainLength": {"type": "integer"},
    "EffectiveEchoSpacing": {"type": "float"},
    "FlipAngle": {"type": "integer"},
    "GradientSetType": {"type": "string"},
    "HardcopyDeviceSoftwareVersion": {"type": "string"},
    "ImagingFrequency": {"type": "integer"},
    "InPlanePhaseEncodingDirection": {"type": "string"},
    "InstitutionAddress": {"type": "string"},
    "InstitutionName": {"type": "string"},
    "Instructions": {"type": "string"},
    "InversionTime": {"type": "float"},
    "MRAcquisitionType": {"type": "string"},
    "MRTransmitCoilSequence": {"type": "string"},
    "MagneticFieldStrength": {"type": "float"},
    "Manufacturer": {"type": "string"},
    "ManufacturersModelName": {"type": "string"},
    "MatrixCoilMode": {"type": "string"},
    "MultibandAccelerationFactor": {"type": "float"},
    "NumberOfAverages": {"type": "integer"},
    "NumberOfPhaseEncodingSteps": {"type": "integer"},
    "NumberOfVolumesDiscardedByScanner": {"type": "float"},
    "NumberOfVolumesDiscardedByUser": {"type": "float"},
    "NumberShots": {"type": "integer"},
    "ParallelAcquisitionTechnique": {"type": "string"},
    "ParallelReductionFactorInPlane": {"type": "float"},
    "PartialFourier": {"type": "boolean"},
    "PartialFourierDirection": {"type": "string"},
    "PatientPosition": {"type": "string"},
    "PercentPhaseFieldOfView": {"type": "integer"},
    "PercentSampling": {"type": "integer"},
    "PhaseEncodingDirection": {"type": "string"},
    "PixelBandwidth": {"type": "integer"},
    "ProtocolName": {"type": "string"},
    "PulseSequenceDetails": {"type": "string"},
    "PulseSequenceType": {"type": "string"},
    "ReceiveCoilName": {"type": "string"},
    "RepetitionTime": {"type": "float"},
    "ScanOptions": {"type": "string"},
    "ScanningSequence": {"type": "string"},
    "SequenceName": {"type": "string"},
    "SequenceVariant": {"type": "string"},
    "SliceEncodingDirection": {"type": "string"},
    "SoftwareVersions": {"type": "string"},
    "TaskDescription": {"type": "string"},
    "TotalReadoutTime": {"type": "float"},
    "TotalScanTimeSec": {"type": "integer"},
    "TransmitCoilName": {"type": "string"},
    "VariableFlipAngleFlag": {"type": "string"},
}

prov_schema = {
    "_id": {"type": "string", "required": True},
    "version": {"type": "string", "required": True},
    "md5sum": {"type": "string", "required": True},
    "software": {"type": "string", "required": True},
    "mriqc_pred": {"type": "integer"},
    "email": {"type": "string"},
}

settings_schema = {
    "_id": {"type": "string", "required": True},
    "fd_thres": {"type": "float"},
    "hmc_fsl": {"type": "boolean"},
    "testing": {"type": "boolean"},
}

bold_iqms_schema = {
    "_id": {"type": "string", "required": True},
    "aor": {"type": "float", "required": True},
    "aqi": {"type": "float", "required": True},
    "dummy_trs": {"type": "integer"},
    "dvars_nstd": {"type": "float", "required": True},
    "dvars_std": {"type": "float", "required": True},
    "dvars_vstd": {"type": "float", "required": True},
    "efc": {"type": "float", "required": True},
    "fber": {"type": "float", "required": True},
    "fd_mean": {"type": "float", "required": True},
    "fd_num": {"type": "float", "required": True},
    "fd_perc": {"type": "float", "required": True},
    "fwhm_avg": {"type": "float", "required": True},
    "fwhm_x": {"type": "float", "required": True},
    "fwhm_y": {"type": "float", "required": True},
    "fwhm_z": {"type": "float", "required": True},
    "gcor": {"type": "float", "required": True},
    "gsr_x": {"type": "float", "required": True},
    "gsr_y": {"type": "float", "required": True},
    "size_t": {"type": "float", "required": True},
    "size_x": {"type": "float", "required": True},
    "size_y": {"type": "float", "required": True},
    "size_z": {"type": "float", "required": True},
    "snr": {"type": "float", "required": True},
    "spacing_tr": {"type": "float", "required": True},
    "spacing_x": {"type": "float", "required": True},
    "spacing_y": {"type": "float", "required": True},
    "spacing_z": {"type": "float", "required": True},
    "summary_bg_k": {"type": "float", "required": True},
    "summary_bg_mean": {"type": "float", "required": True},
    "summary_bg_median": {"type": "float", "required": True},
    "summary_bg_mad": {"type": "float", "required": True},
    "summary_bg_p05": {"type": "float", "required": True},
    "summary_bg_p95": {"type": "float", "required": True},
    "summary_bg_stdv": {"type": "float", "required": True},
    "summary_bg_n": {"type": "float", "required": True},
    "summary_fg_k": {"type": "float", "required": True},
    "summary_fg_mean": {"type": "float", "required": True},
    "summary_fg_median": {"type": "float", "required": True},
    "summary_fg_mad": {"type": "float", "required": True},
    "summary_fg_p05": {"type": "float", "required": True},
    "summary_fg_p95": {"type": "float", "required": True},
    "summary_fg_stdv": {"type": "float", "required": True},
    "summary_fg_n": {"type": "float", "required": True},
    "tsnr": {"type": "float", "required": True},
}

struct_iqms_schema = {
    "_id": {"type": "string", "required": True},
    "cjv": {"type": "float", "required": True},
    "cnr": {"type": "float", "required": True},
    "efc": {"type": "float", "required": True},
    "fber": {"type": "float", "required": True},
    "fwhm_avg": {"type": "float", "required": True},
    "fwhm_x": {"type": "float", "required": True},
    "fwhm_y": {"type": "float", "required": True},
    "fwhm_z": {"type": "float", "required": True},
    "icvs_csf": {"type": "float", "required": True},
    "icvs_gm": {"type": "float", "required": True},
    "icvs_wm": {"type": "float", "required": True},
    "inu_med": {"type": "float", "required": True},
    "inu_range": {"type": "float", "required": True},
    "qi_1": {"type": "float", "required": True},
    "qi_2": {"type": "float", "required": True},
    "rpve_csf": {"type": "float", "required": True},
    "rpve_gm": {"type": "float", "required": True},
    "rpve_wm": {"type": "float", "required": True},
    "size_x": {"type": "integer", "required": True},
    "size_y": {"type": "integer", "required": True},
    "size_z": {"type": "integer", "required": True},
    "snr_csf": {"type": "float", "required": True},
    "snr_gm": {"type": "float", "required": True},
    "snr_total": {"type": "float", "required": True},
    "snr_wm": {"type": "float", "required": True},
    "snrd_csf": {"type": "float", "required": True},
    "snrd_gm": {"type": "float", "required": True},
    "snrd_total": {"type": "float", "required": True},
    "snrd_wm": {"type": "float", "required": True},
    "spacing_x": {"type": "float", "required": True},
    "spacing_y": {"type": "float", "required": True},
    "spacing_z": {"type": "float", "required": True},
    "summary_bg_k": {"type": "float", "required": True},
    "summary_bg_mean": {"type": "float", "required": True},
    "summary_bg_median": {"type": "float"},
    "summary_bg_mad": {"type": "float"},
    "summary_bg_p05": {"type": "float", "required": True},
    "summary_bg_p95": {"type": "float", "required": True},
    "summary_bg_stdv": {"type": "float", "required": True},
    "summary_bg_n": {"type": "float"},
    "summary_csf_k": {"type": "float", "required": True},
    "summary_csf_mean": {"type": "float", "required": True},
    "summary_csf_median": {"type": "float"},
    "summary_csf_mad": {"type": "float"},
    "summary_csf_p05": {"type": "float", "required": True},
    "summary_csf_p95": {"type": "float", "required": True},
    "summary_csf_stdv": {"type": "float", "required": True},
    "summary_csf_n": {"type": "float"},
    "summary_gm_k": {"type": "float", "required": True},
    "summary_gm_mean": {"type": "float", "required": True},
    "summary_gm_median": {"type": "float"},
    "summary_gm_mad": {"type": "float"},
    "summary_gm_p05": {"type": "float", "required": True},
    "summary_gm_p95": {"type": "float", "required": True},
    "summary_gm_stdv": {"type": "float", "required": True},
    "summary_gm_n": {"type": "float"},
    "summary_wm_k": {"type": "float", "required": True},
    "summary_wm_mean": {"type": "float", "required": True},
    "summary_wm_median": {"type": "float"},
    "summary_wm_mad": {"type": "float"},
    "summary_wm_p05": {"type": "float", "required": True},
    "summary_wm_p95": {"type": "float", "required": True},
    "summary_wm_stdv": {"type": "float", "required": True},
    "summary_wm_n": {"type": "float"},
    "tpm_overlap_csf": {"type": "float", "required": True},
    "tpm_overlap_gm": {"type": "float", "required": True},
    "tpm_overlap_wm": {"type": "float", "required": True},
    "wm2max": {"type": "float", "required": True},
}

rating_schema = {
    "_id": {"type": "string", "required": True},
    "rating": {"type": "string", "required": True},
    "name": {"type": "string", "required": False},
    "comment": {"type": "string", "required": False},
    "md5sum": {"type": "string", "required": True},
}

type_mappings = {
    "integer": pl.Int32,
    "float": pl.Float32,
    "string": pl.Utf8,
    "boolean": pl.Boolean,
}

bids_schema2: dict[str, pl.DataType] = {
    k: type_mappings[v.get("type")] for k, v in bids_schema.items()
}

rating_schema2: dict[str, pl.DataType] = {
    k: type_mappings.get(v.get("type")) for k, v in rating_schema.items()
}  # type: ignore

struct_iqms_schema2: dict[str, pl.DataType] = {
    k: type_mappings.get(v.get("type")) for k, v in struct_iqms_schema.items()
}  # type: ignore

bold_iqms_schema2: dict[str, pl.DataType] = {
    k: type_mappings.get(v.get("type")) for k, v in bold_iqms_schema.items()
}  # type: ignore
prov_schema2: dict[str, pl.DataType] = {
    k: type_mappings.get(v.get("type")) for k, v in prov_schema.items()
}  # type: ignore

settings_schema2: dict[str, pl.DataType] = {
    k: type_mappings.get(v.get("type")) for k, v in settings_schema.items()
}  # type: ignore


def get_iqms(modality: str, page: int = 1, max_results: int = 50) -> pl.DataFrame:
    """
    Grab all iqms for the given modality and the list of versions
    """
    url_root = f"https://mriqc.nimh.nih.gov/api/v1/{modality}"

    with requests.Session() as s:
        retries = Retry(total=3, backoff_factor=0.1)
        s.mount(url_root, HTTPAdapter(max_retries=retries))
        r = s.get(url_root, params={"page": page, "max_results": max_results})

    data = r.json()
    bids_meta = pl.from_dicts(
        [dict(item.get("bids_meta"), _id=item.get("_id")) for item in data["_items"]],
        schema=bids_schema2,
    )

    settings = pl.from_dicts(
        [
            dict(item.get("provenance").get("settings"), _id=item.get("_id"))
            for item in data["_items"]
        ],
        schema=settings_schema2,
    )

    provenance = pl.from_dicts(
        [dict(item.get("provenance"), _id=item.get("_id")) for item in data["_items"]],
        schema=prov_schema2,
    )

    iqms = pl.from_dicts(
        data["_items"],
        schema=struct_iqms_schema2 if modality == "T1w" else bold_iqms_schema2,
    )
    return (
        iqms.join(provenance, on="_id")
        .join(settings, on="_id")
        .join(bids_meta, on="_id")
    )


def main(outdir: Path, modality: str, max_pages: int = 50):
    max_results: int = 50
    ds: list[pl.DataFrame] = []
    page: int = 1
    while page < max_pages:
        print(f"{page=}")
        try:
            d = get_iqms(modality, page=page, max_results=max_results)
            ds.append(d)
        except BaseException as e:
            logging.error(e)

        time.sleep(0.1)

        page += 1

    t1outdir = outdir
    if not t1outdir.exists():
        t1outdir.mkdir(parents=True)
    pl.concat(ds).write_parquet(t1outdir / f"{modality}.parquet", statistics=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("modality", choices=["T1w", "bold"])
    parser.add_argument("--dst", required=False, default=Path("."), type=Path)
    parser.add_argument("--max-pages", required=False, default=50, type=int)

    args = parser.parse_args()
    main(outdir=args.dst, modality=args.modality, max_pages=args.max_pages)
