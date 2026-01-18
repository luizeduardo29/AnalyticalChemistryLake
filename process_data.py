import re
import uuid
import time
import subprocess
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

from pyteomics import mzml
import clickhouse_connect


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Diretório com .raw e .D")
    p.add_argument("--out", required=True, help="Diretório para mzML temporário")
    return p.parse_args()


args = parse_args()

BASE_DIR = Path(args.input)
OUT_MZML_DIR = Path(args.out)
OUT_MZML_DIR.mkdir(parents=True, exist_ok=True)

MSCONVERT = Path(r"C:\Program Files\ProteoWizard\ProteoWizard 3.0.25218.15b0739\msconvert.exe")

CH = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username="default",
    # username="user",
    password="1234",
    # password="Default@2026",
    database="analyticalChemistryLake",
)

BATCH_POINTS = 100_000
BATCH_SCANS = 50_000
RETRY_DELAY = 5


def ch_insert_retry(table: str, rows, column_names):
    while True:
        try:
            CH.insert(table, rows, column_names=column_names)
            return
        except Exception:
            time.sleep(RETRY_DELAY)


def find_inputs(base_dir: Path):
    items = []
    for p in base_dir.rglob("*"):
        suf = p.suffix.lower()
        if suf == ".raw" and (p.is_file() or p.is_dir()):
            items.append(p)
        elif suf == ".d" and p.is_dir():
            items.append(p)
    return items


def technique_from_input(p: Path) -> str:
    if p.suffix.lower() == ".raw":
        return "LC-MS"
    if p.suffix.lower() == ".d":
        return "GC-MS"
    return "UNKNOWN"


def run_msconvert(input_path: Path) -> Path:
    mzml_path = OUT_MZML_DIR / f"{input_path.stem}.mzML"

    if mzml_path.exists():
        mzml_path.unlink()

    subprocess.run(
        [
            str(MSCONVERT),
            str(input_path),
            "--mzML",
            "--filter", "peakPicking true 1-",
            "--outdir", str(OUT_MZML_DIR),
            "--outfile", mzml_path.name,
        ],
        check=True,
    )

    return mzml_path


def insert_sample(sample_name: str) -> str:
    sid = str(uuid.uuid4())
    CH.insert(
        "samples",
        [[sid, sample_name, datetime.utcnow()]],
        ["sample_id", "sample_name", "created_at"],
    )
    return sid


def insert_channel(sample_id: str, technique: str,
                   scan_filter: Optional[str],
                   sim_ion_name: Optional[str]) -> str:
    cid = str(uuid.uuid4())
    CH.insert(
        "sample_channels",
        [[cid, sample_id, technique, scan_filter, sim_ion_name, datetime.utcnow()]],
        [
            "channel_id",
            "sample_id",
            "chromatography_technique",
            "scan_filter",
            "sim_ion_name",
            "created_at",
        ],
    )
    return cid


def extract_target_mz(label: str) -> Optional[str]:
    m = re.search(r"(\d+\.\d+)", label)
    return str(float(m.group(1))) if m else None


def ingest_gcms_sim(mzml_path: Path, sample_id: str):
    ion_channels: Dict[str, str] = {}

    with mzml.MzML(str(mzml_path)) as reader:
        for chrom in reader.iterfind("chromatogram"):
            label = chrom.get("id", "")
            ion = None

            if "TIC" in label and "SIC" not in label:
                ion = "TIC"
            elif "SIC" in label:
                ion = extract_target_mz(label)

            if not ion:
                continue

            if ion not in ion_channels:
                ion_channels[ion] = insert_channel(sample_id, "GC-MS", None, ion)

            rt = chrom.get("time array")
            it = chrom.get("intensity array")
            if rt is None or it is None:
                continue

            rows = []
            for r, i in zip(rt, it):
                rows.append([ion_channels[ion], float(r), float(i)])
                if len(rows) >= BATCH_SCANS:
                    ch_insert_retry(
                        "chromatogram_points",
                        rows,
                        ["channel_id", "rt", "intensity"],
                    )
                    rows.clear()

            if rows:
                ch_insert_retry(
                    "chromatogram_points",
                    rows,
                    ["channel_id", "rt", "intensity"],
                )


def ingest_lcms_chromatograms(mzml_path: Path, sample_id: str):
    channels: Dict[str, str] = {}

    with mzml.MzML(str(mzml_path)) as reader:
        for chrom in reader.iterfind("chromatogram"):
            cid = chrom.get("id", "")
            if not cid:
                continue

            if cid not in channels:
                channels[cid] = insert_channel(sample_id, "LC-MS", None, cid)

            rt = chrom.get("time array")
            it = chrom.get("intensity array")
            if rt is None or it is None:
                continue

            rows = []
            for r, i in zip(rt, it):
                rows.append([channels[cid], float(r), float(i)])
                if len(rows) >= BATCH_SCANS:
                    ch_insert_retry(
                        "chromatogram_points",
                        rows,
                        ["channel_id", "rt", "intensity"],
                    )
                    rows.clear()

            if rows:
                ch_insert_retry(
                    "chromatogram_points",
                    rows,
                    ["channel_id", "rt", "intensity"],
                )


def ingest_lcms_fullscan_ms2(mzml_path: Path, sample_id: str):
    channels: Dict[str, str] = {}
    counters: Dict[str, int] = {}
    scans = []
    points = []

    with mzml.read(str(mzml_path)) as reader:
        for spec in reader:
            ms = spec.get("ms level")
            if ms not in (1, 2):
                continue

            scan = spec.get("scanList", {}).get("scan", [{}])[0]
            filt = scan.get("filter string", "")
            if ms == 1 and "sim" in filt.lower():
                continue

            rt = scan.get("scan start time")
            if rt is None:
                continue

            mzs = spec.get("m/z array")
            ints = spec.get("intensity array")
            if mzs is None or ints is None:
                continue

            if filt not in channels:
                channels[filt] = insert_channel(sample_id, "LC-MS", filt, None)
                counters[filt] = 0

            counters[filt] += 1
            idx = counters[filt]
            cid = channels[filt]

            scans.append([cid, idx, float(rt), int(ms)])

            for m, i in zip(mzs, ints):
                points.append([cid, idx, float(m), float(i)])

            if len(points) >= BATCH_POINTS:
                ch_insert_retry(
                    "lcms_scans",
                    scans,
                    ["channel_id", "scan_index", "rt", "ms_level"],
                )
                scans.clear()

                ch_insert_retry(
                    "lcms_spectra_points",
                    points,
                    ["channel_id", "scan_index", "mz", "intensity"],
                )
                points.clear()

    if scans:
        ch_insert_retry(
            "lcms_scans",
            scans,
            ["channel_id", "scan_index", "rt", "ms_level"],
        )

    if points:
        ch_insert_retry(
            "lcms_spectra_points",
            points,
            ["channel_id", "scan_index", "mz", "intensity"],
        )


def process_all():
    for inp in find_inputs(BASE_DIR):
        mzml = None
        try:
            mzml = run_msconvert(inp)
            sid = insert_sample(inp.stem)

            if technique_from_input(inp) == "GC-MS":
                ingest_gcms_sim(mzml, sid)
            else:
                ingest_lcms_chromatograms(mzml, sid)
                ingest_lcms_fullscan_ms2(mzml, sid)

        finally:
            if mzml and mzml.exists():
                mzml.unlink()


if __name__ == "__main__":
    process_all()
    print("FIM")
