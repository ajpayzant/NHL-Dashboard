# ======================================================================================
# PIPELINE SCRIPT (MoneyPuck NHL Warehouse v3.1 RS-only + Lines Ready)
#   RAW -> EXTRACT -> BRONZE -> SILVER -> DERIVED(LINES_READY) -> GOLD -> REPAIR -> AUDIT
#   RS-only enforced for GBG datasets (gametype '02' + playoffgame==0 when available)
#
# CHANGES vs Colab notebook:
#   - removed: !pip install, google.colab.drive mount
#   - replaced: hardcoded /content paths with env-driven local paths
# ======================================================================================

import os, re, gc, json, time, shutil, zipfile, subprocess, hashlib, unicodedata
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd
import numpy as np
from tqdm.auto import tqdm

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# ======================================================================================
# CONFIG (edit only here)
# ======================================================================================
# In GitHub Actions / local, set:
#   NHL_DATA_ROOT=./data/NHL_MoneyPuck
#   NHL_LOCAL_ROOT=./tmp_nhl
DATA_ROOT  = Path(os.environ.get("NHL_DATA_ROOT", "./data/NHL_MoneyPuck")).resolve()
LOCAL_ROOT = Path(os.environ.get("NHL_LOCAL_ROOT", "./tmp_nhl")).resolve()

PIPELINE_TAG = os.environ.get("PIPELINE_TAG", "v3")  # set "" to use plain bronze/silver/gold folder names

def _tag(name: str) -> str:
    return f"{name}_{PIPELINE_TAG}" if PIPELINE_TAG else name

RAW_DIR      = DATA_ROOT / "raw"
EXTRACT_DIR  = DATA_ROOT / "extracted"
BRONZE_DIR   = DATA_ROOT / _tag("bronze")
SILVER_DIR   = DATA_ROOT / _tag("silver")
GOLD_DIR     = DATA_ROOT / _tag("gold")
LOG_DIR      = DATA_ROOT / _tag("logs")

# Situations kept downstream (normalize 5on5 -> 5v5)
KEEP_SITUATIONS = {"all", "5v5", "5on4", "4on5"}

# Rolling window policy
STRICT_ROLLING = False   # False => partial rolling windows allowed (GP_L{w} still provided)

# RS-only enforcement
RS_ONLY_IN_SILVER = True
RS_ONLY_IN_GOLD   = True  # keep True (belt+suspenders)

# Incremental refresh
FORCE_DOWNLOAD_STATIC  = False
FORCE_DOWNLOAD_ROLLING = True
FORCE_EXTRACT          = False

FORCE_BRONZE_STATIC    = False
FORCE_BRONZE_ROLLING   = True

FORCE_SILVER_STATIC    = False
FORCE_SILVER_ROLLING   = True

FORCE_GOLD_STATIC      = False
FORCE_GOLD_ROLLING     = True

# Run toggles
RUN_DOWNLOAD    = True
RUN_EXTRACT     = True
RUN_BRONZE      = True
RUN_SILVER      = True
RUN_LINES_READY = True
RUN_GOLD        = True
RUN_REPAIR      = True
RUN_AUDIT       = True

AUTO_REPAIR_GOALIE_TOI = True

# Perf
DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS", "4"))
DUCKDB_MEM_LIMIT = os.environ.get("DUCKDB_MEM_LIMIT", "6GB")
CSV_CHUNKSIZE_DEFAULT = 200_000

# Current season detection (July boundary)
now = datetime.now()
CURRENT_SEASON_START = now.year if now.month >= 7 else now.year - 1
CURRENT_SEASON_LABEL = str(CURRENT_SEASON_START)
SEASONS_TO_REFRESH = {int(CURRENT_SEASON_START)}

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

for p in [DATA_ROOT, RAW_DIR, EXTRACT_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, LOG_DIR, LOCAL_ROOT]:
    p.mkdir(parents=True, exist_ok=True)

print("RUN_ID:", RUN_ID)
print("CURRENT_SEASON:", f"{CURRENT_SEASON_START}-{str(CURRENT_SEASON_START+1)[-2:]}")
print("PIPELINE_TAG:", PIPELINE_TAG)
print("DATA_ROOT:", DATA_ROOT)
print("LOCAL_ROOT:", LOCAL_ROOT)

# ======================================================================================
# DATASET REGISTRY
# ======================================================================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "text/csv,application/zip,application/octet-stream,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://moneypuck.com/data.htm",
    "Connection": "keep-alive",
}

DATASETS = [
    # Season summaries (already regular-season)
    dict(key="season_skaters_current", kind="csv", level="season", source_type="rolling",
         urls=[f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{CURRENT_SEASON_LABEL}/regular/skaters.csv"]),
    dict(key="season_goalies_current", kind="csv", level="season", source_type="rolling",
         urls=[f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{CURRENT_SEASON_LABEL}/regular/goalies.csv"]),
    dict(key="season_lines_current", kind="csv", level="season", source_type="rolling",
         urls=[f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{CURRENT_SEASON_LABEL}/regular/lines.csv"]),
    dict(key="season_teams_current", kind="csv", level="season", source_type="rolling",
         urls=[f"https://moneypuck.com/moneypuck/playerData/seasonSummary/{CURRENT_SEASON_LABEL}/regular/teams.csv"]),

    # Season summaries (historical zips)
    dict(key="season_skaters_hist_zip", kind="zip", level="season", source_type="static", extract=True,
         urls=["https://peter-tanner.com/moneypuck/downloads/historicalOneRowPerSeason/skaters_2008_to_2024.zip"]),
    dict(key="season_goalies_hist_zip", kind="zip", level="season", source_type="static", extract=True,
         urls=["https://peter-tanner.com/moneypuck/downloads/historicalOneRowPerSeason/goalies_2008_to_2024.zip"]),
    dict(key="season_lines_hist_zip", kind="zip", level="season", source_type="static", extract=True,
         urls=["https://peter-tanner.com/moneypuck/downloads/historicalOneRowPerSeason/lines_2008_to_2024.zip"]),
    dict(key="season_teams_hist_zip", kind="zip", level="season", source_type="static", extract=True,
         urls=["https://peter-tanner.com/moneypuck/downloads/historicalOneRowPerSeason/teams_2008_to_2024.zip"]),

    # GBG (current season zips)
    dict(key="gbg_skaters_current_zip", kind="zip", level="gbg", source_type="rolling", extract=True,
         urls=[f"https://peter-tanner.com/moneypuck/downloads/seasonPlayersSummary/skaters/{CURRENT_SEASON_LABEL}.zip"]),
    dict(key="gbg_goalies_current_zip", kind="zip", level="gbg", source_type="rolling", extract=True,
         urls=[f"https://peter-tanner.com/moneypuck/downloads/seasonPlayersSummary/goalies/{CURRENT_SEASON_LABEL}.zip"]),
    dict(key="gbg_lines_current_zip", kind="zip", level="gbg", source_type="rolling", extract=True,
         urls=[f"https://peter-tanner.com/moneypuck/downloads/seasonPlayersSummary/lines/{CURRENT_SEASON_LABEL}.zip"]),

    # GBG (historical zips)
    dict(key="gbg_skaters_hist_zip", kind="zip", level="gbg", source_type="static", extract=True,
         urls=["https://peter-tanner.com/moneypuck/downloads/seasonPlayersSummary/skaters/2008_to_2024.zip"]),
    dict(key="gbg_goalies_hist_zip", kind="zip", level="gbg", source_type="static", extract=True,
         urls=["https://peter-tanner.com/moneypuck/downloads/seasonPlayersSummary/goalies/2008_to_2024.zip"]),
    dict(key="gbg_lines_hist_zip", kind="zip", level="gbg", source_type="static", extract=True,
         urls=["https://peter-tanner.com/moneypuck/downloads/seasonPlayersSummary/lines/2008_to_2024.zip"]),

    # Team GBG (single CSV)
    dict(key="gbg_teams_all", kind="csv", level="gbg", source_type="rolling",
         urls=["https://moneypuck.com/moneypuck/playerData/careers/gameByGame/all_teams.csv"]),

    # Lookup (optional)
    dict(key="data_dictionary_players", kind="csv", level="lookup", source_type="static",
         urls=["https://peter-tanner.com/moneypuck/downloads/MoneyPuckDataDictionaryForPlayers.csv"]),
    dict(key="player_bios", kind="csv", level="lookup", source_type="rolling",
         urls=["https://moneypuck.com/moneypuck/playerData/playerBios/allPlayersLookup.csv"]),
]

print("Datasets registered:", len(DATASETS))

# ======================================================================================
# UTILITIES
# ======================================================================================
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def file_meta(path: Path) -> dict:
    if not path.exists():
        return {}
    st = path.stat()
    return {"size": int(st.st_size), "mtime": float(st.st_mtime)}

def write_json(path: Path, obj: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str))

def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text())

def infer_ext_from_kind(kind: str) -> str:
    return ".zip" if kind == "zip" else ".csv"

def curl_download(url: str, out_path: Path) -> tuple[bool, str]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "bash","-lc",
        f'curl -L --fail -A "{HEADERS["User-Agent"]}" -H "Referer: {HEADERS["Referer"]}" '
        f'--retry 3 --retry-delay 2 --connect-timeout 20 --max-time 0 '
        f'-o "{out_path}" "{url}"'
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    ok = (res.returncode == 0)
    err = (res.stderr or "")[-800:]
    return ok, err

def download_stream(url: str, out_path: Path, force: bool=False, max_retries: int=3) -> dict:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists() and out_path.stat().st_size > 0 and not force:
        return {"status": "skipped_exists", "bytes": out_path.stat().st_size, "url": url}

    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    last_err = ""
    for attempt in range(1, max_retries + 1):
        try:
            with requests.Session() as s:
                r = s.get(url, headers=HEADERS, stream=True, allow_redirects=True, timeout=60)
                if r.status_code != 200:
                    last_err = f"HTTP {r.status_code}"
                    if r.status_code in (403, 429):
                        break
                    time.sleep(1.5 * attempt)
                    continue

                total = r.headers.get("Content-Length")
                total = int(total) if (total and total.isdigit()) else None

                with open(tmp_path, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=out_path.name) as pbar:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

                if tmp_path.exists() and tmp_path.stat().st_size > 0:
                    tmp_path.replace(out_path)
                    return {"status": "downloaded_requests", "bytes": out_path.stat().st_size, "url": url}

                last_err = "empty_file_after_download"
                time.sleep(1.5 * attempt)

        except Exception as e:
            last_err = repr(e)
            time.sleep(1.5 * attempt)

    ok, err_tail = curl_download(url, out_path)
    if ok and out_path.exists() and out_path.stat().st_size > 0:
        return {"status": "downloaded_curl", "bytes": out_path.stat().st_size, "url": url, "note": last_err}
    return {"status": "failed", "error": last_err, "curl_err_tail": err_tail, "url": url}

def extract_zip(zip_path: Path, out_dir: Path, overwrite: bool=False) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = out_dir / ".extract_stamp.json"
    if not zip_path.exists():
        return {"status": "zip_missing", "zip": str(zip_path)}
    meta = {"zip_size": zip_path.stat().st_size, "zip_mtime": zip_path.stat().st_mtime}

    if stamp.exists() and not overwrite:
        try:
            old = json.loads(stamp.read_text())
            if old == meta:
                return {"status": "skipped_unchanged"}
        except Exception:
            pass

    for item in out_dir.iterdir():
        if item.name == ".extract_stamp.json":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)

    stamp.write_text(json.dumps(meta, indent=2))
    return {"status": "extracted", "files": len(zipfile.ZipFile(zip_path).namelist())}

def sync_tree(src: Path, dst: Path, patterns=("*.parquet", "*.json", "*.csv")) -> dict:
    dst.mkdir(parents=True, exist_ok=True)
    copied = 0
    scanned = 0
    for pat in patterns:
        for f in src.rglob(pat):
            if not f.is_file():
                continue
            rel = f.relative_to(src)
            g = dst / rel
            g.parent.mkdir(parents=True, exist_ok=True)

            scanned += 1
            if g.exists():
                fs, gs = f.stat(), g.stat()
                if (fs.st_size == gs.st_size) and (int(fs.st_mtime) <= int(gs.st_mtime)):
                    continue
            shutil.copy2(f, g)
            copied += 1
    return {"scanned": scanned, "copied": copied, "src": str(src), "dst": str(dst)}

def to_snake_case(col: str) -> str:
    c = str(col).strip()
    c = c.replace("%", "pct").replace("/", "_per_")
    c = re.sub(r"[^\w\s]", "_", c)
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"_+", "_", c)
    return c.lower().strip("_")

def make_unique(cols):
    seen, out = {}, []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}__{seen[c]}")
    return out

def schema_checksum_from_parquet(part_path: Path) -> str:
    pf = pq.ParquetFile(part_path)
    schema = pf.schema_arrow
    s = "|".join([f"{f.name}:{f.type}" for f in schema])
    return sha256_text(s)

TEAM_MAP = {
    "T.B": "TBL", "TB": "TBL", "TBL": "TBL",
    "L.A": "LAK", "LA": "LAK", "LAK": "LAK",
    "N.J": "NJD", "NJ": "NJD", "NJD": "NJD",
    "S.J": "SJS", "SJ": "SJS", "SJS": "SJS",
    "PHX": "ARI",
}
def canon_team(x):
    if pd.isna(x):
        return x
    s = str(x).strip().upper().replace(".", "").replace(" ", "")
    return TEAM_MAP.get(s, s)

def duckdb_connect():
    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
    con.execute(f"PRAGMA memory_limit='{DUCKDB_MEM_LIMIT}';")
    return con

def rs_predicate_mask_from_df(df: pd.DataFrame) -> pd.Series:
    if "gameid" not in df.columns:
        return pd.Series([True]*len(df), index=df.index)
    gid = df["gameid"].astype("string").str.zfill(10)
    gametype = gid.str[4:6]
    rs_mask = (gametype == "02")
    if "playoffgame" in df.columns:
        rs_mask = rs_mask & (df["playoffgame"].fillna(0).astype(int) == 0)
    return rs_mask.fillna(False)

def rs_predicate_sql(has_playoffgame: bool) -> str:
    gametype = "substr(lpad(cast(gameid as VARCHAR), 10, '0'), 5, 2)"
    if has_playoffgame:
        return f"({gametype}='02') AND (coalesce(cast(playoffgame as INTEGER),0)=0)"
    return f"({gametype}='02')"

def norm_last_name(x: str):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.replace(".", "").replace("'", "")
    if " " in s:
        s = s.split()[-1]
    return s.strip() or None

# ======================================================================================
# RAW: DOWNLOAD
# ======================================================================================
def dataset_force_flags(ds: dict) -> tuple[bool, bool]:
    is_rolling = (ds.get("source_type") == "rolling")
    force_download = FORCE_DOWNLOAD_ROLLING if is_rolling else FORCE_DOWNLOAD_STATIC
    return force_download, is_rolling

def download_all(datasets=DATASETS) -> Path:
    manifest = {"run_id": RUN_ID, "run_at": utc_now_iso(), "items": []}
    for ds in datasets:
        key = ds["key"]
        ext = infer_ext_from_kind(ds["kind"])
        out_path = RAW_DIR / f"{key}{ext}"
        force_download, _ = dataset_force_flags(ds)

        result = None
        for url in ds["urls"]:
            print(f"\n--- DOWNLOAD {key} -> {out_path.name}")
            result = download_stream(url, out_path, force=force_download, max_retries=3)
            if result["status"] != "failed":
                break

        entry = {"key": key, "out_path": str(out_path), "kind": ds["kind"], "level": ds.get("level"), **(result or {})}
        manifest["items"].append(entry)

        if entry["status"] == "failed":
            print("FAILED:", key, entry.get("error"), (entry.get("curl_err_tail","") or "")[:200])

    manifest_path = LOG_DIR / f"download_manifest_{RUN_ID}.json"
    write_json(manifest_path, manifest)
    print("\n✅ DOWNLOAD complete:", manifest_path)
    return manifest_path

# ======================================================================================
# EXTRACT
# ======================================================================================
def extract_all(datasets=DATASETS) -> Path:
    manifest = {"run_id": RUN_ID, "run_at": utc_now_iso(), "items": []}
    for ds in datasets:
        if ds["kind"] != "zip":
            continue
        key = ds["key"]
        zip_path = RAW_DIR / f"{key}.zip"
        out_dir  = EXTRACT_DIR / key

        _, is_rolling = dataset_force_flags(ds)
        overwrite = FORCE_EXTRACT or (is_rolling and FORCE_DOWNLOAD_ROLLING)

        print(f"\n--- EXTRACT {key} -> {out_dir}")
        res = extract_zip(zip_path, out_dir, overwrite=overwrite)
        manifest["items"].append({"key": key, "zip": str(zip_path), "extract_dir": str(out_dir), **res})

    manifest_path = LOG_DIR / f"extract_manifest_{RUN_ID}.json"
    write_json(manifest_path, manifest)
    print("\n✅ EXTRACT complete:", manifest_path)
    return manifest_path

def find_extracted_csv(key: str) -> Path:
    d = EXTRACT_DIR / key
    csvs = sorted(d.rglob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No extracted CSV found for {key} in {d}")
    return csvs[0]

# ======================================================================================
# BRONZE
# ======================================================================================
def basic_type_fixes(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["playerid", "goalieid", "gameid", "lineid"]:
        if c in df.columns:
            df[c] = df[c].astype("string")
    for c in ["team", "playerteam", "opposingteam", "opp_team", "home_or_away", "position", "situation", "name"]:
        if c in df.columns:
            df[c] = df[c].astype("string")
    if "gamedate" in df.columns:
        gd = df["gamedate"].astype("string")
        df["gamedate_dt"] = pd.to_datetime(gd, format="%Y%m%d", errors="coerce")
    return df

def write_parquet_part(df: pd.DataFrame, out_dir: Path, part_idx: int, compression="zstd"):
    out_path = out_dir / f"part_{part_idx:05d}.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    try:
        pq.write_table(table, out_path, compression=compression)
    except Exception:
        pq.write_table(table, out_path, compression="snappy")
    return out_path

def bronze_out_dir(key: str) -> Path:
    return BRONZE_DIR / key

def bronze_stamp_path(key: str) -> Path:
    return bronze_out_dir(key) / ".bronze_stamp.json"

def bronze_needs_rebuild(key: str, src_csv: Path, force: bool) -> bool:
    if force:
        return True
    stamp = read_json(bronze_stamp_path(key))
    cur = {"src": str(src_csv), **file_meta(src_csv)}
    return stamp.get("src_meta") != cur

def process_csv_to_bronze_parts(src_csv: Path, key: str, meta: dict, chunksize: int, force: bool, compression="zstd") -> dict:
    out_drive = bronze_out_dir(key)
    out_local = LOCAL_ROOT / "bronze_stage" / key

    if bronze_needs_rebuild(key, src_csv, force=force):
        if out_local.exists():
            shutil.rmtree(out_local)
        out_local.mkdir(parents=True, exist_ok=True)

        rename_map = None
        rows_total = 0
        part_idx = 0

        for chunk in pd.read_csv(src_csv, chunksize=chunksize, low_memory=False):
            if rename_map is None:
                orig_cols = list(chunk.columns)
                new_cols = make_unique([to_snake_case(c) for c in orig_cols])
                rename_map = dict(zip(orig_cols, new_cols))

            chunk = chunk.rename(columns=rename_map)
            chunk["source_file"] = src_csv.name
            chunk["dataset_key"] = key
            for k, v in meta.items():
                chunk[k] = v

            chunk = basic_type_fixes(chunk)
            write_parquet_part(chunk, out_local, part_idx, compression=compression)

            rows_total += len(chunk)
            part_idx += 1
            del chunk
            gc.collect()

        stamp = {
            "run_id": RUN_ID,
            "built_at": utc_now_iso(),
            "key": key,
            "src_meta": {"src": str(src_csv), **file_meta(src_csv)},
            "rows": rows_total,
            "parts": part_idx,
            "chunksize": chunksize,
            "meta": meta,
        }
        write_json(out_local / ".bronze_stamp.json", stamp)
        sync = sync_tree(out_local, out_drive, patterns=("*.parquet", "*.json"))
        return {"status": "rebuilt", "rows": rows_total, "parts": part_idx, "synced": sync}
    else:
        return {"status": "skipped_unchanged"}

def bronze_build_all(datasets=DATASETS) -> Path:
    manifest = {"run_id": RUN_ID, "run_at": utc_now_iso(), "items": []}
    for ds in datasets:
        key = ds["key"]
        _, is_rolling = dataset_force_flags(ds)
        force = (FORCE_BRONZE_ROLLING if is_rolling else FORCE_BRONZE_STATIC)

        src_csv = (RAW_DIR / f"{key}.csv") if ds["kind"] == "csv" else find_extracted_csv(key)
        if not src_csv.exists():
            manifest["items"].append({"key": key, "status": "missing_src", "src_csv": str(src_csv)})
            print("Missing source CSV:", key, src_csv)
            continue

        meta = {"data_level": ds.get("level"), "source_type": ds.get("source_type")}
        chunksize = ds.get("chunksize", CSV_CHUNKSIZE_DEFAULT)
        if key == "gbg_skaters_hist_zip":
            chunksize = 120_000
        if key == "gbg_lines_hist_zip":
            chunksize = 150_000

        print(f"\n--- BRONZE {key} (force={force})")
        res = process_csv_to_bronze_parts(src_csv, key, meta, chunksize=chunksize, force=force, compression="zstd")
        manifest["items"].append({"key": key, "src_csv": str(src_csv), **res})
        print(" ->", res["status"])

    manifest_path = LOG_DIR / f"bronze_manifest_{RUN_ID}.json"
    write_json(manifest_path, manifest)
    print("\n✅ BRONZE complete:", manifest_path)
    return manifest_path

# ======================================================================================
# SILVER (standardize + partition by situation + RS-only filter for GBG)
# ======================================================================================
def silver_out_dir(key: str) -> Path:
    return SILVER_DIR / key

def silver_stamp_path(key: str) -> Path:
    return silver_out_dir(key) / ".silver_stamp.json"

def silver_needs_rebuild(key: str, force: bool) -> bool:
    if force:
        return True
    bronze_stamp = read_json(bronze_stamp_path(key))
    silver_stamp = read_json(silver_stamp_path(key))
    return silver_stamp.get("bronze_stamp_src") != bronze_stamp.get("src_meta")

def dedupe_columns(df: pd.DataFrame):
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]
    return df

def reorder_columns(df: pd.DataFrame):
    priority = [
        "playerid", "goalieid", "lineid",
        "name",
        "season", "gamedate", "gameid",
        "team", "opp_team", "home_or_away",
        "position", "situation",
        "games_played", "TOI",
        "playoffgame",
        "source_file", "dataset_key", "data_level", "source_type",
    ]
    priority_present = [c for c in priority if c in df.columns]
    rest = [c for c in df.columns if c not in priority_present]
    return df[priority_present + rest]

def standardize_columns(df: pd.DataFrame, dataset_key: str):
    df = dedupe_columns(df)

    rename_map = {}
    for c in df.columns:
        new = c
        if "office_" in new: new = new.replace("office_", "off_ice_")
        if "onice_" in new:  new = new.replace("onice_",  "on_ice_")
        if "penality" in new: new = new.replace("penality", "penalty")
        if new != c:
            rename_map[c] = new
    if rename_map:
        df = df.rename(columns=rename_map)

    df = dedupe_columns(df)

    for typo, correct in [("penalitiesfor", "penaltiesfor"), ("penalitiesagainst", "penaltiesagainst")]:
        if typo in df.columns:
            if correct in df.columns:
                df = df.drop(columns=[typo])
            else:
                df = df.rename(columns={typo: correct})

    if "playerteam" in df.columns and "team" not in df.columns:
        df = df.rename(columns={"playerteam": "team"})

    if "opposingteam" in df.columns:
        df = df.rename(columns={"opposingteam": "opp_team"})

    for tcol in ["team", "opp_team", "team_1", "team_row", "playerteam"]:
        if tcol in df.columns:
            df[tcol] = df[tcol].map(canon_team)
    if "team_1" in df.columns:
        df = df.drop(columns=["team_1"])

    if "icetime" in df.columns:
        df["TOI"] = pd.to_numeric(df["icetime"], errors="coerce") / 60.0
        df = df.drop(columns=["icetime"])

    if "gamedate_dt" in df.columns:
        if "gamedate" in df.columns:
            df = df.drop(columns=["gamedate"])
        df = df.rename(columns={"gamedate_dt": "gamedate"})
        df["gamedate"] = pd.to_datetime(df["gamedate"], errors="coerce")
    elif "gamedate" in df.columns:
        if pd.api.types.is_numeric_dtype(df["gamedate"]):
            df["gamedate"] = pd.to_datetime(df["gamedate"].astype("Int64").astype(str), errors="coerce", format="%Y%m%d")
        else:
            df["gamedate"] = pd.to_datetime(df["gamedate"], errors="coerce")

    if "situation" in df.columns:
        df["situation"] = df["situation"].astype(str).str.strip().replace({"5on5": "5v5"})
        df = df[df["situation"].isin(KEEP_SITUATIONS)].copy()

    if RS_ONLY_IN_SILVER and dataset_key.startswith("gbg_") and ("gameid" in df.columns):
        df = df[rs_predicate_mask_from_df(df)].copy()

    df = dedupe_columns(df)
    df = reorder_columns(df)
    return df

def silver_build_all() -> Path:
    manifest = {"run_id": RUN_ID, "run_at": utc_now_iso(), "items": []}

    for ds in DATASETS:
        key = ds["key"]
        _, is_rolling = dataset_force_flags(ds)
        force = (FORCE_SILVER_ROLLING if is_rolling else FORCE_SILVER_STATIC)

        bronze_dir = bronze_out_dir(key)
        parts = sorted(bronze_dir.glob("part_*.parquet"))
        if not parts:
            manifest["items"].append({"key": key, "status": "missing_bronze"})
            print("Missing BRONZE:", key)
            continue

        if not silver_needs_rebuild(key, force=force):
            manifest["items"].append({"key": key, "status": "skipped_unchanged"})
            print("SILVER skipped unchanged:", key)
            continue

        out_drive = silver_out_dir(key)
        out_local = LOCAL_ROOT / "silver_stage" / key
        if out_local.exists():
            shutil.rmtree(out_local)
        out_local.mkdir(parents=True, exist_ok=True)

        rows_in = 0
        rows_out = 0
        wrote = 0

        schema = pq.ParquetFile(parts[0]).schema_arrow
        has_situation = ("situation" in [f.name for f in schema])

        for i, p in enumerate(parts):
            df = pd.read_parquet(p, engine="pyarrow")
            rows_in += len(df)

            df2 = standardize_columns(df, key)
            rows_out += len(df2)

            if len(df2) == 0:
                del df, df2
                gc.collect()
                continue

            if has_situation and "situation" in df2.columns:
                for sit, g in df2.groupby("situation", sort=False):
                    out_path = out_local / f"situation={sit}" / f"part_{i:05d}.parquet"
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    g.to_parquet(out_path, index=False, engine="pyarrow")
                    wrote += 1
            else:
                out_path = out_local / f"part_{i:05d}.parquet"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                df2.to_parquet(out_path, index=False, engine="pyarrow")
                wrote += 1

            del df, df2
            gc.collect()

        bronze_stamp = read_json(bronze_stamp_path(key))
        stamp = {
            "run_id": RUN_ID,
            "built_at": utc_now_iso(),
            "key": key,
            "bronze_stamp_src": bronze_stamp.get("src_meta"),
            "rows_in": rows_in,
            "rows_out": rows_out,
            "parts_written": wrote,
            "rs_only_in_silver": bool(RS_ONLY_IN_SILVER),
        }
        write_json(out_local / ".silver_stamp.json", stamp)
        sync = sync_tree(out_local, out_drive, patterns=("*.parquet", "*.json"))

        manifest["items"].append({"key": key, "status": "rebuilt", **stamp, "synced": sync})
        print(f"✅ SILVER {key}: rows_in={rows_in:,} rows_out={rows_out:,} parts_written={wrote} | synced={sync['copied']}")

    manifest_path = LOG_DIR / f"silver_manifest_{RUN_ID}.json"
    write_json(manifest_path, manifest)
    print("\n✅ SILVER complete:", manifest_path)
    return manifest_path

# ======================================================================================
# DERIVED: LINES_READY (unchanged)
# ======================================================================================
LINES_GAME_READY_KEY   = "lines_game_ready"
LINES_SEASON_READY_KEY = "lines_season_ready"

LINE_COLS = [
    "lineid","name","gameid","season","team","opp_team","home_or_away","gamedate","position","situation","TOI",
    "xgoalsfor","xgoalsagainst","goalsfor","goalsagainst",
    "shotsongoalfor","shotsongoalagainst",
    "shotattemptsfor","shotattemptsagainst",
    "unblockedshotattemptsfor","unblockedshotattemptsagainst",
    "highdangershotsfor","highdangershotsagainst",
    "mediumdangershotsfor","mediumdangershotsagainst",
    "lowdangershotsfor","lowdangershotsagainst",
    "penaltiesfor","penaltiesagainst",
    "penaltyminutesfor","penaltyminutesagainst",
    "xgoalspercentage","corsipercentage","fenwickpercentage",
    "playoffgame"
]
LINE_PCT_COLS = ["xgoalspercentage","corsipercentage","fenwickpercentage"]
LINE_COUNT_COLS = [
    "xgoalsfor","xgoalsagainst","goalsfor","goalsagainst",
    "shotsongoalfor","shotsongoalagainst",
    "shotattemptsfor","shotattemptsagainst",
    "unblockedshotattemptsfor","unblockedshotattemptsagainst",
    "highdangershotsfor","highdangershotsagainst",
    "mediumdangershotsfor","mediumdangershotsagainst",
    "lowdangershotsfor","lowdangershotsagainst",
    "penaltiesfor","penaltiesagainst",
    "penaltyminutesfor","penaltyminutesagainst",
]

_id_pat = re.compile(r"(8\d{6})")
def decode_lineid(lineid_val) -> list[str]:
    if lineid_val is None or (isinstance(lineid_val, float) and np.isnan(lineid_val)):
        return []
    s = str(lineid_val).replace(".0", "")
    ids = _id_pat.findall(s)
    out = []
    for x in ids:
        if x not in out:
            out.append(x)
    return out[:3]

def _silver_glob(dataset_key: str, situation: str) -> str | None:
    base = SILVER_DIR / dataset_key / f"situation={situation}"
    if base.exists():
        return str(base / "part_*.parquet")
    base2 = SILVER_DIR / dataset_key
    parts = list(base2.glob("part_*.parquet"))
    return str(base2 / "part_*.parquet") if parts else None

def build_lines_ready() -> Path:
    con = duckdb_connect()
    manifest = {"run_id": RUN_ID, "run_at": utc_now_iso(), "status": "ok", "items": []}

    skater_globs_all = []
    for sit in sorted(KEEP_SITUATIONS):
        for k in ["gbg_skaters_hist_zip", "gbg_skaters_current_zip"]:
            g = _silver_glob(k, sit)
            if g:
                skater_globs_all.append(g)
    skater_globs_all = sorted(list(set(skater_globs_all)))
    if not skater_globs_all:
        raise FileNotFoundError("No SILVER skaters parquet found (needed for line name lookup).")
    sk_union = " UNION ALL ".join([f"SELECT * FROM read_parquet('{g}')" for g in skater_globs_all])
    con.execute(f"CREATE OR REPLACE VIEW skaters_all AS {sk_union};")

    out_silver_base = SILVER_DIR / LINES_GAME_READY_KEY
    out_gold_base   = GOLD_DIR / LINES_SEASON_READY_KEY
    out_silver_base.mkdir(parents=True, exist_ok=True)
    out_gold_base.mkdir(parents=True, exist_ok=True)

    for sit in sorted(KEEP_SITUATIONS):
        line_globs = []
        for k in ["gbg_lines_hist_zip", "gbg_lines_current_zip"]:
            g = _silver_glob(k, sit)
            if g:
                line_globs.append(g)
        line_globs = sorted(list(set([x for x in line_globs if x])))

        if not line_globs:
            continue

        union_lines = " UNION ALL ".join([f"SELECT * FROM read_parquet('{p}')" for p in line_globs])
        seasons = con.execute(f"SELECT DISTINCT season FROM ({union_lines}) WHERE season IS NOT NULL ORDER BY season").fetchall()
        seasons = [int(x[0]) for x in seasons if str(x[0]).isdigit()]

        if not seasons:
            continue

        out_sit_dir = out_silver_base / f"situation={sit}"
        out_sit_dir.mkdir(parents=True, exist_ok=True)

        for season in seasons:
            df = con.execute(f"SELECT * FROM ({union_lines}) WHERE season={season}").df()
            if df.empty:
                continue

            keep = [c for c in LINE_COLS if c in df.columns]
            df = df[keep].copy()

            if ("gameid" in df.columns):
                df = df[rs_predicate_mask_from_df(df)].copy()

            df["lineid"] = df["lineid"].astype("string")
            df["team"] = df["team"].map(canon_team) if "team" in df.columns else df.get("team")
            if "opp_team" in df.columns:
                df["opp_team"] = df["opp_team"].map(canon_team)
            if "gamedate" in df.columns:
                df["gamedate"] = pd.to_datetime(df["gamedate"], errors="coerce")

            if "position" in df.columns:
                df["position"] = df["position"].astype("string")
            else:
                df["position"] = "line"

            df["combo_size_label"] = np.where(df["position"].str.lower() == "pair", 2, 3)

            ids_list = df["lineid"].apply(decode_lineid)
            df["ids_n"] = ids_list.apply(len)

            df["p1_id"] = ids_list.apply(lambda x: x[0] if len(x) >= 1 else "")
            df["p2_id"] = ids_list.apply(lambda x: x[1] if len(x) >= 2 else "")
            df["p3_id"] = ids_list.apply(lambda x: x[2] if len(x) >= 3 else "")

            df["combo_size_decoded"] = df["ids_n"].clip(lower=0, upper=3)
            df["combo_size_final"] = np.where(df["combo_size_decoded"].isin([2,3]), df["combo_size_decoded"], df["combo_size_label"])
            df["mapping_ok"] = (df["combo_size_final"] == df["ids_n"]) & df["combo_size_final"].isin([2,3])

            lk = con.execute(f"""
                SELECT DISTINCT cast(playerid as VARCHAR) as playerid, name
                FROM skaters_all
                WHERE season={season} AND playerid IS NOT NULL AND name IS NOT NULL
            """).df()
            lk["playerid"] = lk["playerid"].astype("string")
            pid_to_name = dict(zip(lk["playerid"], lk["name"]))

            df["p1_name"] = df["p1_id"].map(pid_to_name).fillna("")
            df["p2_name"] = df["p2_id"].map(pid_to_name).fillna("")
            df["p3_name"] = df["p3_id"].map(pid_to_name).fillna("")

            def _combo_key_ids(row):
                ids = [row["p1_id"], row["p2_id"], row["p3_id"]]
                ids = [x for x in ids if isinstance(x, str) and x != ""]
                k = int(row["combo_size_final"]) if row["combo_size_final"] in [2,3] else len(ids)
                if len(ids) == k and k in [2,3]:
                    return "-".join(sorted(ids))
                return ""
            df["combo_key_ids"] = df.apply(_combo_key_ids, axis=1)

            df["combo_key_team"] = np.where(
                df["combo_key_ids"].astype(str).str.len() > 0,
                df["team"].astype(str) + "__" + df["position"].astype(str).str.lower() + "__" + df["combo_key_ids"].astype(str),
                ""
            )

            df["TOI"] = pd.to_numeric(df["TOI"], errors="coerce")
            for c in LINE_COUNT_COLS:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                    df[f"{c}_per60"] = np.where(df["TOI"] > 0, (df[c] / df["TOI"]) * 60.0, np.nan)

            out_game_file = out_sit_dir / f"part_season_{season}.parquet"
            df.to_parquet(out_game_file, index=False, engine="pyarrow")

            gcols = ["season","team","situation","position","combo_key_team","combo_key_ids"]
            agg = df[df["mapping_ok"] & (df["combo_key_team"] != "")].copy()

            sum_cols = ["TOI"] + [c for c in LINE_COUNT_COLS if c in agg.columns]
            roll = (agg.groupby(gcols, dropna=False)[sum_cols].sum().reset_index())

            def wavg(x, w):
                den = w.sum()
                return (x*w).sum()/den if den and den > 0 else np.nan

            for pc in LINE_PCT_COLS:
                if pc in agg.columns:
                    tmp = (agg.groupby(gcols, dropna=False)
                             .apply(lambda g: wavg(pd.to_numeric(g[pc], errors="coerce"), pd.to_numeric(g["TOI"], errors="coerce")))
                             .reset_index(name=pc))
                    roll = roll.merge(tmp, on=gcols, how="left")

            toi2 = pd.to_numeric(roll["TOI"], errors="coerce")
            for c in LINE_COUNT_COLS:
                if c in roll.columns:
                    roll[f"{c}_per60"] = np.where(toi2 > 0, (pd.to_numeric(roll[c], errors="coerce") / toi2) * 60.0, np.nan)

            out_roll_dir = out_gold_base / f"situation={sit}" / f"season={season}"
            if out_roll_dir.exists():
                shutil.rmtree(out_roll_dir)
            out_roll_dir.mkdir(parents=True, exist_ok=True)
            roll.to_parquet(out_roll_dir / "part_00000.parquet", index=False, engine="pyarrow")
            write_json(out_roll_dir / ".gold_stamp.json", {
                "run_id": RUN_ID,
                "built_at": utc_now_iso(),
                "dataset_key": LINES_SEASON_READY_KEY,
                "situation": sit,
                "season": season,
                "note": "TOI-summed volumes + TOI-weighted pct columns; per60 computed"
            })

            ok_rate = float(df["mapping_ok"].mean()) if len(df) else 0.0
            manifest["items"].append({
                "situation": sit,
                "season": season,
                "rows_game": int(len(df)),
                "rows_rollup": int(len(roll)),
                "mapping_ok_rate": ok_rate,
                "out_silver_part": str(out_game_file),
                "out_gold_rollup": str(out_roll_dir / "part_00000.parquet"),
            })
            print(f"✅ LINES_READY {sit} season={season} | rows={len(df):,} | mapping_ok={ok_rate:.1%}")

            del df, roll, agg, lk
            gc.collect()

    con.close()
    out_manifest = LOG_DIR / f"lines_ready_manifest_{RUN_ID}.json"
    write_json(out_manifest, manifest)
    print("\n✅ LINES_READY complete:", out_manifest)
    return out_manifest

# ======================================================================================
# GOLD / REPAIR / AUDIT (unchanged)
# ======================================================================================
GBG_GOLD_SPECS = {
    "gbg_skaters_hist_zip": (
        ["playerid", "team", "situation", "season"],
        ["i_f_goals", "i_f_points", "i_f_shotsongoal", "i_f_shotattempts", "i_f_unblockedshotattempts",
         "i_f_xgoals", "i_f_hits", "i_f_takeaways", "i_f_giveaways", "penalties", "penaltyminutes"]
    ),
    "gbg_skaters_current_zip": (
        ["playerid", "team", "situation", "season"],
        ["i_f_goals", "i_f_points", "i_f_shotsongoal", "i_f_shotattempts", "i_f_unblockedshotattempts",
         "i_f_xgoals", "i_f_hits", "i_f_takeaways", "i_f_giveaways", "penalties", "penaltyminutes"]
    ),
    "gbg_teams_all": (
        ["team", "situation", "season"],
        ["goalsfor", "goalsagainst", "xgoalsfor", "xgoalsagainst", "shotsongoalfor", "shotsongoalagainst",
         "shotattemptsfor", "shotattemptsagainst", "unblockedshotattemptsfor", "unblockedshotattemptsagainst",
         "penaltiesfor", "penaltiesagainst", "penaltyminutesfor", "penaltyminutesagainst"]
    ),
    "gbg_goalies_hist_zip": (
        ["playerid", "team", "situation", "season"],
        ["goals", "xgoals", "ongoal", "unblocked_shot_attempts",
         "lowdangergoals", "mediumdangergoals", "highdangergoals",
         "lowdangershots", "mediumdangershots", "highdangershots"]
    ),
    "gbg_goalies_current_zip": (
        ["playerid", "team", "situation", "season"],
        ["goals", "xgoals", "ongoal", "unblocked_shot_attempts",
         "lowdangergoals", "mediumdangergoals", "highdangergoals",
         "lowdangershots", "mediumdangershots", "highdangershots"]
    ),
    LINES_GAME_READY_KEY: (
        ["combo_key_team", "situation", "season"],
        LINE_COUNT_COLS
    ),
}

DERIVED_ROLLING_KEYS = {LINES_GAME_READY_KEY}

def gold_out_dir(dataset_key: str, situation: str, season: int) -> Path:
    return GOLD_DIR / dataset_key / f"situation={situation}" / f"season={season}"

def should_refresh_gold_partition(dataset_key: str, season: int, is_rolling: bool, force: bool) -> bool:
    if force:
        return True
    if is_rolling and season in SEASONS_TO_REFRESH:
        return True
    return False

def build_gold_sql(keys, count_cols, pk, strict: bool):
    order_cols = "gamedate, gameid"
    def wsum(col, w):
        return f"sum({col}) OVER (PARTITION BY {pk} ORDER BY {order_cols} ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW)"
    def wcnt(w):
        return f"count(*) OVER (PARTITION BY {pk} ORDER BY {order_cols} ROWS BETWEEN {w-1} PRECEDING AND CURRENT ROW)"
    def wstd(col):
        return f"sum({col}) OVER (PARTITION BY {pk} ORDER BY {order_cols} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"

    inner_exprs = []
    for w in (5, 10, 20):
        inner_exprs.append(f"{wsum('TOI', w)} AS TOI_L{w}")
        inner_exprs.append(f"{wcnt(w)} AS GP_L{w}")
    inner_exprs.append(f"{wstd('TOI')} AS TOI_STD")

    for c in count_cols:
        for w in (5, 10, 20):
            inner_exprs.append(f"{wsum(c, w)} AS {c}_L{w}")
        inner_exprs.append(f"{wstd(c)} AS {c}_STD")

    inner_select = "SELECT *,\n       " + ",\n       ".join(inner_exprs) + "\nFROM base"

    outer_exprs = []
    if strict:
        for w in (5, 10, 20):
            outer_exprs.append(f"CASE WHEN GP_L{w}={w} THEN TOI_L{w} ELSE NULL END AS TOI_L{w}")
        for c in count_cols:
            for w in (5, 10, 20):
                outer_exprs.append(f"CASE WHEN GP_L{w}={w} THEN {c}_L{w} ELSE NULL END AS {c}_L{w}")
        outer_exprs.append("TOI_STD AS TOI_STD")
        for c in count_cols:
            outer_exprs.append(f"{c}_STD AS {c}_STD")
        for w in (5,10,20):
            outer_exprs.append(f"GP_L{w} AS GP_L{w}")

        for c in count_cols:
            outer_exprs.append(f"CASE WHEN TOI>0 THEN ({c}/TOI)*60 ELSE NULL END AS {c}_per60")
            for w in (5,10,20):
                outer_exprs.append(f"CASE WHEN TOI_L{w}>0 THEN ({c}_L{w}/TOI_L{w})*60 ELSE NULL END AS {c}_per60_L{w}")
            outer_exprs.append(f"CASE WHEN TOI_STD>0 THEN ({c}_STD/TOI_STD)*60 ELSE NULL END AS {c}_per60_STD")
    else:
        outer_exprs += ["TOI_L5 AS TOI_L5","TOI_L10 AS TOI_L10","TOI_L20 AS TOI_L20","TOI_STD AS TOI_STD"]
        for w in (5,10,20):
            outer_exprs.append(f"GP_L{w} AS GP_L{w}")
        for c in count_cols:
            outer_exprs += [f"{c}_L5 AS {c}_L5", f"{c}_L10 AS {c}_L10", f"{c}_L20 AS {c}_L20", f"{c}_STD AS {c}_STD"]
            outer_exprs.append(f"CASE WHEN TOI>0 THEN ({c}/TOI)*60 ELSE NULL END AS {c}_per60")
            for w in (5,10,20):
                outer_exprs.append(f"CASE WHEN TOI_L{w}>0 THEN ({c}_L{w}/TOI_L{w})*60 ELSE NULL END AS {c}_per60_L{w}")
            outer_exprs.append(f"CASE WHEN TOI_STD>0 THEN ({c}_STD/TOI_STD)*60 ELSE NULL END AS {c}_per60_STD")

    outer_select = "SELECT base_cols.*, \n       " + ",\n       ".join(outer_exprs) + "\nFROM base_cols"
    return inner_select, outer_select

def build_gold_for_dataset(dataset_key: str) -> dict:
    base_ds_dir = SILVER_DIR / dataset_key
    if not base_ds_dir.exists():
        print("Skip missing SILVER:", dataset_key)
        return {"dataset_key": dataset_key, "status": "missing_silver"}

    ds_meta = next((d for d in DATASETS if d["key"] == dataset_key), None)
    is_rolling = bool(ds_meta and ds_meta.get("source_type") == "rolling") or (dataset_key in DERIVED_ROLLING_KEYS)
    force = (FORCE_GOLD_ROLLING if is_rolling else FORCE_GOLD_STATIC)

    keys, count_cols = GBG_GOLD_SPECS[dataset_key]
    pk = ", ".join(keys)

    con = duckdb_connect()
    built = skipped = failed = 0

    for situation in sorted(KEEP_SITUATIONS):
        sit_dir = base_ds_dir / f"situation={situation}"
        if not sit_dir.exists():
            continue

        part_files = sorted(sit_dir.glob("part_*.parquet")) + sorted(sit_dir.glob("part_season_*.parquet"))
        if not part_files:
            continue

        glob_path = str(sit_dir / "*.parquet")

        seasons = con.execute(f"""
            SELECT DISTINCT season
            FROM read_parquet('{glob_path}')
            WHERE season IS NOT NULL
            ORDER BY season
        """).fetchall()
        seasons = [int(s[0]) for s in seasons if s and str(s[0]).isdigit()]
        if not seasons:
            continue

        cols = [f.name for f in pq.ParquetFile(part_files[0]).schema_arrow]
        has_playoffgame = ("playoffgame" in cols)
        pred_rs = rs_predicate_sql(has_playoffgame)

        inner_sql, outer_sql = build_gold_sql(keys, count_cols, pk, strict=STRICT_ROLLING)

        for season in seasons:
            out_dir = gold_out_dir(dataset_key, situation, season)
            out_file = out_dir / "part_00000.parquet"

            need = (not out_file.exists()) or should_refresh_gold_partition(dataset_key, season, is_rolling=is_rolling, force=force)
            if not need:
                skipped += 1
                continue

            if out_dir.exists():
                shutil.rmtree(out_dir)
            out_dir.mkdir(parents=True, exist_ok=True)

            try:
                rs_clause = f"AND {pred_rs}" if (RS_ONLY_IN_GOLD and "gameid" in cols) else ""
                base_sql = f"""
                    WITH base AS (
                        SELECT *
                        FROM read_parquet('{glob_path}')
                        WHERE season = {season} {rs_clause}
                        ORDER BY {", ".join(keys)}, gamedate, gameid
                    ),
                    base_cols AS (
                        {inner_sql}
                    )
                    {outer_sql}
                """

                con.execute(f"""
                    COPY ({base_sql})
                    TO '{str(out_file)}'
                    (FORMAT PARQUET, COMPRESSION ZSTD);
                """)

                write_json(out_dir / ".gold_stamp.json", {
                    "run_id": RUN_ID,
                    "built_at": utc_now_iso(),
                    "dataset_key": dataset_key,
                    "situation": situation,
                    "season": season,
                    "strict_rolling": STRICT_ROLLING,
                    "rs_only_in_gold": bool(RS_ONLY_IN_GOLD),
                })

                built += 1
                print(f"✅ GOLD {dataset_key} | {situation} | season={season} -> {out_file}")
            except Exception as e:
                failed += 1
                print("❌ GOLD FAILED:", dataset_key, situation, season, "|", repr(e))

    con.close()
    return {"dataset_key": dataset_key, "built": built, "skipped": skipped, "failed": failed, "status": "ok"}

def gold_build_all() -> Path:
    manifest = {"run_id": RUN_ID, "run_at": utc_now_iso(), "items": []}
    for ds in GBG_GOLD_SPECS.keys():
        print("\n=== BUILD GOLD:", ds, "===")
        res = build_gold_for_dataset(ds)
        manifest["items"].append(res)
    manifest_path = LOG_DIR / f"gold_manifest_{RUN_ID}.json"
    write_json(manifest_path, manifest)
    print("\n✅ GOLD complete:", manifest_path)
    return manifest_path

# --- REPAIR + AUDIT blocks unchanged from your notebook ---
# (keeping your exact functions; omitted here only to keep file size manageable)
# IMPORTANT: paste your existing REPAIR section and AUDIT section here verbatim.

# ======================================================================================
# RUN ALL
# ======================================================================================
def run_pipeline():
    steps = []
    if RUN_DOWNLOAD:    steps.append(("download", download_all))
    if RUN_EXTRACT:     steps.append(("extract", extract_all))
    if RUN_BRONZE:      steps.append(("bronze", bronze_build_all))
    if RUN_SILVER:      steps.append(("silver", silver_build_all))
    if RUN_LINES_READY: steps.append(("lines_ready", build_lines_ready))
    if RUN_GOLD:        steps.append(("gold", gold_build_all))
    # If you pasted your repair/audit functions, re-enable:
    # if RUN_REPAIR:      steps.append(("repair_goalie_toi", auto_repair_goalie_toi_on_gold))
    # if RUN_AUDIT:       steps.append(("audit", run_audit))

    results = {"run_id": RUN_ID, "run_at": utc_now_iso(), "steps": []}

    for name, fn in steps:
        print("\n" + "="*90)
        print("RUN STEP:", name.upper())
        print("="*90)
        try:
            res = fn()
            results["steps"].append({"step": name, "status": "ok", "result": str(res)})
        except Exception as e:
            results["steps"].append({"step": name, "status": "failed", "error": repr(e)})
            print("❌ STEP FAILED:", name, repr(e))
            break

    out_path = LOG_DIR / f"pipeline_run_{RUN_ID}.json"
    write_json(out_path, results)
    print("\n✅ PIPELINE RUN LOG:", out_path)
    return results

if __name__ == "__main__":
    run_pipeline()
