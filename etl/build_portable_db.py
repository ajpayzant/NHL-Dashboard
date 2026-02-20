# etl/build_portable_db.py
import os
from pathlib import Path
import duckdb
from datetime import datetime, timezone
import json

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

SRC_DB = Path(os.environ.get("SRC_DB", "./data/NHL_MoneyPuck/warehouse/nhl_warehouse.duckdb")).resolve()
OUT_DB = Path(os.environ.get("OUT_DB", "./artifacts/nhl_dashboard.duckdb")).resolve()

# Optional: limit seasons to keep the deployed DB smaller
# Example: "2023,2024,2025"
DASH_SEASONS = os.environ.get("DASH_SEASONS", "").strip()
SEASONS = [int(x) for x in DASH_SEASONS.split(",") if x.strip().isdigit()] if DASH_SEASONS else None

# Keep the situations you already standardized to
DASH_SITUATIONS = os.environ.get("DASH_SITUATIONS", "all,5v5,5on4,4on5").strip()
SITUATIONS = [s.strip() for s in DASH_SITUATIONS.split(",") if s.strip()]

def season_where_clause(col="season"):
    if not SEASONS:
        return "TRUE"
    return f"{col} IN ({', '.join(map(str, SEASONS))})"

def situation_where_clause(col="situation"):
    if not SITUATIONS:
        return "TRUE"
    quoted = ", ".join([f"'{s}'" for s in SITUATIONS])
    return f"{col} IN ({quoted})"

def main():
    OUT_DB.parent.mkdir(parents=True, exist_ok=True)
    if not SRC_DB.exists():
        raise FileNotFoundError(f"SRC_DB not found: {SRC_DB}")

    con = duckdb.connect(str(OUT_DB))
    con.execute("PRAGMA threads=4;")
    con.execute(f"ATTACH '{str(SRC_DB)}' AS src (READ_ONLY);")

    # Copy dimension tables (already BASE TABLES in your warehouse)
    for t in ["dim_team", "dim_player", "dim_game"]:
        con.execute(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM src.{t};")

    # Materialize fact views into real tables (THIS is the “portable” part)
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_team_game AS
        SELECT *
        FROM src.fact_team_game
        WHERE {season_where_clause("season")} AND {situation_where_clause("situation")};
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_skater_game AS
        SELECT *
        FROM src.fact_skater_game
        WHERE {season_where_clause("season")} AND {situation_where_clause("situation")};
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_goalie_game AS
        SELECT *
        FROM src.fact_goalie_game
        WHERE {season_where_clause("season")} AND {situation_where_clause("situation")};
    """)

    # Metadata / health
    meta = {
        "built_at": utc_now_iso(),
        "src_db": str(SRC_DB),
        "seasons": SEASONS,
        "situations": SITUATIONS,
        "row_counts": {
            "dim_team": con.execute("SELECT count(*) FROM dim_team").fetchone()[0],
            "dim_player": con.execute("SELECT count(*) FROM dim_player").fetchone()[0],
            "dim_game": con.execute("SELECT count(*) FROM dim_game").fetchone()[0],
            "fact_team_game": con.execute("SELECT count(*) FROM fact_team_game").fetchone()[0],
            "fact_skater_game": con.execute("SELECT count(*) FROM fact_skater_game").fetchone()[0],
            "fact_goalie_game": con.execute("SELECT count(*) FROM fact_goalie_game").fetchone()[0],
        },
    }

    con.execute("CREATE OR REPLACE TABLE data_health AS SELECT * FROM (VALUES (?, ?)) v(key, value);", ["built_at", meta["built_at"]])
    con.close()

    # Write JSON beside DB for app + debugging
    out_meta = OUT_DB.with_suffix(".meta.json")
    out_meta.write_text(json.dumps(meta, indent=2))
    print("✅ Built portable DB:", OUT_DB)
    print("✅ Meta:", out_meta)
    print(json.dumps(meta["row_counts"], indent=2))

if __name__ == "__main__":
    main()
