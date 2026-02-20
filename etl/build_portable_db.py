import os
from pathlib import Path
import duckdb
from datetime import datetime, timezone
import json

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

SRC_DB = Path(os.environ.get("SRC_DB", "./data/NHL_MoneyPuck/warehouse/nhl_warehouse.duckdb")).resolve()
OUT_DB = Path(os.environ.get("OUT_DB", "./artifacts/nhl_dashboard.duckdb")).resolve()

# Optional: limit seasons to keep deployed DB smaller
# Option A: explicit list e.g. "2023,2024,2025"
DASH_SEASONS = os.environ.get("DASH_SEASONS", "").strip()
SEASONS = [int(x) for x in DASH_SEASONS.split(",") if x.strip().isdigit()] if DASH_SEASONS else None

# Option B: cutoff e.g. "2015" (keeps season >= 2015)
DASH_MIN_SEASON = os.environ.get("DASH_MIN_SEASON", "").strip()
MIN_SEASON = int(DASH_MIN_SEASON) if DASH_MIN_SEASON.isdigit() else None

# Situations
DASH_SITUATIONS = os.environ.get("DASH_SITUATIONS", "all,5v5,5on4,4on5").strip()
SITUATIONS = [s.strip() for s in DASH_SITUATIONS.split(",") if s.strip()]

# Optional: run VACUUM to compact DB file (can reduce size)
DO_VACUUM = os.environ.get("DASH_VACUUM", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

def season_where_clause(col="season"):
    # If explicit seasons list is provided, it wins.
    if SEASONS:
        return f"{col} IN ({', '.join(map(str, SEASONS))})"
    # Else if a min season cutoff is provided, use it.
    if MIN_SEASON is not None:
        return f"{col} >= {MIN_SEASON}"
    return "TRUE"

def situation_where_clause(col="situation"):
    if not SITUATIONS:
        return "TRUE"
    quoted = ", ".join([f"'{s}'" for s in SITUATIONS])
    return f"{col} IN ({quoted})"

def table_exists(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = ?
      AND table_name = ?
    LIMIT 1
    """
    return con.execute(q, [schema, name]).fetchone() is not None

def main():
    OUT_DB.parent.mkdir(parents=True, exist_ok=True)
    if not SRC_DB.exists():
        raise FileNotFoundError(f"SRC_DB not found: {SRC_DB}")

    con = duckdb.connect(str(OUT_DB))
    con.execute("PRAGMA threads=4;")
    con.execute(f"ATTACH '{str(SRC_DB)}' AS src (READ_ONLY);")

    # ---------------------------
    # Copy DIM tables
    # ---------------------------
    con.execute("CREATE OR REPLACE TABLE dim_team   AS SELECT * FROM src.dim_team;")
    con.execute("CREATE OR REPLACE TABLE dim_player AS SELECT * FROM src.dim_player;")

    # Filter dim_game by season so app doesn’t show seasons not included in facts.
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_game AS
        SELECT *
        FROM src.dim_game
        WHERE {season_where_clause("season")};
    """)

    # Lines combo dim (if present)
    if table_exists(con, "src", "dim_line_combo"):
        con.execute(f"""
            CREATE OR REPLACE TABLE dim_line_combo AS
            SELECT *
            FROM src.dim_line_combo
            WHERE
              ({season_where_clause("first_season")} OR {season_where_clause("last_season")})
              AND {situation_where_clause("situation")};
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TABLE dim_line_combo AS
            SELECT NULL::VARCHAR AS combo_key_team WHERE FALSE;
        """)

    # ---------------------------
    # Materialize FACTS into real tables (portable)
    # ---------------------------
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

    # Lines facts (if present)
    if table_exists(con, "src", "fact_lines_game"):
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_lines_game AS
            SELECT *
            FROM src.fact_lines_game
            WHERE {season_where_clause("season")} AND {situation_where_clause("situation")};
        """)
    else:
        con.execute("CREATE OR REPLACE TABLE fact_lines_game AS SELECT NULL::VARCHAR AS combo_key_team WHERE FALSE;")

    if table_exists(con, "src", "fact_lines_season"):
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_lines_season AS
            SELECT *
            FROM src.fact_lines_season
            WHERE {season_where_clause("season")} AND {situation_where_clause("situation")};
        """)
    else:
        con.execute("CREATE OR REPLACE TABLE fact_lines_season AS SELECT NULL::VARCHAR AS combo_key_team WHERE FALSE;")

    # ---------------------------
    # Metadata / health
    # ---------------------------
    row_counts = {
        "dim_team":           con.execute("SELECT count(*) FROM dim_team").fetchone()[0],
        "dim_player":         con.execute("SELECT count(*) FROM dim_player").fetchone()[0],
        "dim_game":           con.execute("SELECT count(*) FROM dim_game").fetchone()[0],
        "dim_line_combo":     con.execute("SELECT count(*) FROM dim_line_combo").fetchone()[0],
        "fact_team_game":     con.execute("SELECT count(*) FROM fact_team_game").fetchone()[0],
        "fact_skater_game":   con.execute("SELECT count(*) FROM fact_skater_game").fetchone()[0],
        "fact_goalie_game":   con.execute("SELECT count(*) FROM fact_goalie_game").fetchone()[0],
        "fact_lines_game":    con.execute("SELECT count(*) FROM fact_lines_game").fetchone()[0],
        "fact_lines_season":  con.execute("SELECT count(*) FROM fact_lines_season").fetchone()[0],
    }

    meta = {
        "built_at": utc_now_iso(),
        "src_db": str(SRC_DB),
        "seasons": SEASONS,
        "min_season": MIN_SEASON,
        "situations": SITUATIONS,
        "row_counts": row_counts,
    }

    con.execute("CREATE OR REPLACE TABLE data_health(key VARCHAR, value VARCHAR);")
    con.execute("INSERT INTO data_health VALUES (?,?)", ["built_at", meta["built_at"]])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["src_db", meta["src_db"]])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["seasons", json.dumps(meta["seasons"])])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["min_season", str(meta["min_season"])])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["situations", json.dumps(meta["situations"])])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["row_counts", json.dumps(meta["row_counts"])])

    # Optional compaction
    if DO_VACUUM:
        # CHECKPOINT ensures everything is flushed, VACUUM can shrink the file
        con.execute("CHECKPOINT;")
        con.execute("VACUUM;")

    con.close()

    out_meta = OUT_DB.with_suffix(".meta.json")
    out_meta.write_text(json.dumps(meta, indent=2))

    # Print file size (useful in Actions logs)
    size_bytes = OUT_DB.stat().st_size if OUT_DB.exists() else 0
    print("✅ Built portable DB:", OUT_DB)
    print("✅ Size bytes:", size_bytes, "| ~GB:", round(size_bytes / (1024**3), 3))
    print("✅ Meta:", out_meta)
    print(json.dumps(row_counts, indent=2))

if __name__ == "__main__":
    main()
