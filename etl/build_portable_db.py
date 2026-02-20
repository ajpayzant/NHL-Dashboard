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

def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema','pg_catalog')
      AND table_name = ?
    LIMIT 1
    """
    return con.execute(q, [name]).fetchone() is not None

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
    # IMPORTANT: If SEASONS is set, filter dim_game so your app doesn't show seasons not included in facts.
    for t in ["dim_team", "dim_player"]:
        con.execute(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM src.{t};")

    con.execute(f"""
        CREATE OR REPLACE TABLE dim_game AS
        SELECT *
        FROM src.dim_game
        WHERE {season_where_clause("season")};
    """)

    # Lines combo dim (if present)
    if table_exists(con, "src.dim_line_combo") or table_exists(con, "dim_line_combo"):
        con.execute(f"""
            CREATE OR REPLACE TABLE dim_line_combo AS
            SELECT *
            FROM src.dim_line_combo
            WHERE
              ({season_where_clause("first_season")} OR {season_where_clause("last_season")})
              AND {situation_where_clause("situation")};
        """)
    else:
        # keep the DB build from failing if warehouse didn't include it yet
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

    # Lines facts (NEW)
    # If the warehouse didn't have them for some reason, create empty tables rather than fail.
    if table_exists(con, "src.fact_lines_game") or table_exists(con, "fact_lines_game"):
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_lines_game AS
            SELECT *
            FROM src.fact_lines_game
            WHERE {season_where_clause("season")} AND {situation_where_clause("situation")};
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TABLE fact_lines_game AS
            SELECT NULL::VARCHAR AS combo_key_team WHERE FALSE;
        """)

    if table_exists(con, "src.fact_lines_season") or table_exists(con, "fact_lines_season"):
        con.execute(f"""
            CREATE OR REPLACE TABLE fact_lines_season AS
            SELECT *
            FROM src.fact_lines_season
            WHERE {season_where_clause("season")} AND {situation_where_clause("situation")};
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TABLE fact_lines_season AS
            SELECT NULL::VARCHAR AS combo_key_team WHERE FALSE;
        """)

    # ---------------------------
    # Metadata / health
    # ---------------------------
    row_counts = {
        "dim_team":        con.execute("SELECT count(*) FROM dim_team").fetchone()[0],
        "dim_player":      con.execute("SELECT count(*) FROM dim_player").fetchone()[0],
        "dim_game":        con.execute("SELECT count(*) FROM dim_game").fetchone()[0],
        "dim_line_combo":  con.execute("SELECT count(*) FROM dim_line_combo").fetchone()[0],
        "fact_team_game":  con.execute("SELECT count(*) FROM fact_team_game").fetchone()[0],
        "fact_skater_game":con.execute("SELECT count(*) FROM fact_skater_game").fetchone()[0],
        "fact_goalie_game":con.execute("SELECT count(*) FROM fact_goalie_game").fetchone()[0],
        "fact_lines_game": con.execute("SELECT count(*) FROM fact_lines_game").fetchone()[0],
        "fact_lines_season": con.execute("SELECT count(*) FROM fact_lines_season").fetchone()[0],
    }

    meta = {
        "built_at": utc_now_iso(),
        "src_db": str(SRC_DB),
        "seasons": SEASONS,
        "situations": SITUATIONS,
        "row_counts": row_counts,
    }

    # A small key/value table is handy inside the app
    con.execute("CREATE OR REPLACE TABLE data_health(key VARCHAR, value VARCHAR);")
    con.execute("INSERT INTO data_health VALUES (?,?)", ["built_at", meta["built_at"]])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["src_db", meta["src_db"]])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["seasons", json.dumps(meta["seasons"])])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["situations", json.dumps(meta["situations"])])
    con.execute("INSERT INTO data_health VALUES (?,?)", ["row_counts", json.dumps(meta["row_counts"])])

    con.close()

    # Write JSON beside DB for app + debugging
    out_meta = OUT_DB.with_suffix(".meta.json")
    out_meta.write_text(json.dumps(meta, indent=2))
    print("✅ Built portable DB:", OUT_DB)
    print("✅ Meta:", out_meta)
    print(json.dumps(row_counts, indent=2))

if __name__ == "__main__":
    main()
