# ======================================================================================
# DASHBOARD WAREHOUSE BUILD (DuckDB)
#   - Builds: dim_player, dim_team, dim_game, dim_line_combo
#   - Creates: fact views: fact_team_game, fact_skater_game, fact_goalie_game, fact_lines_game, fact_lines_season
#   - Writes:  <DATA_ROOT>/warehouse/nhl_warehouse.duckdb
#
# CHANGES vs your notebook:
#   - no Colab paths; uses NHL_DATA_ROOT + PIPELINE_TAG to find GOLD
#   - includes lines_game_ready + lines_season_ready
# ======================================================================================

import os, glob
from pathlib import Path
import duckdb
import pyarrow.parquet as pq

# ---------- CONFIG ----------
DATA_ROOT = Path(os.environ.get("NHL_DATA_ROOT", "./data/NHL_MoneyPuck")).resolve()
PIPELINE_TAG = os.environ.get("PIPELINE_TAG", "v3")

def _tag(name: str) -> str:
    return f"{name}_{PIPELINE_TAG}" if PIPELINE_TAG else name

GOLD_DIR = DATA_ROOT / _tag("gold")

WAREHOUSE_DIR = Path(os.environ.get("NHL_WAREHOUSE_DIR", str(DATA_ROOT / "warehouse"))).resolve()
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(os.environ.get("NHL_WAREHOUSE_DB", str(WAREHOUSE_DIR / "nhl_warehouse.duckdb"))).resolve()

DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS", "4"))
DUCKDB_MEM_LIMIT = os.environ.get("DUCKDB_MEM_LIMIT", "6GB")

def _list_files(pattern: str) -> list[str]:
    return sorted(glob.glob(pattern))

def _sql_list(files: list[str]) -> str:
    return "[" + ", ".join([f"'{f}'" for f in files]) + "]"

def _sample_cols(file_path: str) -> set[str]:
    return set([f.name for f in pq.ParquetFile(file_path).schema_arrow])

print("Warehouse DB will be written to:", DB_PATH)
print("GOLD_DIR:", GOLD_DIR)

# ---------- COLLECT GOLD PART FILES ----------
skater_files = (
    _list_files(str(GOLD_DIR / "gbg_skaters_hist_zip"   / "situation=all" / "season=*" / "part_*.parquet")) +
    _list_files(str(GOLD_DIR / "gbg_skaters_current_zip"/ "situation=all" / "season=*" / "part_*.parquet"))
)
goalie_files = (
    _list_files(str(GOLD_DIR / "gbg_goalies_hist_zip"   / "situation=all" / "season=*" / "part_*.parquet")) +
    _list_files(str(GOLD_DIR / "gbg_goalies_current_zip"/ "situation=all" / "season=*" / "part_*.parquet"))
)
team_files = _list_files(str(GOLD_DIR / "gbg_teams_all" / "situation=all" / "season=*" / "part_*.parquet"))

# Lines (NEW)
lines_game_files = _list_files(str(GOLD_DIR / "lines_game_ready" / "situation=*" / "season=*" / "part_*.parquet"))
lines_season_files = _list_files(str(GOLD_DIR / "lines_season_ready" / "situation=*" / "season=*" / "part_*.parquet"))

if not skater_files:
    raise FileNotFoundError("No GOLD skater parquet files found.")
if not goalie_files:
    raise FileNotFoundError("No GOLD goalie parquet files found.")
if not team_files:
    raise FileNotFoundError("No GOLD team parquet files found.")

# Detect goalie ID column safely
g_cols = _sample_cols(goalie_files[0])
if "playerid" in g_cols:
    GOALIE_ID_COL = "playerid"
elif "goalieid" in g_cols:
    GOALIE_ID_COL = "goalieid"
else:
    raise ValueError(f"Goalie parquet missing both playerid and goalieid. Columns seen: {sorted(list(g_cols))[:50]}")

# Detect team columns for game build
t_cols = _sample_cols(team_files[0])
HAS_OPP = ("opp_team" in t_cols)
HAS_HOA = ("home_or_away" in t_cols)

print(f"Found GOLD files | skaters={len(skater_files)} goalies={len(goalie_files)} teams={len(team_files)}")
print(f"Found GOLD lines | lines_game_ready={len(lines_game_files)} lines_season_ready={len(lines_season_files)}")
print("Goalie id column:", GOALIE_ID_COL)
print("Team has opp_team:", HAS_OPP, "| has home_or_away:", HAS_HOA)

# ---------- CONNECT DUCKDB ----------
con = duckdb.connect(str(DB_PATH))
con.execute(f"PRAGMA threads={DUCKDB_THREADS};")
con.execute(f"PRAGMA memory_limit='{DUCKDB_MEM_LIMIT}';")

# ---------- SOURCE VIEWS ----------
con.execute(f"""
CREATE OR REPLACE VIEW v_skaters_src AS
SELECT * FROM read_parquet({_sql_list(skater_files)}, union_by_name=true);
""")

con.execute(f"""
CREATE OR REPLACE VIEW v_goalies_src AS
SELECT * FROM read_parquet({_sql_list(goalie_files)}, union_by_name=true);
""")

con.execute(f"""
CREATE OR REPLACE VIEW v_teams_src AS
SELECT * FROM read_parquet({_sql_list(team_files)}, union_by_name=true);
""")

# Lines views (NEW) — only create if files exist
if lines_game_files:
    con.execute(f"""
    CREATE OR REPLACE VIEW v_lines_game_src AS
    SELECT * FROM read_parquet({_sql_list(lines_game_files)}, union_by_name=true);
    """)
else:
    con.execute("CREATE OR REPLACE VIEW v_lines_game_src AS SELECT NULL::VARCHAR AS combo_key_team WHERE FALSE;")

if lines_season_files:
    con.execute(f"""
    CREATE OR REPLACE VIEW v_lines_season_src AS
    SELECT * FROM read_parquet({_sql_list(lines_season_files)}, union_by_name=true);
    """)
else:
    con.execute("CREATE OR REPLACE VIEW v_lines_season_src AS SELECT NULL::VARCHAR AS combo_key_team WHERE FALSE;")

# ---------- dim_player (same fixed union-by-shape logic) ----------
con.execute(f"""
CREATE OR REPLACE TABLE dim_player AS
WITH
sk AS (
    SELECT
        CAST(playerid AS VARCHAR)        AS playerid,
        CAST(name AS VARCHAR)            AS name,
        CAST(position AS VARCHAR)        AS position,
        CAST(team AS VARCHAR)            AS team,
        CAST(season AS INTEGER)          AS season,
        CAST(gamedate AS DATE)           AS gamedate
    FROM v_skaters_src
    WHERE playerid IS NOT NULL AND CAST(playerid AS VARCHAR) <> ''
),
go AS (
    SELECT
        CAST({GOALIE_ID_COL} AS VARCHAR) AS playerid,
        CAST(name AS VARCHAR)            AS name,
        'G'                              AS position,
        CAST(team AS VARCHAR)            AS team,
        CAST(season AS INTEGER)          AS season,
        CAST(gamedate AS DATE)           AS gamedate
    FROM v_goalies_src
    WHERE {GOALIE_ID_COL} IS NOT NULL AND CAST({GOALIE_ID_COL} AS VARCHAR) <> ''
),
base AS (
    SELECT * FROM sk
    UNION ALL BY NAME
    SELECT * FROM go
)
SELECT
    playerid,
    arg_max(name,     COALESCE(gamedate, DATE '1900-01-01')) AS name,
    arg_max(position, COALESCE(gamedate, DATE '1900-01-01')) AS position,
    arg_max(team,     COALESCE(gamedate, DATE '1900-01-01')) AS team_current,
    MIN(season) AS first_season,
    MAX(season) AS last_season,
    COUNT(*)    AS fact_rows,
    COUNT(DISTINCT team) AS teams_seen
FROM base
GROUP BY playerid;
""")

# ---------- dim_team ----------
con.execute("""
CREATE OR REPLACE TABLE dim_team AS
SELECT DISTINCT CAST(team AS VARCHAR) AS team
FROM v_teams_src
WHERE team IS NOT NULL AND CAST(team AS VARCHAR) <> ''
ORDER BY team;
""")

# ---------- dim_game ----------
home_away_logic = """
CASE
  WHEN home_or_away IS NULL THEN NULL
  WHEN lower(CAST(home_or_away AS VARCHAR)) IN ('home','h') OR lower(CAST(home_or_away AS VARCHAR)) LIKE '%home%' THEN 1
  WHEN lower(CAST(home_or_away AS VARCHAR)) IN ('away','a') OR lower(CAST(home_or_away AS VARCHAR)) LIKE '%away%' THEN 0
  ELSE NULL
END
""" if HAS_HOA else "NULL"

opp_expr = "CAST(opp_team AS VARCHAR) AS opp_team," if HAS_OPP else "NULL AS opp_team,"
hoa_expr = "CAST(home_or_away AS VARCHAR) AS home_or_away," if HAS_HOA else "NULL AS home_or_away,"

con.execute(f"""
CREATE OR REPLACE TABLE dim_game AS
WITH t AS (
    SELECT
        CAST(gameid AS VARCHAR)   AS gameid,
        CAST(season AS INTEGER)   AS season,
        CAST(gamedate AS DATE)    AS gamedate,
        CAST(team AS VARCHAR)     AS team,
        {opp_expr}
        {hoa_expr}
        TRY_CAST(goalsfor AS INTEGER)     AS goalsfor,
        TRY_CAST(goalsagainst AS INTEGER) AS goalsagainst,
        TRY_CAST(xgoalsfor AS DOUBLE)     AS xgoalsfor,
        TRY_CAST(xgoalsagainst AS DOUBLE) AS xgoalsagainst
    FROM v_teams_src
    WHERE gameid IS NOT NULL
),
t2 AS (
    SELECT
        *,
        {home_away_logic} AS is_home
    FROM t
)
SELECT
    gameid,
    MAX(season)   AS season,
    MAX(gamedate) AS gamedate,
    MAX(CASE WHEN is_home=1 THEN team END) AS home_team,
    MAX(CASE WHEN is_home=0 THEN team END) AS away_team,
    MAX(CASE WHEN is_home=1 THEN goalsfor END) AS home_goals,
    MAX(CASE WHEN is_home=0 THEN goalsfor END) AS away_goals,
    MAX(CASE WHEN is_home=1 THEN xgoalsfor END) AS home_xgoals,
    MAX(CASE WHEN is_home=0 THEN xgoalsfor END) AS away_xgoals
FROM t2
GROUP BY gameid;
""")

# ---------- dim_line_combo (NEW; useful for app filters + combo pages) ----------
# Uses season rollups if available; falls back to game-ready if season rollups absent.
con.execute("""
CREATE OR REPLACE TABLE dim_line_combo AS
WITH base AS (
    SELECT
        CAST(combo_key_team AS VARCHAR) AS combo_key_team,
        CAST(combo_key_ids  AS VARCHAR) AS combo_key_ids,
        CAST(team AS VARCHAR)           AS team,
        CAST(position AS VARCHAR)       AS position,
        CAST(situation AS VARCHAR)      AS situation,
        CAST(season AS INTEGER)         AS season,
        TRY_CAST(TOI AS DOUBLE)         AS TOI
    FROM v_lines_season_src
    WHERE combo_key_team IS NOT NULL AND CAST(combo_key_team AS VARCHAR) <> ''
    UNION ALL
    SELECT
        CAST(combo_key_team AS VARCHAR) AS combo_key_team,
        CAST(combo_key_ids  AS VARCHAR) AS combo_key_ids,
        CAST(team AS VARCHAR)           AS team,
        CAST(position AS VARCHAR)       AS position,
        CAST(situation AS VARCHAR)      AS situation,
        CAST(season AS INTEGER)         AS season,
        TRY_CAST(TOI AS DOUBLE)         AS TOI
    FROM v_lines_game_src
    WHERE combo_key_team IS NOT NULL AND CAST(combo_key_team AS VARCHAR) <> ''
)
SELECT
    combo_key_team,
    arg_max(combo_key_ids, season) AS combo_key_ids,
    arg_max(team, season)          AS team,
    arg_max(position, season)      AS position,
    arg_max(situation, season)     AS situation,
    MIN(season) AS first_season,
    MAX(season) AS last_season,
    COUNT(*)    AS fact_rows,
    SUM(COALESCE(TOI,0)) AS toi_total
FROM base
GROUP BY combo_key_team;
""")

# ---------- Fact views ----------
con.execute("CREATE OR REPLACE VIEW fact_team_game   AS SELECT * FROM v_teams_src;")
con.execute("CREATE OR REPLACE VIEW fact_skater_game AS SELECT * FROM v_skaters_src;")
con.execute("CREATE OR REPLACE VIEW fact_goalie_game AS SELECT * FROM v_goalies_src;")

# Lines facts (NEW)
con.execute("CREATE OR REPLACE VIEW fact_lines_game   AS SELECT * FROM v_lines_game_src;")
con.execute("CREATE OR REPLACE VIEW fact_lines_season AS SELECT * FROM v_lines_season_src;")

# ---------- SANITY PRINTS ----------
n_players = con.execute("SELECT COUNT(*) FROM dim_player;").fetchone()[0]
n_teams   = con.execute("SELECT COUNT(*) FROM dim_team;").fetchone()[0]
n_games   = con.execute("SELECT COUNT(*) FROM dim_game;").fetchone()[0]
n_combos  = con.execute("SELECT COUNT(*) FROM dim_line_combo;").fetchone()[0]

print("\n✅ Warehouse build complete")
print("DB_PATH:", DB_PATH)
print("dim_player rows:", n_players)
print("dim_team rows:", n_teams)
print("dim_game rows:", n_games)
print("dim_line_combo rows:", n_combos)

print("\nSample dim_player:")
print(con.execute("SELECT * FROM dim_player ORDER BY last_season DESC, fact_rows DESC LIMIT 10;").df())

print("\nSample dim_line_combo:")
print(con.execute("SELECT * FROM dim_line_combo ORDER BY toi_total DESC NULLS LAST LIMIT 10;").df())

con.close()
