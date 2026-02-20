import streamlit as st
from shared import DB_PATH, query_df, sidebar_filters, find_relation_with_cols

st.title("🧩 Lines / Pairs")

season, situation = sidebar_filters(str(DB_PATH))

# Your pipeline creates derived lines keys; portable DB must include a relation for it.
LINES_REQ = ["combo_key_team", "team", "season", "situation", "TOI"]
lines_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=LINES_REQ,
    prefer_names=("fact_lines_game", "lines_game_ready", "fact_line_game", "v_lines_src"),
)

if not lines_rel:
    st.error(
        "I can't find lines data inside your portable DB.\n\n"
        "Fix: update `etl/build_portable_db.py` to copy the lines relation into the dashboard DB "
        "(e.g., create `fact_lines_game` as a table/view in the portable DB)."
    )
    st.stop()

teams = query_df(str(DB_PATH), f"SELECT DISTINCT team FROM {lines_rel} ORDER BY team")["team"].tolist()
team = st.selectbox("Team", teams)

sql = f"""
WITH base AS (
  SELECT *
  FROM {lines_rel}
  WHERE season = ? AND situation = ? AND team = ? AND combo_key_team IS NOT NULL AND combo_key_team <> ''
),
agg AS (
  SELECT
    combo_key_team,
    MAX(COALESCE(position, 'line')) AS position,
    SUM(TRY_CAST(TOI AS DOUBLE)) AS toi,
    SUM(TRY_CAST(xgoalsfor AS DOUBLE)) AS xgf,
    SUM(TRY_CAST(xgoalsagainst AS DOUBLE)) AS xga,
    SUM(TRY_CAST(goalsfor AS DOUBLE)) AS gf,
    SUM(TRY_CAST(goalsagainst AS DOUBLE)) AS ga
  FROM base
  GROUP BY combo_key_team
)
SELECT
  combo_key_team,
  position,
  toi,
  gf, ga,
  xgf, xga,
  ROUND(xgf / NULLIF(xgf + xga, 0), 3) AS xgf_pct,
  ROUND(gf / NULLIF(toi, 0) * 60, 3) AS gf_per60,
  ROUND(ga / NULLIF(toi, 0) * 60, 3) AS ga_per60
FROM agg
ORDER BY toi DESC
LIMIT 50;
"""

df = query_df(str(DB_PATH), sql, (season, situation, team))
st.caption(f"Source: {lines_rel}")
st.dataframe(df, use_container_width=True)
