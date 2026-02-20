import streamlit as st
import pandas as pd
from shared import DB_PATH, query_df, sidebar_filters, find_relation_with_cols

st.title("📊 League Table")

season, situation = sidebar_filters(str(DB_PATH))

TEAM_REQ = ["team", "season", "situation", "goalsfor", "goalsagainst", "xgoalsfor", "xgoalsagainst"]
team_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=TEAM_REQ,
    prefer_names=("fact_team_game", "v_teams_src", "fact_teams_game"),
)
if not team_rel:
    st.error(f"Missing team fact relation with required columns: {TEAM_REQ}")
    st.stop()

sql = f"""
WITH base AS (
  SELECT *
  FROM {team_rel}
  WHERE season = ? AND situation = ?
),
agg AS (
  SELECT
    team,
    COUNT(*) AS gp,
    SUM(TRY_CAST(goalsfor AS DOUBLE))      AS gf,
    SUM(TRY_CAST(goalsagainst AS DOUBLE))  AS ga,
    SUM(TRY_CAST(xgoalsfor AS DOUBLE))     AS xgf,
    SUM(TRY_CAST(xgoalsagainst AS DOUBLE)) AS xga
  FROM base
  GROUP BY team
)
SELECT
  team,
  gp,
  gf,
  ga,
  ROUND(gf/gp, 3) AS gf_pg,
  ROUND(ga/gp, 3) AS ga_pg,
  xgf,
  xga,
  ROUND(xgf / NULLIF(xgf + xga, 0), 3) AS xgf_pct
FROM agg
ORDER BY xgf_pct DESC NULLS LAST;
"""

df = query_df(str(DB_PATH), sql, (season, situation))

st.caption(f"Source: {team_rel} | Season={season} | Situation={situation}")
st.dataframe(df, use_container_width=True)

st.subheader("xGF% distribution")
if not df.empty and "xgf_pct" in df.columns:
    st.bar_chart(df.set_index("team")["xgf_pct"])
