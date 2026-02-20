import streamlit as st
from datetime import date
from shared import DB_PATH, query_df, sidebar_filters, find_relation_with_cols

st.title("🗓️ Game Browser")

season, situation = sidebar_filters(str(DB_PATH))

TEAM_REQ = ["team", "season", "situation", "gamedate", "gameid", "goalsfor", "goalsagainst"]
team_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=TEAM_REQ,
    prefer_names=("fact_team_game", "v_teams_src", "fact_teams_game"),
)
if not team_rel:
    st.error(f"Missing team fact relation with required columns: {TEAM_REQ}")
    st.stop()

teams = query_df(str(DB_PATH), f"SELECT DISTINCT team FROM {team_rel} ORDER BY team")["team"].tolist()
team_filter = st.multiselect("Teams (optional)", teams, default=[])

# date bounds from data
bounds = query_df(
    str(DB_PATH),
    f"""
    SELECT MIN(gamedate) AS dmin, MAX(gamedate) AS dmax
    FROM {team_rel}
    WHERE season = ? AND situation = ?;
    """,
    (season, situation)
)
dmin = bounds.loc[0, "dmin"]
dmax = bounds.loc[0, "dmax"]

c1, c2 = st.columns(2)
with c1:
    start = st.date_input("Start date", value=dmin if dmin else date(season, 10, 1))
with c2:
    end = st.date_input("End date", value=dmax if dmax else date(season + 1, 4, 30))

where_team = ""
params = [season, situation, start, end]
if team_filter:
    placeholders = ", ".join(["?"] * len(team_filter))
    where_team = f" AND team IN ({placeholders}) "
    params += team_filter

sql = f"""
SELECT
  gamedate,
  gameid,
  team,
  TRY_CAST(goalsfor AS INTEGER) AS gf,
  TRY_CAST(goalsagainst AS INTEGER) AS ga
FROM {team_rel}
WHERE season = ?
  AND situation = ?
  AND gamedate >= ?
  AND gamedate <= ?
  {where_team}
ORDER BY gamedate DESC, gameid DESC
LIMIT 500;
"""

df = query_df(str(DB_PATH), sql, tuple(params))
st.caption(f"Source: {team_rel}")
st.dataframe(df, use_container_width=True)
