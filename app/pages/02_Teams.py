import streamlit as st
from shared import DB_PATH, query_df, sidebar_filters, find_relation_with_cols

st.title("🏟️ Teams")

season, situation = sidebar_filters(str(DB_PATH))

TEAM_REQ = ["team", "season", "situation", "gamedate", "gameid", "goalsfor", "goalsagainst", "xgoalsfor", "xgoalsagainst"]
team_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=TEAM_REQ,
    prefer_names=("fact_team_game", "v_teams_src", "fact_teams_game"),
)
if not team_rel:
    st.error(f"Missing team fact relation with required columns: {TEAM_REQ}")
    st.stop()

teams = query_df(str(DB_PATH), f"SELECT DISTINCT team FROM {team_rel} ORDER BY team")["team"].tolist()
team = st.selectbox("Team", teams)

sql_log = f"""
SELECT
  gamedate,
  gameid,
  team,
  TRY_CAST(goalsfor AS INTEGER) AS gf,
  TRY_CAST(goalsagainst AS INTEGER) AS ga,
  TRY_CAST(xgoalsfor AS DOUBLE) AS xgf,
  TRY_CAST(xgoalsagainst AS DOUBLE) AS xga
FROM {team_rel}
WHERE season = ? AND situation = ? AND team = ?
ORDER BY gamedate, gameid;
"""
log = query_df(str(DB_PATH), sql_log, (season, situation, team))

st.caption(f"Source: {team_rel}")
st.dataframe(log, use_container_width=True)

st.subheader("Trends")
if not log.empty:
    log2 = log.copy()
    log2["xgf_pct"] = log2["xgf"] / (log2["xgf"] + log2["xga"])
    log2 = log2.set_index("gamedate")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.line_chart(log2[["gf", "ga"]])
    with c2:
        st.line_chart(log2[["xgf", "xga"]])
    with c3:
        st.line_chart(log2[["xgf_pct"]])
else:
    st.info("No rows for that filter selection.")
