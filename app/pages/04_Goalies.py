import streamlit as st
from shared import DB_PATH, query_df, sidebar_filters, find_relation_with_cols

st.title("🥅 Goalies")

season, situation = sidebar_filters(str(DB_PATH))

GOALIE_REQ = ["playerid", "name", "team", "season", "situation", "gamedate", "gameid"]
goalie_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=GOALIE_REQ,
    prefer_names=("fact_goalie_game", "v_goalies_src", "fact_goalies_game"),
)
if not goalie_rel:
    st.error(f"Missing goalie fact relation with required columns: {GOALIE_REQ}")
    st.stop()

goalies = query_df(
    str(DB_PATH),
    f"""
    SELECT DISTINCT playerid, name
    FROM {goalie_rel}
    WHERE name IS NOT NULL AND playerid IS NOT NULL
    ORDER BY name
    """
)
name_to_id = dict(zip(goalies["name"], goalies["playerid"]))
goalie_name = st.selectbox("Goalie", goalies["name"].tolist())
goalie_id = name_to_id[goalie_name]

summary = query_df(
    str(DB_PATH),
    f"""
    SELECT
      season,
      situation,
      MAX(team) AS team,
      COUNT(*) AS gp
    FROM {goalie_rel}
    WHERE playerid = ?
    GROUP BY season, situation
    ORDER BY season DESC, situation;
    """,
    (goalie_id,)
)

st.subheader("Season / Situation availability")
st.dataframe(summary, use_container_width=True)

st.subheader("Game log (filtered)")
log = query_df(
    str(DB_PATH),
    f"""
    SELECT *
    FROM {goalie_rel}
    WHERE playerid = ? AND season = ? AND situation = ?
    ORDER BY gamedate, gameid;
    """,
    (goalie_id, season, situation)
)

st.caption(f"Source: {goalie_rel}")
st.dataframe(log, use_container_width=True)
