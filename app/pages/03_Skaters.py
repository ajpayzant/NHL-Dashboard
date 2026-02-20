import streamlit as st
from shared import DB_PATH, query_df, sidebar_filters, find_relation_with_cols

st.title("🧑‍🦱 Skaters")

season, situation = sidebar_filters(str(DB_PATH))

SKATER_REQ = ["playerid", "name", "team", "season", "situation", "gamedate", "gameid"]
skater_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=SKATER_REQ,
    prefer_names=("fact_skater_game", "v_skaters_src", "fact_skaters_game"),
)
if not skater_rel:
    st.error(f"Missing skater fact relation with required columns: {SKATER_REQ}")
    st.stop()

# player picker (limit list for UI speed)
players = query_df(
    str(DB_PATH),
    f"""
    SELECT DISTINCT playerid, name
    FROM {skater_rel}
    WHERE name IS NOT NULL AND playerid IS NOT NULL
    ORDER BY name
    """
)
name_to_id = dict(zip(players["name"], players["playerid"]))
player_name = st.selectbox("Player", players["name"].tolist())
player_id = name_to_id[player_name]

# season summary (basic)
summary = query_df(
    str(DB_PATH),
    f"""
    SELECT
      season,
      situation,
      MAX(team) AS team,
      COUNT(*) AS gp
    FROM {skater_rel}
    WHERE playerid = ?
    GROUP BY season, situation
    ORDER BY season DESC, situation;
    """,
    (player_id,)
)

st.subheader("Season / Situation availability")
st.dataframe(summary, use_container_width=True)

st.subheader("Game log (filtered)")
log = query_df(
    str(DB_PATH),
    f"""
    SELECT *
    FROM {skater_rel}
    WHERE playerid = ? AND season = ? AND situation = ?
    ORDER BY gamedate, gameid;
    """,
    (player_id, season, situation)
)

st.caption(f"Source: {skater_rel}")
st.dataframe(log, use_container_width=True)
