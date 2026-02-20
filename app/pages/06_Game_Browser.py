# app/pages/06_Game_Browser.py
import streamlit as st

from shared import (
    DB_PATH, top_filter_bar, query_df, find_relation_with_cols, cols_of, prep_table_for_display, relation_exists
)

st.header("Game Browser")

season, situation = top_filter_bar(str(DB_PATH))

if not relation_exists(str(DB_PATH), "dim_game"):
    st.error("dim_game not found in portable DB. Copy it into the portable DB to enable the game browser.")
    st.stop()

games = query_df(
    str(DB_PATH),
    """
    SELECT gameid, gamedate, home_team, away_team, home_goals, away_goals
    FROM dim_game
    WHERE season=?
    ORDER BY gamedate DESC
    """,
    (season,),
)

if games.empty:
    st.warning("No games found for that season.")
    st.stop()

games_disp = games.copy()
games_disp["label"] = games_disp.apply(
    lambda r: f"{str(r['gamedate'])[:10]} — {r['away_team']} @ {r['home_team']} ({r['away_goals']}-{r['home_goals']})",
    axis=1
)

pick = st.selectbox("Select game", games_disp["label"].tolist(), index=0)
gameid = games_disp.loc[games_disp["label"] == pick, "gameid"].iloc[0]

TEAM_REQ = ["gameid","team","season","situation","gamedate"]
team_rel = find_relation_with_cols(str(DB_PATH), TEAM_REQ, prefer_names=("fact_team_game","v_teams_src"))
SK_REQ = ["gameid","playerid","name","team","season","situation","gamedate"]
sk_rel = find_relation_with_cols(str(DB_PATH), SK_REQ, prefer_names=("fact_skater_game","v_skaters_src"))
GO_REQ = ["gameid","playerid","name","team","season","situation","gamedate"]
go_rel = find_relation_with_cols(str(DB_PATH), GO_REQ, prefer_names=("fact_goalie_game","v_goalies_src"))

tabs = st.tabs(["Team Totals", "Skaters", "Goalies"])

with tabs[0]:
    if not team_rel:
        st.info("Team fact relation not found.")
    else:
        cols = set(cols_of(str(DB_PATH), team_rel))
        want = [c for c in ["gamedate","team","goalsfor","goalsagainst","xgoalsfor","xgoalsagainst","shotsongoalfor","shotsongoalagainst","shotattemptsfor","shotattemptsagainst"] if c in cols]
        df = query_df(
            str(DB_PATH),
            f"""
            SELECT {", ".join(want)}
            FROM {team_rel}
            WHERE season=? AND situation=? AND gameid=?
            """,
            (season, situation, str(gameid)),
        )
        st.dataframe(prep_table_for_display(df), width="stretch")

with tabs[1]:
    if not sk_rel:
        st.info("Skater fact relation not found.")
    else:
        cols = set(cols_of(str(DB_PATH), sk_rel))
        want = [c for c in ["gamedate","team","name","TOI","i_f_goals","i_f_points","i_f_shotsongoal","i_f_xgoals"] if c in cols]
        df = query_df(
            str(DB_PATH),
            f"""
            SELECT {", ".join(want)}
            FROM {sk_rel}
            WHERE season=? AND situation=? AND gameid=?
            ORDER BY TOI DESC
            """,
            (season, situation, str(gameid)),
        )
        st.dataframe(prep_table_for_display(df), width="stretch")

with tabs[2]:
    if not go_rel:
        st.info("Goalie fact relation not found.")
    else:
        cols = set(cols_of(str(DB_PATH), go_rel))
        want = [c for c in ["gamedate","team","name","ongoal","goals","xgoals","TOI"] if c in cols]
        df = query_df(
            str(DB_PATH),
            f"""
            SELECT {", ".join(want)}
            FROM {go_rel}
            WHERE season=? AND situation=? AND gameid=?
            """,
            (season, situation, str(gameid)),
        )
        st.dataframe(prep_table_for_display(df), width="stretch")
