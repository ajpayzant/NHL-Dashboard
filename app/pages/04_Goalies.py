# app/pages/04_Goalies.py
import streamlit as st
import pandas as pd

from shared import (
    DB_PATH, top_filter_bar, query_df, find_relation_with_cols, cols_of, prep_table_for_display, relation_exists
)

st.header("Goalies")

season, situation = top_filter_bar(str(DB_PATH))

GO_REQ = ["playerid","name","team","season","situation","gamedate","TOI"]
go_rel = find_relation_with_cols(str(DB_PATH), GO_REQ, prefer_names=("fact_goalie_game","v_goalies_src"))
if not go_rel:
    st.error("Goalie fact table not found.")
    st.stop()

go_cols = set(cols_of(str(DB_PATH), go_rel))

goalies = query_df(
    str(DB_PATH),
    f"""
    SELECT DISTINCT playerid, name
    FROM {go_rel}
    WHERE season=? AND situation=?
    ORDER BY name
    """,
    (season, situation),
)
if goalies.empty:
    st.warning("No goalies found for this season/situation.")
    st.stop()

goalie_name = st.selectbox("Goalie", goalies["name"].tolist(), index=0)
playerid = goalies.loc[goalies["name"] == goalie_name, "playerid"].iloc[0]

st.subheader(goalie_name)

# dim_player basic
if relation_exists(str(DB_PATH), "dim_player"):
    dim = query_df(str(DB_PATH), "SELECT * FROM dim_player WHERE playerid=?", (str(playerid),))
    if not dim.empty:
        r = dim.iloc[0].to_dict()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Team", r.get("team_current",""))
        c2.metric("First Season", r.get("first_season",""))
        c3.metric("Last Season", r.get("last_season",""))
        c4.metric("Fact Rows", r.get("fact_rows",""))

# Career log
st.divider()
st.subheader("Career Log (by season + team)")

need = [c for c in ["playerid","team","season","goals","xgoals","ongoal"] if c in go_cols]
car = query_df(
    str(DB_PATH),
    f"""
    SELECT {", ".join(need)}
    FROM {go_rel}
    WHERE playerid=?
    """,
    (str(playerid),),
)
if car.empty:
    st.info("No career rows found.")
    st.stop()

car["GP"] = 1
agg = car.groupby(["season","team"], as_index=False).agg(GP=("GP","sum"))
if "ongoal" in car.columns:
    agg["SA"] = car.groupby(["season","team"])["ongoal"].sum().values
if "goals" in car.columns:
    agg["GA"] = car.groupby(["season","team"])["goals"].sum().values
if "SA" in agg.columns and "GA" in agg.columns:
    agg["SV"] = agg["SA"] - agg["GA"]
    agg["SV%"] = (agg["SV"] / agg["SA"]).where(agg["SA"] > 0)
if "xgoals" in car.columns and "GA" in agg.columns:
    xga = car.groupby(["season","team"])["xgoals"].sum().values
    agg["xGA"] = xga
    agg["GSAx"] = agg["xGA"] - agg["GA"]

show = ["season","team","GP"] + [c for c in ["SA","GA","SV","SV%","xGA","GSAx"] if c in agg.columns]
st.dataframe(prep_table_for_display(agg[show].sort_values(["season","team"], ascending=[False, True])), width="stretch")

# Game logs
st.divider()
st.subheader("Game Logs")

last_n = st.slider("Show last N games", 5, 50, 15, step=1)

want = [c for c in ["gamedate","team","goals","xgoals","ongoal"] if c in go_cols]
games = query_df(
    str(DB_PATH),
    f"""
    SELECT {", ".join(want)}
    FROM {go_rel}
    WHERE season=? AND situation=? AND playerid=?
    ORDER BY gamedate DESC
    LIMIT ?
    """,
    (season, situation, str(playerid), int(last_n)),
)

if games.empty:
    st.info("No games found for this filter.")
    st.stop()

if "ongoal" in games.columns and "goals" in games.columns:
    games["SV"] = pd.to_numeric(games["ongoal"], errors="coerce") - pd.to_numeric(games["goals"], errors="coerce")
    games["SV%"] = (games["SV"] / pd.to_numeric(games["ongoal"], errors="coerce")).where(pd.to_numeric(games["ongoal"], errors="coerce") > 0)

keep = [c for c in ["gamedate","team","ongoal","goals","SV","SV%","xgoals"] if c in games.columns]
st.dataframe(prep_table_for_display(games[keep]), width="stretch")
