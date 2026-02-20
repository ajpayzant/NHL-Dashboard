# app/pages/01_League_Table.py
import streamlit as st
import pandas as pd

from shared import (
    DB_PATH, top_filter_bar, query_df, find_relation_with_cols, cols_of, prep_table_for_display
)

st.header("League Table")

season, situation = top_filter_bar(str(DB_PATH))

TEAM_REQ = ["team", "season", "situation", "gamedate"]
team_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=TEAM_REQ,
    prefer_names=("fact_team_game", "fact_teams", "v_teams_src", "fact_team_game"),
)

if not team_rel:
    st.error("Could not find a team-game fact table/view in the DB.")
    st.stop()

team_cols = set(cols_of(str(DB_PATH), team_rel))

# columns we *want* if available
want = [
    "team","season","situation",
    "goalsfor","goalsagainst",
    "xgoalsfor","xgoalsagainst",
    "shotsongoalfor","shotsongoalagainst",
    "shotattemptsfor","shotattemptsagainst",
    "unblockedshotattemptsfor","unblockedshotattemptsagainst",
]

use = [c for c in want if c in team_cols]

df = query_df(
    str(DB_PATH),
    f"""
    SELECT {", ".join(use)}
    FROM {team_rel}
    WHERE season=? AND situation=?
    """,
    (season, situation),
)

if df.empty:
    st.warning("No rows for this season/situation.")
    st.stop()

# W/L from per-game rows
if "goalsfor" in df.columns and "goalsagainst" in df.columns:
    df["_W"] = (pd.to_numeric(df["goalsfor"], errors="coerce") > pd.to_numeric(df["goalsagainst"], errors="coerce")).astype(int)
    df["_L"] = (pd.to_numeric(df["goalsfor"], errors="coerce") < pd.to_numeric(df["goalsagainst"], errors="coerce")).astype(int)
else:
    df["_W"] = 0
    df["_L"] = 0

g = df.groupby("team", as_index=False).agg(
    GP=("team","size"),
    W=("_W","sum"),
    L=("_L","sum"),
)

# sums
sum_cols = [c for c in use if c not in ("team","season","situation")]
for c in sum_cols:
    g[c] = df.groupby("team")[c].sum().values

# Derived per-game and advanced
if "goalsfor" in g.columns and "GP" in g.columns:
    g["GF/GP"] = g["goalsfor"] / g["GP"]
    g["GA/GP"] = g["goalsagainst"] / g["GP"]

if "xgoalsfor" in g.columns and "xgoalsagainst" in g.columns:
    g["xG%"] = g["xgoalsfor"] / (g["xgoalsfor"] + g["xgoalsagainst"])

view = st.radio("View", ["Totals", "Per Game", "Advanced"], horizontal=True)

if view == "Totals":
    show = ["team","GP","W","L"]
    for c in ["goalsfor","goalsagainst","shotsongoalfor","shotsongoalagainst","shotattemptsfor","shotattemptsagainst","xgoalsfor","xgoalsagainst"]:
        if c in g.columns:
            show.append(c)
    out = g[show].sort_values(["W","goalsfor"] if "goalsfor" in g.columns else ["W"], ascending=False)

elif view == "Per Game":
    show = ["team","GP","W","L"]
    for c in ["GF/GP","GA/GP"]:
        if c in g.columns:
            show.append(c)
    # add SOG/GP etc if present
    if "shotsongoalfor" in g.columns:
        g["SOGF/GP"] = g["shotsongoalfor"] / g["GP"]
    if "shotsongoalagainst" in g.columns:
        g["SOGA/GP"] = g["shotsongoalagainst"] / g["GP"]
    for c in ["SOGF/GP","SOGA/GP"]:
        if c in g.columns:
            show.append(c)
    out = g[show].sort_values(["GF/GP"] if "GF/GP" in g.columns else ["W"], ascending=False)

else:  # Advanced
    show = ["team","GP","W","L"]
    for c in ["xG%","xgoalsfor","xgoalsagainst","unblockedshotattemptsfor","unblockedshotattemptsagainst"]:
        if c in g.columns:
            show.append(c)
    out = g[show].sort_values(["xG%"] if "xG%" in g.columns else ["W"], ascending=False)

st.dataframe(prep_table_for_display(out).reset_index(drop=True), width="stretch")

# Purposeful visual: xG% bar chart (top 10)
if "xG%" in out.columns:
    st.subheader("Top 10 Teams by xG%")
    top10 = out.sort_values("xG%", ascending=False).head(10)[["team","xG%"]]
    st.bar_chart(top10.set_index("team"))

# ------------------------
# League Leaders
# ------------------------
st.divider()
st.subheader("League Leaders")

tab1, tab2 = st.tabs(["Skaters", "Goalies"])

with tab1:
    SK_REQ = ["playerid","name","team","season","situation","gamedate","TOI"]
    sk_rel = find_relation_with_cols(
        str(DB_PATH),
        required_cols=SK_REQ,
        prefer_names=("fact_skater_game","fact_skaters","v_skaters_src"),
    )
    if not sk_rel:
        st.info("Skater fact table not found in DB.")
    else:
        sk_cols = set(cols_of(str(DB_PATH), sk_rel))
        needed = [c for c in ["playerid","name","team","i_f_goals","i_f_points","i_f_shotsongoal","i_f_xgoals","TOI"] if c in sk_cols]
        sk = query_df(
            str(DB_PATH),
            f"""
            SELECT {", ".join(needed)}
            FROM {sk_rel}
            WHERE season=? AND situation=?
            """,
            (season, situation),
        )
        if sk.empty:
            st.info("No skater rows for this season/situation.")
        else:
            # aggregates
            sk["GP"] = 1
            agg = sk.groupby(["playerid","name","team"], as_index=False).agg(
                GP=("GP","sum"),
                TOI=("TOI","sum") if "TOI" in sk.columns else ("GP","sum"),
            )
            if "i_f_goals" in sk.columns:
                agg["G"] = sk.groupby(["playerid","name","team"])["i_f_goals"].sum().values
            if "i_f_points" in sk.columns:
                agg["PTS"] = sk.groupby(["playerid","name","team"])["i_f_points"].sum().values
            if "G" in agg.columns and "PTS" in agg.columns:
                agg["A"] = agg["PTS"] - agg["G"]

            metric = st.selectbox("Skater leader metric", [c for c in ["G","A","PTS","TOI"] if c in agg.columns], index=0)
            leaders = agg.sort_values(metric, ascending=False).head(25)
            st.dataframe(prep_table_for_display(leaders), width="stretch")

with tab2:
    GO_REQ = ["playerid","name","team","season","situation","gamedate","TOI"]
    go_rel = find_relation_with_cols(
        str(DB_PATH),
        required_cols=GO_REQ,
        prefer_names=("fact_goalie_game","fact_goalies","v_goalies_src"),
    )
    if not go_rel:
        st.info("Goalie fact table not found in DB.")
    else:
        go_cols = set(cols_of(str(DB_PATH), go_rel))
        needed = [c for c in ["playerid","name","team","goals","xgoals","ongoal","TOI"] if c in go_cols]
        go = query_df(
            str(DB_PATH),
            f"""
            SELECT {", ".join(needed)}
            FROM {go_rel}
            WHERE season=? AND situation=?
            """,
            (season, situation),
        )
        if go.empty:
            st.info("No goalie rows for this season/situation.")
        else:
            go["GP"] = 1
            agg = go.groupby(["playerid","name","team"], as_index=False).agg(GP=("GP","sum"))
            if "ongoal" in go.columns:
                agg["SA"] = go.groupby(["playerid","name","team"])["ongoal"].sum().values
            if "goals" in go.columns:
                agg["GA"] = go.groupby(["playerid","name","team"])["goals"].sum().values
            if "SA" in agg.columns and "GA" in agg.columns:
                agg["SV"] = agg["SA"] - agg["GA"]
                agg["SV%"] = (agg["SV"] / agg["SA"]).where(agg["SA"] > 0)

            if "xgoals" in go.columns and "goals" in go.columns:
                agg["GSAx"] = go.groupby(["playerid","name","team"])["xgoals"].sum().values - agg["GA"]

            metric = st.selectbox("Goalie leader metric", [c for c in ["SV","SV%","GSAx","SA"] if c in agg.columns], index=0)
            leaders = agg.sort_values(metric, ascending=False).head(25)
            st.dataframe(prep_table_for_display(leaders), width="stretch")
