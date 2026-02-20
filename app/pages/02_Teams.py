# app/pages/02_Teams.py
import streamlit as st
import pandas as pd

from shared import (
    DB_PATH, top_filter_bar, query_df, find_relation_with_cols, cols_of, prep_table_for_display
)

st.header("Teams")

season, situation = top_filter_bar(str(DB_PATH))

TEAM_REQ = ["team","season","situation","gamedate"]
team_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=TEAM_REQ,
    prefer_names=("fact_team_game","v_teams_src"),
)
if not team_rel:
    st.error("Team fact table not found.")
    st.stop()

team_cols = set(cols_of(str(DB_PATH), team_rel))
teams = query_df(str(DB_PATH), f"SELECT DISTINCT team FROM {team_rel} WHERE season=? AND situation=? ORDER BY team", (season, situation))
if teams.empty:
    st.warning("No teams found for this season/situation.")
    st.stop()

team = st.selectbox("Team", teams["team"].tolist(), index=0)

# Pull team game rows
want = [c for c in ["gamedate","gameid","team","opp_team","home_or_away","goalsfor","goalsagainst","xgoalsfor","xgoalsagainst","shotsongoalfor","shotsongoalagainst"] if c in team_cols]
df = query_df(
    str(DB_PATH),
    f"""
    SELECT {", ".join(want)}
    FROM {team_rel}
    WHERE season=? AND situation=? AND team=?
    ORDER BY gamedate DESC
    """,
    (season, situation, team),
)

if df.empty:
    st.warning("No games found for that team.")
    st.stop()

# Team summary (W/L + ranks)
gf = pd.to_numeric(df.get("goalsfor", 0), errors="coerce").fillna(0)
ga = pd.to_numeric(df.get("goalsagainst", 0), errors="coerce").fillna(0)
W = int((gf > ga).sum())
L = int((gf < ga).sum())
GP = int(len(df))

st.subheader(f"{team} — {W}-{L} (GP: {GP})")

# League rank: compute across all teams
league = query_df(
    str(DB_PATH),
    f"""
    SELECT team, goalsfor, goalsagainst
    FROM {team_rel}
    WHERE season=? AND situation=?
    """,
    (season, situation),
)
league["goalsfor"] = pd.to_numeric(league["goalsfor"], errors="coerce").fillna(0)
league["goalsagainst"] = pd.to_numeric(league["goalsagainst"], errors="coerce").fillna(0)
league_g = league.groupby("team", as_index=False).agg(GF=("goalsfor","sum"), GA=("goalsagainst","sum"))
league_g["W"] = league.groupby("team").apply(lambda g: (pd.to_numeric(g["goalsfor"], errors="coerce") > pd.to_numeric(g["goalsagainst"], errors="coerce")).sum()).values
league_g["L"] = league.groupby("team").apply(lambda g: (pd.to_numeric(g["goalsfor"], errors="coerce") < pd.to_numeric(g["goalsagainst"], errors="coerce")).sum()).values
league_g["GF_rank"] = league_g["GF"].rank(ascending=False, method="min").astype(int)
league_g["GA_rank"] = league_g["GA"].rank(ascending=True, method="min").astype(int)

row = league_g[league_g["team"] == team].iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("GF", int(row["GF"]), help="Total goals for")
c2.metric("GF Rank", int(row["GF_rank"]))
c3.metric("GA", int(row["GA"]), help="Total goals against")
c4.metric("GA Rank", int(row["GA_rank"]))

# Recent games
st.divider()
st.subheader("Recent Games")

last_n = st.slider("Show last N games", 5, 25, 10, step=1)

recent = df.head(last_n).copy()
if "goalsfor" in recent.columns and "goalsagainst" in recent.columns:
    recent["Result"] = (pd.to_numeric(recent["goalsfor"], errors="coerce") > pd.to_numeric(recent["goalsagainst"], errors="coerce")).map({True:"W", False:"L"})
    recent.loc[pd.to_numeric(recent["goalsfor"], errors="coerce") == pd.to_numeric(recent["goalsagainst"], errors="coerce"), "Result"] = "T"

show_cols = [c for c in ["gamedate","Result","opp_team","home_or_away","goalsfor","goalsagainst","xgoalsfor","xgoalsagainst","shotsongoalfor","shotsongoalagainst"] if c in recent.columns]
st.dataframe(prep_table_for_display(recent[show_cols]), width="stretch")

# Roster / skaters who played for team
st.divider()
st.subheader("Roster (Players who appeared for team)")

SK_REQ = ["playerid","name","team","season","situation","gamedate","TOI"]
sk_rel = find_relation_with_cols(str(DB_PATH), SK_REQ, prefer_names=("fact_skater_game","v_skaters_src"))
if not sk_rel:
    st.info("Skater fact table not found.")
    st.stop()

sk_cols = set(cols_of(str(DB_PATH), sk_rel))
need = [c for c in ["playerid","name","team","TOI","i_f_goals","i_f_points","i_f_xgoals","i_f_shotsongoal"] if c in sk_cols]

sk = query_df(
    str(DB_PATH),
    f"""
    SELECT {", ".join(need)}
    FROM {sk_rel}
    WHERE season=? AND situation=? AND team=?
    """,
    (season, situation, team),
)

if sk.empty:
    st.info("No skater rows for that team/season/situation.")
    st.stop()

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

if "TOI" in agg.columns and "GP" in agg.columns:
    agg["TOI/GP"] = agg["TOI"] / agg["GP"]

mode = st.radio("Roster view", ["Totals", "Per Game", "Advanced"], horizontal=True)

out = agg.copy()
if mode == "Per Game":
    for c in ["G","A","PTS"]:
        if c in out.columns:
            out[f"{c}/GP"] = out[c] / out["GP"]
    keep = ["name","GP"] + [c for c in ["G/GP","A/GP","PTS/GP","TOI/GP"] if c in out.columns]
    out = out[keep].sort_values(["PTS/GP"] if "PTS/GP" in out.columns else ["GP"], ascending=False)
elif mode == "Advanced":
    # show xG and SOG if available
    if "i_f_xgoals" in sk.columns:
        out["xG"] = sk.groupby(["playerid","name","team"])["i_f_xgoals"].sum().values
        out["xG/GP"] = out["xG"] / out["GP"]
    if "i_f_shotsongoal" in sk.columns:
        out["SOG"] = sk.groupby(["playerid","name","team"])["i_f_shotsongoal"].sum().values
        out["SOG/GP"] = out["SOG"] / out["GP"]
    keep = ["name","GP"] + [c for c in ["xG","xG/GP","SOG","SOG/GP","TOI/GP"] if c in out.columns]
    out = out[keep].sort_values(["xG/GP"] if "xG/GP" in out.columns else ["GP"], ascending=False)
else:
    keep = ["name","GP"] + [c for c in ["G","A","PTS","TOI","TOI/GP"] if c in out.columns]
    out = out[keep].sort_values(["PTS"] if "PTS" in out.columns else ["GP"], ascending=False)

st.dataframe(prep_table_for_display(out).reset_index(drop=True), width="stretch")
