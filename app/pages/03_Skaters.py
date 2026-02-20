# app/pages/03_Skaters.py
import streamlit as st
import pandas as pd

from shared import (
    DB_PATH, top_filter_bar, query_df, find_relation_with_cols, cols_of, prep_table_for_display, relation_exists
)

st.header("Skaters")

season, situation = top_filter_bar(str(DB_PATH))

SK_REQ = ["playerid","name","team","season","situation","gamedate","TOI"]
sk_rel = find_relation_with_cols(str(DB_PATH), SK_REQ, prefer_names=("fact_skater_game","v_skaters_src"))
if not sk_rel:
    st.error("Skater fact table not found.")
    st.stop()

sk_cols = set(cols_of(str(DB_PATH), sk_rel))

players = query_df(
    str(DB_PATH),
    f"""
    SELECT DISTINCT playerid, name
    FROM {sk_rel}
    WHERE season=? AND situation=?
    ORDER BY name
    """,
    (season, situation),
)
if players.empty:
    st.warning("No skaters found for this season/situation.")
    st.stop()

player_name = st.selectbox("Player", players["name"].tolist(), index=0)
playerid = players.loc[players["name"] == player_name, "playerid"].iloc[0]

# --- Bio header ---
st.subheader(player_name)

# Use dim_player (always) + optional player_bios (if you copy it into portable DB later)
bio_left, bio_right = st.columns(2)

if relation_exists(str(DB_PATH), "dim_player"):
    dim = query_df(str(DB_PATH), "SELECT * FROM dim_player WHERE playerid=?", (str(playerid),))
else:
    dim = pd.DataFrame()

with bio_left:
    if not dim.empty:
        r = dim.iloc[0].to_dict()
        st.write(f"**Position:** {r.get('position','')}")
        st.write(f"**Current Team:** {r.get('team_current','')}")
        st.write(f"**First Season:** {r.get('first_season','')}")
        st.write(f"**Last Season:** {r.get('last_season','')}")
    else:
        st.info("dim_player not found in DB.")

with bio_right:
    # Optional: if you later include player_bios table in portable db
    if relation_exists(str(DB_PATH), "player_bios"):
        pb = query_df(str(DB_PATH), "SELECT * FROM player_bios WHERE CAST(playerid AS VARCHAR)=?", (str(playerid),))
        if not pb.empty:
            # show a few common fields if present
            row = pb.iloc[0].to_dict()
            for k in ["age","nationality","shoots","height","weight","birthdate"]:
                if k in row and row[k] is not None and str(row[k]) != "":
                    st.write(f"**{k.title()}:** {row[k]}")
        else:
            st.caption("player_bios table exists, but no matching row for this player.")
    else:
        st.caption("Bio extras (age/nationality/etc) will appear once `player_bios` is included in the portable DB.")

# --- Career log (season/team) ---
st.divider()
st.subheader("Career Log (by season + team)")

need = [c for c in ["playerid","team","season","TOI","i_f_goals","i_f_points"] if c in sk_cols]
car = query_df(
    str(DB_PATH),
    f"""
    SELECT {", ".join(need)}
    FROM {sk_rel}
    WHERE playerid=?
    """,
    (str(playerid),),
)
if car.empty:
    st.info("No career rows found.")
    st.stop()

car["GP"] = 1
agg = car.groupby(["season","team"], as_index=False).agg(
    GP=("GP","sum"),
    TOI=("TOI","sum") if "TOI" in car.columns else ("GP","sum"),
)
if "i_f_goals" in car.columns:
    agg["G"] = car.groupby(["season","team"])["i_f_goals"].sum().values
if "i_f_points" in car.columns:
    agg["PTS"] = car.groupby(["season","team"])["i_f_points"].sum().values
if "G" in agg.columns and "PTS" in agg.columns:
    agg["A"] = agg["PTS"] - agg["G"]
if "TOI" in agg.columns:
    agg["TOI/GP"] = agg["TOI"] / agg["GP"]

show = ["season","team","GP"] + [c for c in ["G","A","PTS","TOI/GP"] if c in agg.columns]
st.dataframe(prep_table_for_display(agg[show].sort_values(["season","team"], ascending=[False, True])), width="stretch")

# --- Game logs (filtered, no ids) ---
st.divider()
st.subheader("Game Logs")

last_n = st.slider("Show last N games", 5, 50, 15, step=1)

want = [c for c in ["gamedate","opp_team","home_or_away","TOI","i_f_goals","i_f_points","i_f_shotsongoal","i_f_xgoals"] if c in sk_cols]
games = query_df(
    str(DB_PATH),
    f"""
    SELECT {", ".join(want)}
    FROM {sk_rel}
    WHERE season=? AND situation=? AND playerid=?
    ORDER BY gamedate DESC
    LIMIT ?
    """,
    (season, situation, str(playerid), int(last_n)),
)

if games.empty:
    st.info("No games found for this filter.")
    st.stop()

if "i_f_goals" in games.columns and "i_f_points" in games.columns:
    games["A"] = pd.to_numeric(games["i_f_points"], errors="coerce") - pd.to_numeric(games["i_f_goals"], errors="coerce")
    games = games.rename(columns={"i_f_goals":"G","i_f_points":"PTS"})

keep = [c for c in ["gamedate","opp_team","home_or_away","TOI","G","A","PTS","i_f_shotsongoal","i_f_xgoals"] if c in games.columns]
st.dataframe(prep_table_for_display(games[keep]), width="stretch")
