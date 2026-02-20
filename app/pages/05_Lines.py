# app/pages/05_Lines.py
import streamlit as st

from shared import (
    DB_PATH, top_filter_bar, query_df, find_relation_with_cols, cols_of, prep_table_for_display
)

st.header("Lines / Pairs")

season, situation = top_filter_bar(str(DB_PATH))

LINES_REQ = ["combo_key_team","team","season","situation","TOI"]

lines_rel = find_relation_with_cols(
    str(DB_PATH),
    required_cols=LINES_REQ,
    prefer_names=("fact_lines_game","lines_game_ready","v_lines_game_src","fact_line_game"),
)

if not lines_rel:
    st.error(
        "Lines data not found in the portable DB.\n\n"
        "You need your ETL to expose/copy one of these relations into the portable DB:\n"
        "- fact_lines_game (preferred)\n"
        "- lines_game_ready\n"
        "- v_lines_game_src\n\n"
        "Once you update `etl/build_warehouse.py` to create a lines view and update "
        "`etl/build_portable_db.py` to copy it, this page will work."
    )
    st.stop()

cols = set(cols_of(str(DB_PATH), lines_rel))

teams = query_df(
    str(DB_PATH),
    f"SELECT DISTINCT team FROM {lines_rel} WHERE season=? AND situation=? ORDER BY team",
    (season, situation),
)
if teams.empty:
    st.warning("No lines rows for this season/situation.")
    st.stop()

team = st.selectbox("Team", teams["team"].tolist(), index=0)

pos_col = "position" if "position" in cols else None
pos = "line"
if pos_col:
    pos = st.radio("Type", ["line", "pair"], horizontal=True)

min_toi = st.slider("Min TOI (season total)", 0.0, 500.0, 50.0, step=5.0)

# Choose key metric columns if they exist
metric_candidates = ["xgoalsfor_per60","xgoalsagainst_per60","goalsfor_per60","goalsagainst_per60","shotsongoalfor_per60","shotsongoalagainst_per60"]
metrics = [m for m in metric_candidates if m in cols]

base_cols = [c for c in ["team","season","situation","combo_key_team","combo_key_ids","TOI","p1_name","p2_name","p3_name"] if c in cols]
select_cols = base_cols + metrics

sql = f"""
SELECT {", ".join(select_cols)}
FROM {lines_rel}
WHERE season=? AND situation=? AND team=? AND TOI >= ?
"""
params = [season, situation, team, float(min_toi)]

if pos_col:
    sql += " AND lower(CAST(position AS VARCHAR)) = ?"
    params.append(pos)

sql += " ORDER BY TOI DESC LIMIT 200"

df = query_df(str(DB_PATH), sql, tuple(params))

if df.empty:
    st.info("No lines/pairs matched your filters.")
    st.stop()

# cleaner display: show a readable line name
if "p1_name" in df.columns:
    df["Combo"] = df[["p1_name","p2_name","p3_name"]].fillna("").agg(" / ".join, axis=1).str.replace(r"( / )+$", "", regex=True)

show = ["Combo","TOI"] + metrics
if "Combo" not in df.columns:
    show = ["combo_key_team","TOI"] + metrics

st.dataframe(prep_table_for_display(df[show]), width="stretch")
