# app/shared.py
import os
from pathlib import Path
from typing import Iterable, Optional, Tuple, List

import duckdb
import pandas as pd
import streamlit as st


# ----------------------------
# Paths
# ----------------------------
APP_DIR = Path(__file__).resolve().parents[0]
REPO_ROOT = APP_DIR.parents[0]

DEFAULT_DB_PATH = REPO_ROOT / "nhl_dashboard.duckdb"
DB_PATH = Path(os.getenv("APP_DB_PATH", str(DEFAULT_DB_PATH)))


# ----------------------------
# DuckDB helpers
# ----------------------------
@st.cache_resource
def get_con(db_path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db_path, read_only=True)
    # Keep it small + stable on Streamlit Cloud
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA memory_limit='2GB';")
    return con


@st.cache_data(ttl=60)
def query_df(db_path: str, sql: str, params: tuple = ()) -> pd.DataFrame:
    con = get_con(db_path)
    return con.execute(sql, params).df()


@st.cache_data(ttl=300)
def list_relations(db_path: str) -> pd.DataFrame:
    # Works for tables + views; avoids SHOW ALL TABLES multi-column pitfalls
    sql = """
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema','pg_catalog')
    ORDER BY table_schema, table_name;
    """
    return query_df(db_path, sql)


def relation_exists(db_path: str, rel_name: str) -> bool:
    rel = rel_name.strip()
    df = list_relations(db_path)
    # allow schema-qualified or bare
    if "." in rel:
        sch, nm = rel.split(".", 1)
        return ((df["table_schema"] == sch) & (df["table_name"] == nm)).any()
    return (df["table_name"] == rel).any()


def cols_of(db_path: str, rel_name: str) -> List[str]:
    # DuckDB pragma_table_info returns column named "name"
    rel = rel_name.replace("'", "''")
    try:
        df = query_df(db_path, f"SELECT name FROM pragma_table_info('{rel}')")
        if df.empty:
            return []
        return df["name"].astype(str).tolist()
    except Exception:
        return []


def find_relation_with_cols(
    db_path: str,
    required_cols: Iterable[str],
    prefer_names: Iterable[str] = (),
) -> Optional[str]:
    req = list(required_cols)

    # 1) try preferred names
    for nm in prefer_names:
        if relation_exists(db_path, nm):
            cols = set(cols_of(db_path, nm))
            if cols and all(c in cols for c in req):
                return nm

    # 2) scan all relations safely
    rels = list_relations(db_path)
    for _, r in rels.iterrows():
        sch = str(r["table_schema"])
        nm = str(r["table_name"])
        qualified = nm if sch in ("main", "", "None") else f"{sch}.{nm}"
        cols = set(cols_of(db_path, qualified))
        if cols and all(c in cols for c in req):
            return qualified

    return None


# ----------------------------
# Global filter bar (TOP of each page)
# ----------------------------
@st.cache_data(ttl=300)
def get_season_options(db_path: str) -> List[int]:
    # Prefer dim_game; fallback to team facts
    if relation_exists(db_path, "dim_game"):
        df = query_df(db_path, "SELECT DISTINCT season FROM dim_game WHERE season IS NOT NULL ORDER BY season DESC")
    else:
        rel = find_relation_with_cols(db_path, ["season"], prefer_names=("fact_team_game", "v_teams_src"))
        df = query_df(db_path, f"SELECT DISTINCT season FROM {rel} WHERE season IS NOT NULL ORDER BY season DESC") if rel else pd.DataFrame()
    seasons = [int(x) for x in df["season"].tolist()] if not df.empty else []
    return seasons


@st.cache_data(ttl=300)
def get_situation_options(db_path: str) -> List[str]:
    rel = find_relation_with_cols(db_path, ["situation"], prefer_names=("fact_team_game", "v_teams_src"))
    if not rel:
        return ["all"]
    df = query_df(db_path, f"SELECT DISTINCT situation FROM {rel} WHERE situation IS NOT NULL ORDER BY situation")
    sits = [str(x) for x in df["situation"].tolist()] if not df.empty else ["all"]
    # Enforce your desired ordering
    preferred = ["all", "5v5", "5on4", "4on5"]
    out = [s for s in preferred if s in sits] + [s for s in sits if s not in preferred]
    return out


def top_filter_bar(db_path: str) -> Tuple[int, str]:
    seasons = get_season_options(db_path)
    sits = get_situation_options(db_path)

    if not seasons:
        st.error("No seasons found in DB.")
        st.stop()

    # persist across pages
    if "global_season" not in st.session_state:
        st.session_state["global_season"] = seasons[0]
    if "global_situation" not in st.session_state:
        st.session_state["global_situation"] = "all" if "all" in sits else sits[0]

    st.markdown("### Filters")
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        st.session_state["global_season"] = st.selectbox(
            "Season",
            seasons,
            index=seasons.index(st.session_state["global_season"]) if st.session_state["global_season"] in seasons else 0,
        )
    with c2:
        st.session_state["global_situation"] = st.selectbox(
            "Situation",
            sits,
            index=sits.index(st.session_state["global_situation"]) if st.session_state["global_situation"] in sits else 0,
        )
    with c3:
        st.caption("Tip: filters persist across pages.")

    return int(st.session_state["global_season"]), str(st.session_state["global_situation"])


# ----------------------------
# Display formatting utilities
# ----------------------------
def format_mmdd(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%m-%d").fillna("")


def prep_table_for_display(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # gamedate formatting
    if "gamedate" in out.columns:
        out["gamedate"] = format_mmdd(out["gamedate"])

    # drop gameid everywhere for UI
    if "gameid" in out.columns:
        out = out.drop(columns=["gameid"], errors="ignore")

    return out
