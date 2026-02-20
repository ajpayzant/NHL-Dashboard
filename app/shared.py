import os
from pathlib import Path
import requests
import duckdb
import pandas as pd
import streamlit as st

# ----------------------------
# Local DB storage (Streamlit Cloud filesystem is ephemeral, but this is fine;
# it re-downloads on first boot or when you hit Refresh).
# ----------------------------
APP_DATA_DIR = Path(".app_data")
APP_DATA_DIR.mkdir(exist_ok=True)
DB_PATH = APP_DATA_DIR / "nhl_dashboard.duckdb"

def get_db_url() -> str:
    # Priority: Streamlit Secrets -> env var
    try:
        if "DB_URL" in st.secrets:
            return str(st.secrets["DB_URL"]).strip()
    except Exception:
        pass
    return os.getenv("DB_URL", "").strip()

def download_db(db_url: str) -> Path:
    if not db_url:
        raise ValueError("DB_URL is not set. Add it in Streamlit Secrets (DB_URL=...)")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Stream download to a temp file then atomic rename
    tmp = DB_PATH.with_suffix(".part")
    with requests.get(db_url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    tmp.replace(DB_PATH)
    return DB_PATH

@st.cache_resource
def get_con(db_path: str) -> duckdb.DuckDBPyConnection:
    # Read-write is fine; we’re not modifying tables, just querying.
    con = duckdb.connect(db_path)
    con.execute("PRAGMA threads=4;")
    return con

@st.cache_data(ttl=60)
def query_df(db_path: str, sql: str, params: tuple = ()) -> pd.DataFrame:
    con = get_con(db_path)
    return con.execute(sql, params).df()

def relation_exists(db_path: str, rel_name: str) -> bool:
    sql = """
    SELECT COUNT(*) AS n
    FROM information_schema.tables
    WHERE table_name = ?
    """
    df = query_df(db_path, sql, (rel_name,))
    if not df.empty and int(df.loc[0, "n"]) > 0:
        return True

    # also check views (DuckDB sometimes classifies differently)
    sql2 = """
    SELECT COUNT(*) AS n
    FROM information_schema.views
    WHERE table_name = ?
    """
    df2 = query_df(db_path, sql2, (rel_name,))
    return (not df2.empty) and int(df2.loc[0, "n"]) > 0

def cols_of(db_path: str, rel_name: str) -> list[str]:
    # DuckDB PRAGMA table_info returns column "name" (not "column_name")
    df = query_df(db_path, f"SELECT name FROM pragma_table_info('{rel_name}')")
    if df.empty:
        return []
    return df["name"].tolist()

def find_relation_with_cols(db_path: str, required_cols: list[str], prefer_names: tuple[str, ...]) -> str | None:
    # 1) try preferred names
    for name in prefer_names:
        if relation_exists(db_path, name):
            cols = set(cols_of(db_path, name))
            if all(c in cols for c in required_cols):
                return name

    # 2) fallback: scan all tables/views
    all_rel = query_df(db_path, "SHOW ALL TABLES").iloc[:, 0].tolist()
    for name in all_rel:
        cols = set(cols_of(db_path, name))
        if cols and all(c in cols for c in required_cols):
            return name

    return None

def sidebar_filters(db_path: str) -> tuple[int, str]:
    """
    Global filters used on every page.
    We infer seasons/situations from fact_team_game if possible, else fallback.
    """
    # pick team relation for season/situation lists
    team_rel = find_relation_with_cols(
        db_path,
        required_cols=["season", "situation"],
        prefer_names=("fact_team_game", "v_teams_src", "fact_teams_game"),
    )
    if not team_rel:
        st.error("Could not find a team fact relation with columns: season, situation.")
        st.stop()

    seasons = query_df(db_path, f"SELECT DISTINCT season FROM {team_rel} WHERE season IS NOT NULL ORDER BY season DESC")["season"].tolist()
    situations = query_df(db_path, f"SELECT DISTINCT situation FROM {team_rel} WHERE situation IS NOT NULL ORDER BY situation")["situation"].tolist()

    if not seasons:
        st.error("No seasons found in the database.")
        st.stop()

    # defaults
    default_season = seasons[0]
    default_situation = "all" if "all" in situations else (situations[0] if situations else "all")

    with st.sidebar:
        st.header("Filters")
        season = st.selectbox("Season", seasons, index=0)
        situation = st.selectbox("Situation", situations, index=situations.index(default_situation) if default_situation in situations else 0)

        st.divider()
        st.header("Data")
        if st.button("Download / Refresh DB"):
            db_url = get_db_url()
            with st.spinner("Downloading latest DB..."):
                download_db(db_url)
            st.cache_resource.clear()
            st.cache_data.clear()
            st.success("DB refreshed. Reload the page if needed.")

    return int(season), str(situation)
