import os
from pathlib import Path
import requests
import duckdb
import streamlit as st

# Put the release asset URL here (code-only URL is fine)
# Example (replace OWNER/REPO):
# DB_URL = "https://github.com/OWNER/REPO/releases/download/data-latest/nhl_dashboard.duckdb"
DB_URL = os.environ.get("DB_URL", "").strip()

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)
DB_PATH = CACHE_DIR / "nhl_dashboard.duckdb"

@st.cache_data(show_spinner=False)
def download_db(db_url: str) -> str:
    if not db_url:
        raise ValueError("DB_URL is not set. Set it in Streamlit Secrets or environment variables.")
    r = requests.get(db_url, stream=True, timeout=120)
    r.raise_for_status()
    tmp = DB_PATH.with_suffix(".part")
    with open(tmp, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    tmp.replace(DB_PATH)
    return str(DB_PATH)

@st.cache_resource(show_spinner=False)
def get_con(db_path: str):
    con = duckdb.connect(db_path, read_only=True)
    return con

st.set_page_config(page_title="NHL Dashboard", layout="wide")
st.title("NHL MoneyPuck Dashboard")

with st.sidebar:
    st.header("Data")
    if st.button("Download / Refresh DB"):
        download_db(DB_URL)
        st.cache_resource.clear()
        st.cache_data.clear()
        st.success("DB refreshed. Reload the page if needed.")

if not DB_PATH.exists():
    with st.spinner("Downloading latest DB..."):
        download_db(DB_URL)

con = get_con(str(DB_PATH))

# Example: League table
st.subheader("League Table (example)")
season = st.selectbox("Season", [r[0] for r in con.execute("SELECT DISTINCT season FROM fact_team_game ORDER BY season").fetchall()])
situation = st.selectbox("Situation", [r[0] for r in con.execute("SELECT DISTINCT situation FROM fact_team_game ORDER BY situation").fetchall()])

q = """
SELECT team,
       count(*) AS games,
       sum(goalsfor) AS gf,
       sum(goalsagainst) AS ga,
       sum(xgoalsfor) AS xgf,
       sum(xgoalsagainst) AS xga
FROM fact_team_game
WHERE season = ? AND situation = ?
GROUP BY team
ORDER BY xgf - xga DESC
"""
df = con.execute(q, [season, situation]).df()
st.dataframe(df, use_container_width=True)
