import streamlit as st
from shared import DB_PATH, get_db_url, download_db, get_con

st.set_page_config(
    page_title="NHL Dashboard",
    page_icon="🏒",
    layout="wide",
)

st.title("🏒 NHL Dashboard")

# Ensure DB exists
if not DB_PATH.exists():
    db_url = get_db_url()
    with st.spinner("Downloading latest DB..."):
        download_db(db_url)

# smoke test connection
con = get_con(str(DB_PATH))
st.success(f"Database ready: {DB_PATH}")

st.markdown(
"""
Use the pages in the left navigation to explore:

- **League Table** (team aggregates, xGF%)
- **Teams** (game log + rolling trends)
- **Skaters** (player season summaries + game logs)
- **Goalies** (goalie season summaries + game logs)
- **Lines** (top line/pair combos by TOI and results)
- **Game Browser** (filter games by team/date)

If a page shows “missing table”, that means your **portable DB** didn’t include that relation —
you’d update `etl/build_portable_db.py` to copy it in.
"""
)
