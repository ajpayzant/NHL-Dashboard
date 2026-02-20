# app/streamlit_app.py
import os
from pathlib import Path
import streamlit as st
import requests

from shared import DB_PATH

st.set_page_config(page_title="NHL Dashboard", layout="wide")

st.title("NHL Dashboard")

# Sidebar is allowed, but not the only place filters live
with st.sidebar:
    st.header("Data")
    st.write(f"DB: `{DB_PATH}`")

    # Optional: allow manual refresh if you host the DB as a URL asset
    DB_URL = os.getenv("DB_URL", "")
    if st.button("Download / Refresh DB"):
        if not DB_URL:
            st.error("DB_URL not set (Streamlit Secrets or environment).")
        else:
            tmp = DB_PATH.with_suffix(".part")
            r = requests.get(DB_URL, stream=True, timeout=180)
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            tmp.replace(DB_PATH)
            st.success("DB refreshed. Reload the app if needed.")
