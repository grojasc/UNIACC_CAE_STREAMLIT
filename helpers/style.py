import streamlit as st
from pathlib import Path


def local_css(file_name: str):
    path = Path(file_name)
    if path.exists():
        st.markdown(f"<style>{path.read_text()}</style>", unsafe_allow_html=True)
