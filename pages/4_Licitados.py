import streamlit as st
from helpers.file_reader import read_any_file

st.set_page_config(page_title="Licitados", page_icon="📑")

st.title("Licitados")

uploaded = st.file_uploader("Cargar archivo de licitados", type=["csv", "xlsx"])
df = read_any_file(uploaded)
if df is not None:
    st.success("Archivo cargado")
    st.dataframe(df.head())
