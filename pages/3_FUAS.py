import streamlit as st
from helpers import get_connection, read_any_file

st.set_page_config(page_title="FUAS", page_icon="📄")
conn = get_connection(
    server="PUACSCLBI.uniacc.local",
    database="DWH_DAI",
    user="usr_dwhppto",
    password="g8)yT1m23u7H",
)

st.title("Sub-proceso: FUAS")

uploaded = st.file_uploader("Cargar FUAS", type=["csv", "txt", "xlsx"])
df = read_any_file(uploaded)
if df is not None:
    st.success("Archivo cargado")
    st.dataframe(df.head())

    if st.button("Exportar FUAS"):
        df.to_excel("fuas_export.xlsx", index=False)
        st.toast("FUAS exportado", icon="📁")
