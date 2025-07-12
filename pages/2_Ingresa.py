import streamlit as st

st.set_page_config(page_title="Ingresa", page_icon="📝")

st.title("Subprocesos de Ingresa")
st.page_link("pages/3_FUAS.py", label="FUAS", icon="📄")
st.page_link("pages/4_Licitados.py", label="Licitados", icon="📄")
st.page_link("pages/5_Seguimientos.py", label="Seguimiento Firmas", icon="📄")
st.page_link("pages/6_IngresaRenovantes.py", label="Renovantes", icon="📄")
st.page_link("pages/7_Egresados.py", label="Egresados", icon="📄")
