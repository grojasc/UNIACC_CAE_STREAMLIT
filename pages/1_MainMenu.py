import streamlit as st
from helpers.style import local_css

st.set_page_config(page_title="Menú Principal", page_icon="🏠")
local_css("../style.css")

st.title("Menú Principal")

st.page_link("pages/2_Ingresa.py", label="Ir a Ingresa", icon="➡️")
st.page_link("pages/8_Validaciones.py", label="Ir a Validaciones", icon="➡️")
st.page_link("pages/9_Becas.py", label="Ir a Becas", icon="➡️")
