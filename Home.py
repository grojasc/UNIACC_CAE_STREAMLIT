import streamlit as st
from helpers.style import local_css

st.set_page_config(page_title="UNIACC", page_icon="🎓", layout="wide")
local_css("style.css")

if "auth" not in st.session_state:
    st.session_state.auth = False

if not st.session_state.auth:
    st.title("Login de Usuario")
    user = st.text_input("Usuario")
    passwd = st.text_input("Contraseña", type="password")
    if st.button("Ingresar"):
        if user == "admin" and passwd == "12345":
            st.session_state.auth = True
            st.toast("Bienvenido/a " + user, icon="✅")
        else:
            st.error("Usuario o contraseña inválidos")
    st.stop()

st.success("Autenticado correctamente.")
st.page_link("pages/1_MainMenu.py", label="Ir al menú principal")
