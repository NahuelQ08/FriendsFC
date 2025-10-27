import streamlit as st
from utils.auth import check_credentials

def login_screen():
    st.image("assets/logo.png", width=150)
    st.markdown("## Bienvenido al Dashboard Deportivo")

    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Iniciar Sesión", use_container_width=True):
        if check_credentials(username, password):
            st.session_state.logged_in = True
            st.experimental_rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")