import streamlit as st
from login import login_screen

st.set_page_config(
    page_title="Proyecto Deportivo",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# === Aplicar estilos personalizados ===
with open("assets/style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


# Verificar login
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    login_screen()
else:
    st.title("🏠 Home - Proyecto Deportivo")
    st.markdown("### Selecciona una sección:")

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🏆 Ligas", use_container_width=True):
            st.switch_page("pages/1_Ligas.py")

    with col2:
        if st.button("🏟️ Clubes", use_container_width=True):
            st.switch_page("pages/2_Clubes.py")

    with col3:
        if st.button("⚽ Jugadores", use_container_width=True):
            st.switch_page("pages/3_Jugadores.py")
