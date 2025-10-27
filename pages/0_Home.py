import streamlit as st

st.set_page_config(page_title="Home", page_icon="🏠", layout="wide")

st.title("🏠 Home - Proyecto")
st.markdown("### Selecciona una sección para comenzar:")

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

st.markdown("---")
st.markdown("📅 Dashboard deportivo creado con Streamlit | v1.0 | Tema oscuro profesional")