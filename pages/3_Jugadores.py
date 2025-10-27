import streamlit as st

st.set_page_config(page_title="Jugadores", page_icon="⚽", layout="wide", initial_sidebar_state="collapsed")

st.title("⚽ Jugadores")

tab1, tab2, tab3 = st.tabs(["Perfil", "Rendimiento", "Historial"])

with tab1:
    st.subheader("🧍 Perfil del Jugador")
    st.write("Datos personales, posición, edad, etc.")

with tab2:
    st.subheader("📈 Rendimiento")
    st.write("Gráficos de estadísticas individuales.")

with tab3:
    st.subheader("📅 Historial")
    st.write("Trayectoria y temporadas.")
