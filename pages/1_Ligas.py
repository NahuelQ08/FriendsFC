import streamlit as st

st.set_page_config(page_title="Ligas", page_icon="🏆", layout="wide", initial_sidebar_state="collapsed")

st.title("🏆 Ligas")

tab1, tab2, tab3 = st.tabs(["Estadísticas", "Equipos", "Historial"])

with tab1:
    st.subheader("📊 Estadísticas de Ligas")
    st.info("Aquí podrás mostrar gráficos de rendimiento, posiciones, etc.")

with tab2:
    st.subheader("⚙️ Equipos por Liga")
    st.write("Tabla o filtros de equipos participantes.")

with tab3:
    st.subheader("📅 Historial de Ligas")
    st.write("Comparativas por temporadas.")
