import streamlit as st

st.set_page_config(page_title="Clubes", page_icon="🏟️", layout="wide", initial_sidebar_state="collapsed")

st.title("🏟️ Clubes")

tab1, tab2, tab3 = st.tabs(["Datos", "Rendimiento", "Posicionamiento"])

with tab1:
    st.subheader("📋 Datos del Clubes")
    st.write("Información general para clubes.")

with tab2:
    st.subheader("📈 Rendimiento Deportivo")
    st.write("Información general para clubes.")

with tab3:
    st.subheader("📈 Posicionamiento")
    st.write("Información general para clubes.")