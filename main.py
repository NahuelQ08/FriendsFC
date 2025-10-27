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

# Tema oscuro
# st.markdown("""
#     <style>
#         body {
#             background-color: #0e1117;
#             color: #fafafa;
#         }
#         .stButton>button {
#             background-color: #262730;
#             color: white;
#             border-radius: 10px;
#             border: 1px solid #333;
#             padding: 0.5em 1.5em;
#             transition: 0.3s;
#         }
#         .stButton>button:hover {
#             background-color: #3b3c47;
#         }
#         .stTabs [data-baseweb="tab"] {
#             background-color: #1a1d23;
#             color: #fafafa;
#         }
#         .stTabs [data-baseweb="tab"]:hover {
#             background-color: #2a2e38;
#         }
#     </style>
# """, unsafe_allow_html=True)

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
