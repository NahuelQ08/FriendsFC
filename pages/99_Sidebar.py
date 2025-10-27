import streamlit as st

st.title("⚙️ Panel de Configuración")

st.sidebar.header("🔧 Ajustes globales")

# Ejemplo: cambiar tema o configuración de API
modo_tema = st.sidebar.radio("Tema de color:", ["Oscuro", "Claro"])
api_endpoint = st.sidebar.text_input("📡 URL de la API:", "https://api.miapp.com")
autorefresh = st.sidebar.checkbox("Actualizar automáticamente", value=True)

st.write("### Ajustes actuales")
st.json({
    "Tema": modo_tema,
    "API": api_endpoint,
    "Auto-Refresh": autorefresh
})

if st.sidebar.button("💾 Guardar cambios"):
    st.success("Configuraciones actualizadas correctamente.")


st.markdown("""
    <style>
    [data-testid="stSidebarNav"] ul li:first-child {
        display: none !important;
    }
    </style>
    """, unsafe_allow_html=True) 