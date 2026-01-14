import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date, datetime
import plotly.express as px


from utils.filesystem import (
    get_continentes,
    get_paises,
    get_competiciones,
    get_temporadas
)
from utils.loader import load_json

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
st.set_page_config(layout="wide")
st.title("Equipos")

BASE_PATH = Path("scrap/data")

# --------------------------------------------------
# SESSION STATE
# --------------------------------------------------
if "selected_player" not in st.session_state:
    st.session_state.selected_player = None

if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0

# --------------------------------------------------
# FILTROS PRINCIPALES (RAW NAVIGATION)
# --------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    continente = st.selectbox("🌍 Continente", get_continentes())

with col2:
    pais = st.selectbox("🏳️ País", get_paises(continente))

with col3:
    competicion = st.selectbox(
        "🏆 Competición",
        get_competiciones(continente, pais)
    )

with col4:
    temporada = st.selectbox(
        "📅 Temporada",
        get_temporadas(continente, pais, competicion)
    )

# --------------------------------------------------
# VALIDAR SELECCIÓN COMPLETA
# --------------------------------------------------
if not all([continente, pais, competicion, temporada]):
    st.info("Seleccioná continente, país, competición y temporada")
    st.stop()

# --------------------------------------------------
# PATH DE TEMPORADA
# --------------------------------------------------
path_temporada = (
    BASE_PATH
    / continente
    / pais
    / competicion
    / temporada
)

# --------------------------------------------------
# Validar Edad
# --------------------------------------------------

def calcular_edad(fecha_nacimiento: str) -> int | None:
    if not fecha_nacimiento:
        return None

    try:
        birth = datetime.strptime(fecha_nacimiento[:10], "%Y-%m-%d")
        today = datetime.today()
        return (
            today.year
            - birth.year
            - ((today.month, today.day) < (birth.month, birth.day))
        )
    except Exception:
        return None


# --------------------------------------------------
# TABS
# --------------------------------------------------
tab1, tab2, tab3 = st.tabs([
    "👥 Plantel",
    "📊 Temporada",
    "📈 Análisis"
])

# --------------------------------------------------
# TAB 1 - JUGADORES ACTIVOS DEL EQUIPO
# --------------------------------------------------
st.subheader("👥 Jugadores del equipo que jugaron al menos 1 mín")

# --------------------------------------------------
# PATHS squads.json
# --------------------------------------------------
squad_path = path_temporada / "squads.json"

if not squad_path.exists():
    st.warning("No se encontró squads.json para esta temporada")
    st.stop()

# --------------------------------------------------
# PATH PLAYERSBIO (RAW - MISMA TEMPORADA)
# --------------------------------------------------
playersbio_base_path = path_temporada / "playersbio"

if not playersbio_base_path.exists():
    st.warning("No existe la carpeta playersbio para esta temporada")
    st.stop()

# --------------------------------------------------
# CARGAR SQUADS
# --------------------------------------------------
squad_data = load_json(squad_path)

if "squad" not in squad_data:
    st.error("Estructura inesperada en squads.json")
    st.stop()

squads = squad_data["squad"]

# --------------------------------------------------
# FILTRAR EQUIPO
# --------------------------------------------------
teams = sorted({s["contestantName"] for s in squads})
selected_team = st.selectbox("Equipo", teams)

team_data = next(
    s for s in squads if s["contestantName"] == selected_team
)

# ALIAS CLAROS (no cambia nada aguas arriba)
club_id = team_data["contestantId"]
club_name = team_data["contestantName"]

players = team_data.get("person", [])



# --------------------------------------------------
# OBTENER AÑO SELECCIONADO
# --------------------------------------------------
selected_year = temporada.split("-")[-1]  # "liga-pro-2025" → "2025"

# --------------------------------------------------
# FUNCIÓN: MINUTOS JUGADOS POR JUGADOR (RAW DATA)
# --------------------------------------------------
def get_minutes_played(player_id: str) -> int:
    matching_files = list(playersbio_base_path.glob(f"*_{player_id}.json"))

    if not matching_files:
        return 0

    bio = load_json(matching_files[0])
    persons = bio.get("person", [])

    if not persons:
        return 0

    person = persons[0]
    total_minutes = 0

    for membership in person.get("membership", []):
        for stat in membership.get("stat", []):

            calendar_name = str(stat.get("tournamentCalendarName", ""))
            minutes = int(stat.get("minutesPlayed", 0))

            if selected_year in calendar_name:
                total_minutes += minutes

    return total_minutes

# --------------------------------------------------
# CONSTRUIR LISTA DE JUGADORES VÁLIDOS (SOLO DEL CLUB)
# --------------------------------------------------
valid_players = []

for person in players:  # 👈 SOLO jugadores del equipo seleccionado

    player_id = person.get("id")
    if not player_id:
        continue

    minutes_played = get_minutes_played(player_id)

    # solo jugadores con minutos
    if minutes_played <= 0:
        continue

    valid_players.append({
        "player_id": player_id,
        "firstName": person.get("firstName", ""),
        "lastName": person.get("lastName", ""),
        "position": person.get("position", ""),
        "nationality": person.get("nationality", ""),
        "dateOfBirth": person.get("dateOfBirth"),
        "minutes_played": minutes_played
    })

if not valid_players:
    st.warning("No hay jugadores con minutos jugados en esta temporada")
    st.stop()
# --------------------------------------------------
# VALIDACIÓN
# --------------------------------------------------
if not valid_players:
    st.warning("No hay jugadores con minutos jugados en esta temporada")
    st.stop()
        
# --------------------------------------------------
# HEADER DEL CLUB + KPIs
# --------------------------------------------------
df_players = pd.DataFrame(valid_players)

# --------------------------------------------------
# ASEGURAR COLUMNA EDAD
# --------------------------------------------------
if "Edad" not in df_players.columns:
    df_players["Edad"] = df_players["dateOfBirth"].apply(calcular_edad)

# --------------------------------------------------
# MÉTRICAS DEL CLUB
# --------------------------------------------------
total_jugadores = len(df_players)

edad_promedio = (
    round(df_players["Edad"].mean(), 1)
    if total_jugadores > 0
    else 0
)

# ---------- POSICIÓN EN LA TABLA ----------
standing_path = path_temporada / "standings.json"
posicion_club = "N/A"

if standing_path.exists():
    standings = load_json(standing_path)

    try:
        ranking = (
            standings["stage"][0]
                     ["division"][0]
                     ["ranking"]
        )

        for row in ranking:
            if row.get("contestantId") == club_id:
                posicion_club = row.get("rank")
                break

    except (KeyError, IndexError, TypeError):
        posicion_club = "N/A"

# ---------- LAYOUT ----------
st.divider()

col_img, col_title = st.columns([1, 4])

with col_img:
    st.image(
        "https://via.placeholder.com/140x140.png?text=Club",
        width=120
    )

with col_title:
    st.markdown(
        f"""
        <div style="display:flex; align-items:center; height:100%;">
            <h2 style="text-align:center; width:100%;">🏟️ {club_name}</h2>
        </div>
        """,
        unsafe_allow_html=True
    )

# ---------- TARJETAS KPI ----------
kpi1, kpi2, kpi3 = st.columns(3)

with kpi1:
    st.metric(
        label="👥 Jugadores con minutos",
        value=total_jugadores
    )

with kpi2:
    st.metric(
        label="📊 Edad promedio",
        value=f"{edad_promedio} años"
    )

with kpi3:
    st.metric(
        label="🏆 Posición en la liga",
        value=posicion_club
    )

st.divider()


# --------------------------------------------------
# DATAFRAME DE JUGADORES VÁLIDOS
# --------------------------------------------------

# Nombre completo
df_players["Jugador"] = (
    df_players["firstName"].fillna("") + " " +
    df_players["lastName"].fillna("")
)

# Edad
df_players["Edad"] = df_players["dateOfBirth"].apply(calcular_edad)

# Renombrar columnas
df_players = df_players.rename(columns={
    "position": "Posición",
    "nationality": "Nacionalidad",
    "minutes_played": "Minutos"
})

# Seleccionar orden final
df_players = df_players[
    ["Jugador", "Posición", "Nacionalidad", "Edad", "Minutos"]
]

# Ordenar por minutos
df_players = df_players.sort_values("Minutos", ascending=False)

st.dataframe(
    df_players,
    use_container_width=True,
    hide_index=True
)

# --------------------------------------------------
# SEASON STATS (RAW DATA POR EQUIPO - JSON REAL)
# --------------------------------------------------
stats_dict = {}

seasonstats_dir = path_temporada / "seasonstats"

if not seasonstats_dir.exists():
    st.info("ℹ️ Carpeta seasonstats no encontrada")
else:
    season_file = next(
        (f for f in seasonstats_dir.glob("*.json") if club_id in f.name),
        None
    )

    if season_file is None:
        st.info("ℹ️ No hay estadísticas de temporada para este equipo")
    else:
        season_data = load_json(season_file)

        contestant = season_data.get("contestant", {})
        stats = contestant.get("stat", [])

        if not stats:
            st.info("ℹ️ Archivo sin estadísticas")
        else:
            stats_dict = {
                s.get("name"): float(s.get("value", 0))
                for s in stats
                if "name" in s and "value" in s
            }
# --------------------------------------------------
# MÉTRICAS AVANZADAS
# --------------------------------------------------
goals = stats_dict.get("Goals", 0)
total_shots = stats_dict.get("Total Shots", 0)
shots_on_target = stats_dict.get("Shots On Target ( inc goals )", 0)
possession = stats_dict.get("Possession Percentage", 0)

# --------------------------------------------------
# Tarjetas métricas avanzadas
# --------------------------------------------------

st.subheader("📊 Métricas de la temporada")

c1, c2, c3, c4 = st.columns(4)

c1.metric("⚽ Goles totales", int(goals))
c2.metric("📈 Total remates", int(total_shots))
c3.metric("🎯 Remates al arco", int(shots_on_target))
c4.metric("🟡 Posesión promedio", f"{possession}%")


# --------------------------------------------------
# SCATTER – ANÁLISIS COMPARATIVO (SEASONSTATS)
# --------------------------------------------------
st.subheader("📊 Análisis comparativo")

seasonstats_dir = path_temporada / "seasonstats"

if not seasonstats_dir.exists():
    st.warning("No existe la carpeta seasonstats")
    st.stop()

season_files = list(seasonstats_dir.glob("*.json"))

if not season_files:
    st.warning("No hay archivos seasonstats")
    st.stop()

# --------------------------------------------------
# MODO DE ANÁLISIS (SELECCIÓN ÚNICA)
# --------------------------------------------------
analysis_mode = st.radio(
    "Nivel de análisis",
    ["Equipos", "Jugadores"],
    horizontal=True
)

# ==================================================
# =============== MODO EQUIPOS =====================
# ==================================================
if analysis_mode == "Equipos":

    rows = []

    for file in season_files:
        data = load_json(file)
        contestant = data.get("contestant", {})
        stats = contestant.get("stat", [])

        row = {
            "Nombre": contestant.get("name"),
            "id": contestant.get("id"),
            "Tipo": "Equipo"
        }

        for s in stats:
            try:
                row[s["name"]] = float(s["value"])
            except:
                continue

        rows.append(row)

    df_scatter = pd.DataFrame(rows)
    highlight_filter = df_scatter["id"] == club_id

# ==================================================
# ============== MODO JUGADORES ====================
# ==================================================
else:

    rows = []

    for file in season_files:
        data = load_json(file)
        team = data.get("contestant", {})
        players = data.get("player", [])

        for p in players:
            stats = p.get("stat", [])

            row = {
                "Nombre": p.get("name"),
                "id": p.get("id"),
                "Equipo": team.get("name"),
                "EquipoId": team.get("id"),
                "Tipo": "Jugador"
            }

            for s in stats:
                try:
                    row[s["name"]] = float(s["value"])
                except:
                    continue

            rows.append(row)

    df_scatter = pd.DataFrame(rows)
    highlight_filter = df_scatter["EquipoId"] == club_id

# --------------------------------------------------
# VALIDACIONES
# --------------------------------------------------
if df_scatter.empty:
    st.warning("No hay datos para mostrar")
    st.stop()

numeric_cols = df_scatter.select_dtypes(include="number").columns.tolist()

if len(numeric_cols) < 2:
    st.warning("No hay suficientes métricas numéricas")
    st.stop()

# --------------------------------------------------
# SELECTORES DE MÉTRICAS
# --------------------------------------------------
colx, coly = st.columns(2)

with colx:
    x_metric = st.selectbox(
        "Eje X",
        numeric_cols,
        index=numeric_cols.index("Shots On Target (inc goals)")
        if "Shots On Target (inc goals)" in numeric_cols else 0
    )

with coly:
    y_metric = st.selectbox(
        "Eje Y",
        numeric_cols,
        index=1
    )

# --------------------------------------------------
# SCATTER PLOT
# --------------------------------------------------
fig = px.scatter(
    df_scatter,
    x=x_metric,
    y=y_metric,
    hover_name="Nombre",
    color="Tipo",
    opacity=0.5,
    title=f"{x_metric} vs {y_metric}"
)

# Resaltar club o jugadores del club
fig.add_scatter(
    x=df_scatter[highlight_filter][x_metric],
    y=df_scatter[highlight_filter][y_metric],
    mode="markers",
    marker=dict(size=16, color="red"),
    name="Seleccionado"
)

st.plotly_chart(fig, use_container_width=True)

# ==================================================
# TAB 2 — TEMPORADA
# ==================================================
with tab2:
    if st.session_state.selected_player is None:
        st.info("Seleccioná un jugador en el tab Plantel")
        st.stop()

    player = st.session_state.selected_player

    st.subheader(f"📊 Temporada — {player['matchName']}")

    colA, colB, colC = st.columns(3)

    with colA:
        st.metric("Posición", player.get("position", "-"))

    with colB:
        st.metric("Nacionalidad", player.get("nationality", "-"))

    with colC:
        st.metric("Altura", f"{player.get('height', '-') } cm")

    st.info(
        "Acá irán las estadísticas de temporada "
        "(seasonstats / matchstats)"
    )

# ==================================================
# TAB 3 — ANÁLISIS
# ==================================================
with tab3:
    if st.session_state.selected_player is None:
        st.info("Seleccioná un jugador primero")
        st.stop()

    st.subheader("📈 Análisis comparativo")

    st.info(
        "Este tab queda preparado para:\n"
        "- Comparación por posición\n"
        "- Percentiles\n"
        "- Radar charts\n"
        "- Tendencias"
    )
