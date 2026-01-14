import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from scipy.stats import percentileofscore
from pathlib import Path
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
from mplsoccer import VerticalPitch, FontManager , Pitch
import matplotlib.cm as cm
from matplotlib.colors import LinearSegmentedColormap
import numpy as np


from utils.visualizaciones import extract_player_shots, extract_all_season_shots, plot_shots_events
from utils.extraccion_match import extract_player_matchstats, extract_player_matches
from utils.filesystem import (
    get_continentes,
    get_paises,
    get_competiciones,
    get_temporadas
)
from utils.loader import load_json


# ==================================================
# Crear colormap personalizado (similar a pearl_earring)
# Degradado de oscuro a rojo/naranja
# ==================================================

colors_list = ['#15242e', '#2c3e50', '#e74c3c', '#f39c12', '#f1c40f']
n_bins = 100
pearl_earring_cmap = LinearSegmentedColormap.from_list(
    'pearl_earring', colors_list, N=n_bins
)

# --------------------------------------------------
# CONTROL DE TAB ACTIVO
# --------------------------------------------------
if "active_tab" not in st.session_state:
    st.session_state["active_tab"] = "perfil"


# --------------------------------------------------
# CONFIGURACIÓN DE PÁGINA
# --------------------------------------------------
st.set_page_config(layout="wide")
st.title("👤 Jugador – Análisis individual")

BASE_PATH = Path("scrap/data")

# --------------------------------------------------
# FILTROS GLOBALES
# --------------------------------------------------
col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    continente = st.selectbox(
        "🌍 Continente",
        get_continentes()
    )

with col2:
    pais = st.selectbox(
        "🏳️ País",
        get_paises(continente)
    )

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
# PATH TEMPORADA
# --------------------------------------------------
path_temporada = None

if all([continente, pais, competicion, temporada]):
    path_temporada = (
        BASE_PATH
        / continente
        / pais
        / competicion
        / temporada
    )
else:
    st.info("Seleccioná continente, país, competición y temporada")
    st.stop()

# --------------------------------------------------
# PATHS DE DATOS 
# --------------------------------------------------
match_dir = path_temporada / "matches"
matchstats_dir = path_temporada / "matchstats"

# --------------------------------------------------
# CARGAR SQUADS
# --------------------------------------------------
squad_path = path_temporada / "squads.json"

if not squad_path.exists():
    st.warning("No se encontró squads.json para esta temporada")
    st.stop()

squad_data = load_json(squad_path)
squads = squad_data.get("squad", [])

if not squads:
    st.warning("No hay información de squads")
    st.stop()

# --------------------------------------------------
# SELECTOR DE EQUIPO
# --------------------------------------------------
teams = sorted({s["contestantName"] for s in squads})

selected_team = st.selectbox("🏟 Equipo", teams)

team_data = next(
    s for s in squads if s["contestantName"] == selected_team
)

players_team = team_data.get("person", [])

# --------------------------------------------------
# SELECTOR DE JUGADOR
# --------------------------------------------------
players_dict = {
    f"{p.get('firstName', '')} {p.get('lastName', '')}".strip(): p["id"]
    for p in players_team
    if p.get("type") == "player"
}

selected_player_name = st.selectbox(
    "👤 Jugador",
    ["—"] + sorted(players_dict.keys())
)

# Evitar seguir si no hay jugador seleccionado
if selected_player_name == "—":
    st.stop()

selected_player_id = players_dict[selected_player_name]

st.divider()

# --------------------------------------------------
# CONTROL DE TAB ACTIVO
# --------------------------------------------------
if "selected_player_id" not in st.session_state:
    st.session_state["selected_player_id"] = None

# --------------------------------------------------
# TABS PRINCIPALES
# --------------------------------------------------
tab_perfil, tab_temporada, tab_historial, tab_comparativa, tab_similitud = st.tabs(
    [
        "👤 Perfil",
        "📊 Temporada",
        "🕒 Historial",
        "⚖️ Comparativa",
        "🧬 Similitud"
    ]
)

# --------------------------------------------------
# PERFIL DEL JUGADOR (DESDE SQUADS)
# --------------------------------------------------
with tab_perfil:
    if selected_player_name != "—":

        # ID del jugador seleccionado
        selected_player_id = players_dict[selected_player_name]

        # Datos completos del jugador desde squads
        player_data = next(
            p for p in players_team
            if p.get("id") == selected_player_id
        )

        # Edad
        dob = player_data.get("dateOfBirth")
        if dob:
            birth_date = datetime.strptime(dob, "%Y-%m-%d")
            today = datetime.today()
            edad = today.year - birth_date.year - (
                (today.month, today.day) < (birth_date.month, birth_date.day)
            )
        else:
            edad = "—"

        posicion = player_data.get("position", "—")
        altura = player_data.get("height", "—")
        peso = player_data.get("weight", "—")

        col_img, col_info = st.columns([1, 2])

        with col_img:
            st.markdown(
                "<div style='font-size:90px; text-align:center;'>🧑‍💼</div>",
                unsafe_allow_html=True
            )

        with col_info:
            st.markdown(f"## {selected_player_name}")
            st.markdown(f"**🏟 Club:** {selected_team}")
            st.markdown(f"**📍 Posición:** {posicion}")
            st.markdown(f"**🎂 Edad:** {edad} años")
            st.markdown(f"**📏 Altura:** {altura} cm")
            st.markdown(f"**⚖️ Peso:** {peso} kg")




    # --------------------------------------------------
    # Función para comparar jugador vs equipo / liga (SEASONSTATS)
    # --------------------------------------------------

    PLAYER_METRICS_BY_CATEGORY = {
        "🛡️ Defensa": {
            "Tackles Won": "Entradas ganadas",
            "Interceptions": "Intercepciones",
            "Duels won": "Duelos ganados",
            "Aerial Duels won": "Duelos aéreos ganados",
            "Tackles Won": "Entradas ganadas"
        },
        "🎯 Ataque": {
            "Goals": "Goles",
            "Total Shots": "Total Remates",
            "Shots On Target ( inc goals )" : "Remates al arco incluido goles",
        },
        "🧠 Pases": {
            "Total Passes" : "Pases totales",
            "Successful Short Passes": "Pases cortos exitosos",
            "Key Passes (Attempt Assists)": "Pases clave (Key pass)",
            "Assists": "Asistencias",
            "Final Third Touches" : "Toques en último tercio",
            "Forward Passes": "Pases hacia adelante"

        }
        
    }

    def extract_players_from_seasonstats(seasonstats_dir):
        players = []

        for file in seasonstats_dir.glob("*.json"):
            data = load_json(file)

            contestant = data.get("contestant", {})
            team_name = contestant.get("name")

            for p in data.get("player", []):
                row = {
                    "player_id": p.get("id"),
                    "player_name": p.get("matchName"),
                    "position": p.get("position"),
                    "team": team_name
                }

                for s in p.get("stat", []):
                    try:
                        row[s["name"]] = float(s["value"])
                    except:
                        continue

                players.append(row)

        return pd.DataFrame(players)

    # --------------------------------------------------
    # FUNCIÓN GLOBAL: jugadores de TODAS las ligas
    # --------------------------------------------------
    def extract_players_global(base_path):
        players = []
        leagues_loaded = set()

        for continent in base_path.iterdir():
            if not continent.is_dir():
                continue

            for country in continent.iterdir():
                if not country.is_dir():
                    continue

                for competition in country.iterdir():
                    if not competition.is_dir():
                        continue

                    for season in competition.iterdir():
                        seasonstats_dir = season / "seasonstats"
                        if not seasonstats_dir.exists():
                            continue

                        league_name = f"{continent.name} / {country.name} / {competition.name} / {season.name}"
                        leagues_loaded.add(league_name)

                        for file in seasonstats_dir.glob("*.json"):
                            data = load_json(file)
                            contestant = data.get("contestant", {})
                            team_name = contestant.get("name")

                            for p in data.get("player", []):
                                row = {
                                    "player_id": p.get("id"),
                                    "player_name": p.get("matchName"),
                                    "position": p.get("position"),
                                    "team": team_name,
                                    "league": league_name
                                }

                                for s in p.get("stat", []):
                                    try:
                                        row[s["name"]] = float(s["value"])
                                    except:
                                        continue

                                players.append(row)

        return pd.DataFrame(players), sorted(leagues_loaded)



    # --------------------------------------------------
    # DATAFRAME DE JUGADORES DE LA LIGA
    # --------------------------------------------------
    seasonstats_dir = path_temporada / "seasonstats"

    df_players_league = extract_players_from_seasonstats(seasonstats_dir)

    if df_players_league.empty:
        st.warning("No hay datos de jugadores en seasonstats")
        st.stop()

    # --------------------------------------------------
    # JUGADOR SELECCIONADO
    # --------------------------------------------------
    player_id = selected_player_id

    player_row = df_players_league[
        df_players_league["player_id"] == player_id
    ]

    if player_row.empty:
        st.warning("El jugador no tiene estadísticas en esta temporada")
        st.stop()

    player_row = player_row.iloc[0]
    player_position = player_row["position"]
    player_team = player_row["team"]


    # ==================================================
    # bloque de selección de comparación
    # ==================================================

    comparison_mode = st.radio(
        "Comparar contra",
        ["Equipo", "Liga", "Global"],
        horizontal=True
    )

    if comparison_mode == "Equipo":
        df_compare = df_players_league[
            (df_players_league["team"] == player_team) &
            (df_players_league["position"] == player_position)
        ]

    elif comparison_mode == "Liga":
        df_compare = df_players_league[
            df_players_league["position"] == player_position
        ]

    else:  # GLOBAL
        df_players_global, leagues_loaded = extract_players_global(BASE_PATH)

        # 🔍 DEBUG visual
        with st.expander("🌍 Ligas incluidas en comparación global"):
            for lg in leagues_loaded:
                st.markdown(f"- {lg}")

        if df_players_global.empty:
            st.warning("No hay datos globales disponibles")
            st.stop()

        df_compare = df_players_global[
            df_players_global["position"] == player_position
        ]


    # --------------------------------------------------
    # COLOR SEGÚN PERCENTIL (4 NIVELES)
    # --------------------------------------------------
    def percentile_to_color(pct):
        if pct >= 85:
            return "#00c8ff"   # celeste (élite)
        elif pct >= 65:
            return "#2ecc71"   # verde
        elif pct >= 35:
            return "#f1c40f"   # amarillo
        else:
            return "#e74c3c"   # rojo


    # --------------------------------------------------
    # TARJETAS KPI – RESUMEN DE TEMPORADA (SEASONSTATS)
    # --------------------------------------------------

    KPI_METRICS = [
        ("Appearances", "Apariciones"),
        ("Games Played", "Partidos jugados"),
        ("Substitute Off", "Salió de cambio"),
        ("Substitute On", "Entró de cambio"),
    ]

    # Segunda fila (placeholders)
    KPI_METRICS_2 = [
        ("Time Played", "Tiempo Jugado (min)"),
        ("Yellow Cards", "Tarjetas Amarillas"),
        ("Total Red Cards", "Tarjetas Rojas"),
        ("", "Efectividad Pases %"),
    ]

    def get_player_metric(player_row, metric_name):
        """
        Devuelve el valor de la métrica o 0 si no existe / es NaN
        """
        if not metric_name:
            return "—"

        val = player_row.get(metric_name, 0)
        if pd.isna(val):
            return 0
        return int(val) if float(val).is_integer() else round(val, 1)


    def render_kpi_row(metrics):
        cols = st.columns(4)

        for col, (metric_key, metric_label) in zip(cols, metrics):
            value = get_player_metric(player_row, metric_key)

            with col:
                st.markdown(
                    f"""
                    <div style="
                        background-color:#2e2e2e;
                        padding:14px;
                        border-radius:10px;
                        text-align:center;
                    ">
                        <div style="
                            font-size:22px;
                            font-weight:700;
                            color:white;
                        ">
                            {value}
                        </div>
                        <div style="
                            font-size:13px;
                            color:#bdbdbd;
                            margin-top:4px;
                        ">
                            {metric_label}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )


    st.subheader("📋 Resumen de temporada")

    # Fila 1
    render_kpi_row(KPI_METRICS)

    st.markdown("")

    # Fila 2 (placeholders)
    render_kpi_row(KPI_METRICS_2)

    # ==================================================
    # ESPACIO
    # ==================================================
    st.markdown("")
    st.divider()
    st.markdown("")

    # --------------------------------------------------
    # BARRAS TIPO FOTMOB – PERCENTIL REAL (POR CATEGORÍA)
    # --------------------------------------------------

    from scipy.stats import percentileofscore

    def compute_percentile(series, value):
        series = series.fillna(0)
        if series.empty:
            return 0
        return percentileofscore(series, value, kind="rank")

    def percentile_to_color(pct):
        if pct >= 85:
            return "#00c8ff"   # celeste
        elif pct >= 65:
            return "#2ecc71"   # verde
        elif pct >= 35:
            return "#f1c40f"   # amarillo
        else:
            return "#e74c3c"   # rojo


    st.subheader("📊 Comparación por percentil")

    # ==================================================
    # ITERAR POR CATEGORÍAS
    # ==================================================
    for category, metrics in PLAYER_METRICS_BY_CATEGORY.items():

        st.markdown(f"### {category}")

        for metric_name in metrics.keys():

            # 🔹 Asegurar columna
            if metric_name not in df_compare.columns:
                df_compare[metric_name] = 0

            values = df_compare[metric_name].fillna(0)

            player_val = player_row.get(metric_name, 0)
            if pd.isna(player_val):
                player_val = 0

            pct = compute_percentile(values, player_val)
            color = percentile_to_color(pct)

            col1, col2, col3 = st.columns([2.8, 0.8, 4])

            with col1:
                st.markdown(f"**{metric_name}**")

            with col2:
                st.markdown(f"{round(player_val, 2)}")

            with col3:
                st.markdown(
                    f"""
                    <div title="Percentil: {round(pct,1)}%"
                        style="background-color:#3a3a3a;
                                border-radius:6px;
                                height:10px;
                                width:100%;">
                        <div style="
                            width:{pct}%;
                            background-color:{color};
                            height:10px;
                            border-radius:6px;">
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

# --------------------------------------------------
# BOTÓN → IR A TAB TEMPORADA
# --------------------------------------------------
if st.button("📊 Ver temporada del jugador", use_container_width=True):
    st.session_state["selected_player_id"] = selected_player_id
    st.session_state["selected_player_name"] = selected_player_name
    st.session_state["selected_team"] = selected_team
    st.session_state["selected_season"] = temporada






# ==================================================
# TAB 2 – TEMPORADA (matches + events)
# ==================================================
with tab_temporada:

    # --------------------------------------------------
    # VALIDACIÓN DE JUGADOR
    # --------------------------------------------------
    if st.session_state.get("selected_player_id") is None:
        st.info("👈 Selecciona un jugador en el tab Perfil")
        st.stop()

    player_id = st.session_state.get("selected_player_id")
    player_name = st.session_state.get("selected_player_name")
    team_name = st.session_state.get("selected_team")
    season_name = st.session_state.get("selected_season")

    # --------------------------------------------------
    # PATH DE PARTIDOS
    # --------------------------------------------------
    match_dir = path_temporada / "matches"

    if not match_dir.exists():
        st.warning("No existe la carpeta matches para esta temporada")
        st.stop()

    # --------------------------------------------------
    # PARTIDOS DEL JUGADOR
    # --------------------------------------------------
    df_player_matches = extract_player_matches(match_dir, player_id)

    if df_player_matches.empty:
        st.warning("Este jugador no registra partidos en la temporada seleccionada")
        st.stop()

    # --------------------------------------------------
    # TÍTULO Y SUBTÍTULO
    # --------------------------------------------------
    st.markdown(
        f"""
        <h1 style="text-align:center;">
            🎥 Eventos – {player_name}
        </h1>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        f"<p style='text-align:center;color:#aaa;'>"
        f"{team_name} · Temporada {season_name}"
        f"</p>",
        unsafe_allow_html=True
    )

    st.divider()

    
    # --------------------------------------------------
    # RESUMEN DE ESTADÍSTICAS DE TEMPORADA
    # --------------------------------------------------
    SEASON_KPIS = {
        "Appearances": "Apariciones",
        "Games Played": "Partidos jugados",
        "Time Played": "Minutos jugados",
        "Goals": "Goles",
        "Assists": "Asistencias",
        "Total Shots": "Remates",
        "Shooting Accuracy": "Precisión de tiro %",
        "Key Passes (Attempt Assists)": "Pases clave",
        "Tackles Won": "Entradas ganadas",
        "Interceptions": "Intercepciones",
        "Duels won": "Duelos ganados",
        "Aerial Duels won": "Duelos aéreos ganados"
    }
    
    def render_season_kpis(player_row, metrics, cols_per_row=4):
        items = list(metrics.items())

        for i in range(0, len(items), cols_per_row):
            row_items = items[i:i + cols_per_row]
            cols = st.columns(cols_per_row)

            for col, (metric_key, label) in zip(cols, row_items):
                value = player_row.get(metric_key, 0)
                if pd.isna(value):
                    value = 0

                with col:
                    st.markdown(
                        f"""
                        <div style="
                            background-color:#2b2b2b;
                            padding:14px;
                            border-radius:10px;
                            margin-bottom:5px;
                            text-align:center;">
                            <div style="font-size:12px;color:#aaa;">
                                {label}
                            </div>
                            <div style="font-size:26px;font-weight:600;">
                                {int(value) if value == int(value) else round(value,1)}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

    st.subheader("📊 Resumen de la temporada")

    render_season_kpis(
        player_row=player_row,
        metrics=SEASON_KPIS,
        cols_per_row=4
    )

    # --------------------------------------------------
    # ÚLTIMOS PARTIDOS CON ESTADÍSTICAS
    # --------------------------------------------------
    st.subheader("📋 Estadísticas por partido")

    matchstats_dir = path_temporada / "matchstats"

    if not matchstats_dir.exists():
        st.warning("No existe la carpeta matchstats para esta temporada")
    else:
        # Extraer estadísticas del jugador
        df_matchstats = extract_player_matchstats(matchstats_dir, player_id)
        
        if df_matchstats.empty:
            st.info("No hay estadísticas de partidos disponibles para este jugador")
        else:
            # --------------------------------------------------
            # PAGINACIÓN
            # --------------------------------------------------
            if "matchstats_page" not in st.session_state:
                st.session_state["matchstats_page"] = 0
            
            PARTIDOS_POR_PAGINA = 5
            total_partidos = len(df_matchstats)
            total_paginas = (total_partidos + PARTIDOS_POR_PAGINA - 1) // PARTIDOS_POR_PAGINA
            
            # Calcular índices
            inicio = st.session_state["matchstats_page"] * PARTIDOS_POR_PAGINA
            fin = inicio + PARTIDOS_POR_PAGINA
            df_page = df_matchstats.iloc[inicio:fin]
            
            # --------------------------------------------------
            # INFORMACIÓN DE PAGINACIÓN
            # --------------------------------------------------
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col1:
                if st.button("⬅️ Anterior", use_container_width=True):
                    if st.session_state["matchstats_page"] > 0:
                        st.session_state["matchstats_page"] -= 1
                        st.rerun()
            
            with col2:
                st.markdown(
                    f"<p style='text-align:center; color:#aaa;'>"
                    f"Página {st.session_state['matchstats_page'] + 1} de {total_paginas} "
                    f"({total_partidos} partidos)"
                    f"</p>",
                    unsafe_allow_html=True
                )
            
            with col3:
                if st.button("Siguiente ➡️", use_container_width=True):
                    if st.session_state["matchstats_page"] < total_paginas - 1:
                        st.session_state["matchstats_page"] += 1
                        st.rerun()
            
            st.markdown("")
            
            # --------------------------------------------------
            # TABLA DE ESTADÍSTICAS POR PARTIDO
            # --------------------------------------------------
            # Preparar datos para mostrar
            df_display = df_page.copy()
            df_display["date"] = pd.to_datetime(df_display["date"]).dt.strftime("%d/%m/%Y")
            df_display = df_display[[
                "date", "match", "minutes", "goals", "assists", "yellow", "red", "started"
            ]]
            
            df_display.columns = [
                "📅 Fecha", "🎯 Partido", "⏱️ Min", "⚽ Goles", "🅰️ Asist", 
                "🟨 Amarilla", "🔴 Roja", "▶️ Titular"
            ]
            
            st.dataframe(
                df_display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "⏱️ Min": st.column_config.NumberColumn(format="%d"),
                    "⚽ Goles": st.column_config.NumberColumn(format="%d"),
                    "🅰️ Asist": st.column_config.NumberColumn(format="%d"),
                    "🟨 Amarilla": st.column_config.NumberColumn(format="%d"),
                    "🔴 Roja": st.column_config.NumberColumn(format="%d"),
                    "▶️ Titular": st.column_config.NumberColumn(format="%d"),
                }
            )
    

    
    # ==================================================
    # ESPACIO
    # ==================================================
    st.markdown("")
    st.divider()
    st.markdown("")
    # DEBUG - antes del gráfico

    # ==================================================
    # ANÁLISIS DE DISPAROS
    # ==================================================
    
    st.subheader("⚽ Disparos del jugador")
    
    # Agregar opción "Todos" al inicio
    match_options = ["📊 Todos los partidos"] + df_player_matches["label"].tolist()
    
    selected_match_label = st.selectbox(
        "Selecciona un partido",
        options=match_options,
        label_visibility="collapsed",
        key="partido_disparos"
    )

    st.markdown("")
    
    # Si selecciona "Todos", mostrar todos los disparos de la temporada
    if selected_match_label == "📊 Todos los partidos":
        # Extraer TODOS los disparos de la temporada
        all_season_shots = []
        
        for match_file in match_dir.glob("*.json"):
            df_match_shots = extract_player_shots(match_file, player_id)
            if not df_match_shots.empty:
                all_season_shots.append(df_match_shots)
        
        if all_season_shots:
            df_shots_partido = pd.concat(all_season_shots, ignore_index=True)
        else:
            df_shots_partido = pd.DataFrame()
        
        titulo_disparos = f"{player_name} - Temporada completa"
    
    else:
        # Si selecciona un partido específico
        selected_match_row = df_player_matches[
            df_player_matches["label"] == selected_match_label
        ].iloc[0]
        
        match_file = match_dir / selected_match_row["file"]
        df_shots_partido = extract_player_shots(match_file, player_id)
        titulo_disparos = f"{player_name} - {selected_match_label}"

    if df_shots_partido.empty:
        st.info(f"❌ {player_name} no tiene disparos en esta selección")
    else:
        # Contar disparos por tipo
        goles = len(df_shots_partido[df_shots_partido["typeId"] == 16])
        misses = len(df_shots_partido[df_shots_partido["typeId"] == 13])
        atajados = len(df_shots_partido[df_shots_partido["typeId"] == 15])
        
        # Métricas
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Total", len(df_shots_partido))
        with col2:
            st.metric("⚽ Goles", goles)
        with col3:
            st.metric("❌ Miss", misses)
        with col4:
            st.metric("🧤 Atajados", atajados)
        
        st.write(f"DEBUG - Disparos a visualizar: {len(df_shots_partido)}")
        st.write("TypeIds:")
        st.write(df_shots_partido['typeId'].value_counts().sort_index())

        # Verificar misses específicamente
        misses_check = df_shots_partido[df_shots_partido['typeId'] == 13]
        st.write(f"Misses encontrados: {len(misses_check)}")
        if not misses_check.empty:
            st.write("Coordenadas de misses (originales):")
            st.write(misses_check[['x', 'y']].head())

        # Ahora el gráfico
        fig = plot_shots_events(df_shots_partido, titulo_disparos, figsize=(10, 12))

        st.markdown("")
        
        # GRÁFICO - lo importante
        fig = plot_shots_events(df_shots_partido, titulo_disparos, figsize=(10, 12))
        if fig:
            st.pyplot(fig, use_container_width=True)
        else:
            st.warning("No se pudo generar el gráfico")




# ==================================================
# TAB 3 – HISTORIAL (playersbio)
# ==================================================
with tab_historial:
    st.subheader("🕒 Historial del jugador")

    st.info(
        "Trayectoria completa del jugador.\n\n"
        "Fuente: playersbio\n\n"
        "• Clubes\n"
        "• Temporadas\n"
        "• Competencias\n"
        "• Minutos / goles por temporada"
    )

# ==================================================
# TAB 4 – COMPARATIVA
# ==================================================
with tab_comparativa:
    st.subheader("⚖️ Comparativa con otros jugadores")

    st.info(
        "Comparación del jugador contra otros jugadores de la liga.\n\n"
        "• Scatter\n"
        "• Percentiles\n"
        "• Ranking por métrica\n"
        "• Filtros por posición"
    )

# ==================================================
# TAB 5 – SIMILITUD (FUTURO)
# ==================================================
with tab_similitud:
    st.subheader("🧬 Similitud de jugadores")

    st.info(
        "Modelo de similitud entre jugadores.\n\n"
        "• Distancia estadística\n"
        "• Jugadores similares\n"
        "• Radar comparativo\n\n"
        "🚧 En desarrollo"
    )

# --------------------------------------------------
# FOOTER
# --------------------------------------------------
st.caption(
    "Dashboard de análisis · Jugadores · Estructura base"
)
