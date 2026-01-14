import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px
from collections import defaultdict


from utils.filesystem import (
    get_continentes,
    get_paises,
    get_competiciones,
    get_temporadas,
)
from utils.loader import load_json

from utils.visual_liga import extract_duel_timeseries, get_league_nationalities



# --------------------------------------------------
# CONFIG
# --------------------------------------------------
st.set_page_config(layout="wide")
st.title("📊 Ligas | Visión Histórica")

BASE_PATH = Path("scrap/data")

# --------------------------------------------------
# FILTROS PRINCIPALES (HASTA LIGA)
# --------------------------------------------------
col1, col2, col3 = st.columns(3)

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
    liga = st.selectbox(
        "🏆 Liga",
        get_competiciones(continente, pais)
    )

st.divider()

# --------------------------------------------------
# VALIDACIÓN BÁSICA
# --------------------------------------------------
if not continente or not pais or not liga:
    st.info("Seleccioná continente, país y liga")
    st.stop()

# --------------------------------------------------
# OBTENER TEMPORADAS DISPONIBLES (SIN SELECTOR)
# --------------------------------------------------
temporadas = get_temporadas(continente, pais, liga)

if not temporadas:
    st.warning("No hay temporadas disponibles para esta liga")
    st.stop()

# Tomamos las últimas 5 disponibles (o menos si no hay)
temporadas = sorted(temporadas)[-5:]
st.divider()

# --------------------------------------------------
# CÁLCULO DE MÉTRICAS HISTÓRICAS
# --------------------------------------------------
records = []

for temporada in temporadas:
    standings_path = (
        BASE_PATH
        / continente
        / pais
        / liga
        / temporada
        / "standings.json"
    )

    if not standings_path.exists():
        continue

    standings = load_json(standings_path)

    try:
        ranking = (
            standings["stage"][0]
                     ["division"][0]
                     ["ranking"]
        )
    except (KeyError, IndexError, TypeError):
        continue

    if not ranking:
        continue

    n_teams = len(ranking)

    total_points = sum(team.get("points", 0) for team in ranking)
    total_goals = sum(team.get("goalsFor", 0) for team in ranking)
    total_matches_team = sum(team.get("matchesPlayed", 0) for team in ranking)

    total_matches_league = total_matches_team / 2
    if total_matches_league == 0:
        continue

    records.append({
        "season": temporada,
        "avg_points_per_team": round(total_points / n_teams, 2),
        "goals_per_match": round(total_goals / total_matches_league, 2)
    })


# --------------------------------------------------
# Nacionalidades de jugadores en la liga
# --------------------------------------------------
squads_path = (
    BASE_PATH
    / continente
    / pais
    / liga
    / temporada
    / "squads"
)

rows = get_league_nationalities(squads_path)
df_nat = pd.DataFrame(rows)


# --------------------------------------------------
# VALIDACIÓN FINAL
# --------------------------------------------------
if not records:
    st.warning("No hay información histórica disponible para esta liga")
    st.stop()

df_metrics = pd.DataFrame(records).sort_values("season")

# --------------------------------------------------
# POSESIÓN MEDIA POR EQUIPO (ÚLTIMAS TEMPORADAS)
# --------------------------------------------------
team_possession = {}

for temporada in temporadas:
    seasonstats_path = (
        BASE_PATH
        / continente
        / pais
        / liga
        / temporada
        / "seasonstats"
    )

    if not seasonstats_path.exists():
        continue

    for file in seasonstats_path.glob("*.json"):
        data = load_json(file)

        try:
            team_name = data["contestant"]["name"]
            stats = data["contestant"]["stat"]
        except (KeyError, TypeError):
            continue

        possession = None
        for s in stats:
            if s.get("name") == "Possession Percentage":
                possession = float(s.get("value", 0))
                break

        if possession is None:
            continue

        team_possession.setdefault(team_name, []).append(possession)

#===================================================
# DATAFRAME POSESIÓN MEDIA
#===================================================

records_possession = [
    {
        "Equipo": team,
        "Posesión media (%)": sum(values) / len(values)
    }
    for team, values in team_possession.items()
]

df_possession = pd.DataFrame(records_possession)

if df_possession.empty:
    st.warning("No hay datos de posesión disponibles")
    st.stop()

df_possession = df_possession.sort_values(
    "Posesión media (%)",
    ascending=False
)

#===================================================
# DATAFRAME TIROS TOTALES
#===================================================

df_shots = []

for temporada in temporadas:
    seasonstats_path = (
        BASE_PATH
        / continente
        / pais
        / liga
        / temporada
        / "seasonstats"
    )

    if not seasonstats_path.exists():
        continue

    for file in seasonstats_path.glob("*.json"):
        data = load_json(file)

        team_name = data.get("contestant", {}).get("name")
        stats = data.get("contestant", {}).get("stat", [])

        for s in stats:
            if s.get("name") == "Total Shots":
                try:
                    df_shots.append({
                        "Equipo": team_name,
                        "Total Shots": float(s["value"])
                    })
                except:
                    pass

df_shots = pd.DataFrame(df_shots)

#==========================================================
# Data for scatter plot: Shots on Target vs Goals
#==========================================================

df_scatter_team = []

for temporada in temporadas:
    seasonstats_path = (
        BASE_PATH
        / continente
        / pais
        / liga
        / temporada
        / "seasonstats"
    )

    if not seasonstats_path.exists():
        continue

    for file in seasonstats_path.glob("*.json"):
        data = load_json(file)

        team = data.get("contestant", {}).get("name")
        stats = data.get("contestant", {}).get("stat", [])

        shots_on_target = None
        goals = None

        for s in stats:
            if s.get("name") == "Shots On Target ( inc goals )":
                shots_on_target = float(s["value"])
            if s.get("name") == "Goals":
                goals = float(s["value"])

        if team and shots_on_target is not None and goals is not None:
            df_scatter_team.append({
                "Equipo": team,
                "Remates al arco": shots_on_target,
                "Goles": goals
            })

df_scatter_team = pd.DataFrame(df_scatter_team)

#==================================================
# Data frame para la efectividad de los pases para cada equipo
#==================================================
passes_agg = defaultdict(lambda: {"successful": 0, "unsuccessful": 0})

for temporada in temporadas:
    seasonstats_path = (
        BASE_PATH
        / continente
        / pais
        / liga
        / temporada
        / "seasonstats"
    )

    if not seasonstats_path.exists():
        continue

    for file in seasonstats_path.glob("*.json"):
        data = load_json(file)

        team = data.get("contestant", {}).get("name")
        stats = data.get("contestant", {}).get("stat", [])

        successful = 0
        unsuccessful = 0

        for s in stats:
            name = s.get("name", "").lower()

            if "total successful passes" in name:
                successful = int(s["value"])

            if "total unsuccessful passes" in name:
                unsuccessful = int(s["value"])

        if team:
            passes_agg[team]["successful"] += successful
            passes_agg[team]["unsuccessful"] += unsuccessful


# Construcción del DataFrame FINAL
df_pass_eff_team = []

for team, values in passes_agg.items():
    total_passes = values["successful"] + values["unsuccessful"]

    if total_passes == 0:
        continue

    effectiveness = (values["successful"] * 100) / total_passes

    df_pass_eff_team.append({
        "Equipo": team,
        "Efectividad de pase (%)": effectiveness
    })

df_pass_eff_team = pd.DataFrame(df_pass_eff_team)




#---------------------------------------------------
# KPIs PRINCIPALES
# --------------------------------------------------
avg_points = df_metrics["avg_points_per_team"].mean()
avg_goals = df_metrics["goals_per_match"].mean()

col_kpi1, col_kpi2 = st.columns(2)

with col_kpi1:
    st.metric(
        "⚽ Promedio de puntos por equipo (histórico)",
        round(avg_points, 2)
    )

with col_kpi2:
    st.metric(
        "🥅 Goles por partido (histórico)",
        round(avg_goals, 2)
    )

st.divider()

# --------------------------------------------------
# TABLA HISTÓRICA
# --------------------------------------------------
st.subheader("📅 Últimas temporadas disponibles")

df_show = df_metrics.rename(columns={
    "season": "Temporada",
    "avg_points_per_team": "Prom. puntos por equipo",
    "goals_per_match": "Goles por partido"
})

st.dataframe(
    df_show,
    use_container_width=True,
    hide_index=True
)

#==================================================
# Funcion para analizar la posesión por cada liga en los ultimos 5 season
#==================================================
def compute_stat_by_league(
    base_path,
    stat_name: str,
    n_seasons: int = 5
):
    records = []

    for continente in get_continentes():
        for pais in get_paises(continente):
            for liga_name in get_competiciones(continente, pais):

                temporadas = get_temporadas(continente, pais, liga_name)
                temporadas = sorted(temporadas)[-n_seasons:]

                values = []

                for temporada in temporadas:
                    seasonstats_path = (
                        base_path
                        / continente
                        / pais
                        / liga_name
                        / temporada
                        / "seasonstats"
                    )

                    if not seasonstats_path.exists():
                        continue

                    for file in seasonstats_path.glob("*.json"):
                        data = load_json(file)

                        stats = data.get("contestant", {}).get("stat", [])
                        for s in stats:
                            if s.get("name") == stat_name:
                                try:
                                    values.append(float(s["value"]))
                                except:
                                    pass

                if values:
                    records.append({
                        "Liga": liga_name,
                        stat_name: sum(values) / len(values)
                    })

    return pd.DataFrame(records)

#===================================================
# Funcion para analizar la correlación entre remates y goles por liga en los ultimos 5 season
#===================================================
def compute_scatter_by_league(base_path, n_seasons=5):
    records = []

    for continente in get_continentes():
        for pais in get_paises(continente):
            for liga_name in get_competiciones(continente, pais):

                temporadas = get_temporadas(continente, pais, liga_name)
                temporadas = sorted(temporadas)[-n_seasons:]

                shots_values = []
                goals_values = []

                for temporada in temporadas:
                    seasonstats_path = (
                        base_path
                        / continente
                        / pais
                        / liga_name
                        / temporada
                        / "seasonstats"
                    )

                    if not seasonstats_path.exists():
                        continue

                    for file in seasonstats_path.glob("*.json"):
                        data = load_json(file)
                        stats = data.get("contestant", {}).get("stat", [])

                        for s in stats:
                            if s.get("name") == "Shots On Target ( inc goals )":
                                shots_values.append(float(s["value"]))
                            if s.get("name") == "Goals":
                                goals_values.append(float(s["value"]))

                if shots_values and goals_values:
                    records.append({
                        "Liga": liga_name,
                        "Remates al arco": sum(shots_values) / len(shots_values),
                        "Goles": sum(goals_values) / len(goals_values)
                    })

    return pd.DataFrame(records)

#===================================================
# Funcion para analizar la efectividad de pase por liga en los ultimos 5 season
#===================================================
def compute_pass_effectiveness_by_league(base_path, n_seasons=5):
    records = []

    for continente in get_continentes():
        for pais in get_paises(continente):
            for liga_name in get_competiciones(continente, pais):

                temporadas = get_temporadas(continente, pais, liga_name)
                temporadas = sorted(temporadas)[-n_seasons:]

                eff_values = []

                for temporada in temporadas:
                    seasonstats_path = (
                        base_path
                        / continente
                        / pais
                        / liga_name
                        / temporada
                        / "seasonstats"
                    )

                    if not seasonstats_path.exists():
                        continue

                    for file in seasonstats_path.glob("*.json"):
                        data = load_json(file)
                        stats = data.get("contestant", {}).get("stat", [])

                        successful = None
                        unsuccessful = None

                        for s in stats:
                            name = s.get("name", "").lower()

                            if "successful" in name and "pass" in name:
                                successful = float(s["value"])

                            if "unsuccessful" in name and "pass" in name:
                                unsuccessful = float(s["value"])

                        if (
                            successful is not None
                            and unsuccessful is not None
                            and (successful + unsuccessful) > 0
                        ):
                            eff_values.append(
                                successful / (successful + unsuccessful) * 100
                            )

                if eff_values:
                    records.append({
                        "Liga": liga_name,
                        "Efectividad de pase (%)": sum(eff_values) / len(eff_values)
                    })

    return pd.DataFrame(records)

#===================================================
# GRÁFICOS: POSESIÓN Y REMATES
#===================================================
st.subheader("📊 Posesión y remates")

view_mode = st.radio(
    "Vista",
    ["Por equipo", "Por liga"],
    horizontal=True
)

# -------------------------
# CONFIG SEGÚN MODO
# -------------------------
if view_mode == "Por equipo":
    # ---- Posesión (ya calculada antes)
    df_pos_chart = df_possession.copy()
    df_shots_chart = df_shots.copy()

    x_col = "Equipo"
    color_col = None

    y_pos_col = "Posesión media (%)"
    y_shots_col = "Total Shots"

else:  # Por liga
    df_pos_chart = compute_stat_by_league(
        BASE_PATH,
        stat_name="Possession Percentage",
        n_seasons=5
    )

    df_shots_chart = compute_stat_by_league(
        BASE_PATH,
        stat_name="Total Shots",
        n_seasons=5
    )

    for df in [df_pos_chart, df_shots_chart]:
        df["highlight"] = df["Liga"].apply(
            lambda x: "Seleccionada" if x == liga else "Otras"
        )

    x_col = "Liga"
    color_col = "highlight"

    y_pos_col = "Possession Percentage"
    y_shots_col = "Total Shots"


# -------------------------
# LAYOUT
# -------------------------
col_g1, col_g2 = st.columns(2)

BAR_COLOR = "#F7FA63" 
# -------------------------
# POSESIÓN
# -------------------------
with col_g1:
    df_pos_sorted = df_pos_chart.sort_values(
        by=y_pos_col,
        ascending=False
    )

    fig_pos = px.bar(
        df_pos_sorted,
        x=y_pos_col,
        y=x_col,
        orientation="h",
        text_auto=".1f"
    )

    fig_pos.update_traces(marker_color=BAR_COLOR)

    fig_pos.update_layout(
        title="Posesión media",
        xaxis_title="%",
        yaxis_title="",
        xaxis_range=[0, 100],
        showlegend=False
    )

    st.plotly_chart(fig_pos, use_container_width=True)

# -------------------------
# REMATES
# -------------------------
with col_g2:
    df_shots_sorted = df_shots_chart.sort_values(
        by=y_shots_col,
        ascending=False
    )

    fig_shots = px.bar(
        df_shots_sorted,
        x=y_shots_col,
        y=x_col,
        orientation="h",
        text_auto=".1f"
    )

    fig_shots.update_traces(marker_color=BAR_COLOR)

    fig_shots.update_layout(
        title="Remates totales (media)",
        xaxis_title="Remates",
        yaxis_title="",
        showlegend=False
    )

    st.plotly_chart(fig_shots, use_container_width=True)

#--------------------------------------------------
# GRÁFICO: REMATES AL ARCO VS GOLES
#--------------------------------------------------

st.divider()
st.subheader("🎯 Remates al arco vs Goles")

if view_mode == "Por equipo":
    df_scatter = df_scatter_team.copy()
    x_col = "Remates al arco"
    y_col = "Goles"
    label_col = "Equipo"

else:  # Por liga
    df_scatter = compute_scatter_by_league(BASE_PATH, n_seasons=5)
    x_col = "Remates al arco"
    y_col = "Goles"
    label_col = "Liga"

fig_scatter = px.scatter(
    df_scatter,
    x=x_col,
    y=y_col,
    text=label_col
)

fig_scatter.update_traces(
    marker=dict(
        size=12,
        color="#f5c842",
        opacity=0.85
    ),
    textposition="top center"
)

fig_scatter.update_layout(
    xaxis_title="Remates al arco",
    yaxis_title="Goles",
    showlegend=False
)

st.plotly_chart(fig_scatter, use_container_width=True)

#--------------------------------------------------
# GRÁFICO: EFECTIVIDAD DE PASE
#--------------------------------------------------
if view_mode == "Por equipo":
    df_pass_chart = df_pass_eff_team.copy()
    x_col = "Equipo"
else:
    df_pass_chart = compute_pass_effectiveness_by_league(BASE_PATH, n_seasons=5)
    x_col = "Liga"

st.divider()
st.subheader("🎯 Efectividad de pase")

df_pass_eff_team = df_pass_eff_team.sort_values(
    "Efectividad de pase (%)",
    ascending=False
)

fig_pass_eff = px.bar(
    df_pass_eff_team,
    x="Equipo",
    y="Efectividad de pase (%)",
    text_auto=".1f"
)

fig_pass_eff.update_layout(
    title="Efectividad de pase (%)",
    yaxis_title="%",
    xaxis_title="Equipo",
    yaxis_range=[0, 100],
    showlegend=False
)

fig_pass_eff.update_traces(
    marker_color="#f5c842"
)

st.plotly_chart(fig_pass_eff, use_container_width=True)


#==================================================
# data frame SERIE TEMPORAL POR JORNADA
#==================================================

path_matches = (
    BASE_PATH
    / continente
    / pais
    / liga
    / temporada
    / "matches"
)

rows = extract_duel_timeseries(path_matches)
df_duels = pd.DataFrame(rows)

if df_duels.empty:
    st.warning("No hay datos de duelos disponibles")
    st.stop()

metric = st.selectbox(
    "Métrica",
    [
        "Duelos",
        "Duelos ganados",
        "Duelos aéreos",
        "Duelos aéreos ganados",
        "Efectividad duelos (%)"
    ]
)

fig = px.line(
    df_duels.sort_values("Jornada"),
    x="Jornada",
    y=metric,
    color="Equipo",
    markers=True
)

fig.update_layout(
    title=f"{metric} por jornada",
    xaxis_title="Jornada",
    yaxis_title=metric,
    legend_title="Equipo"
)

st.plotly_chart(fig, use_container_width=True)
# --------------------------------------------------
# MENSAJE INFORMATIVO
# --------------------------------------------------
if len(df_metrics) < 5:
    st.info(
        f"ℹ️ Esta liga tiene actualmente {len(df_metrics)} temporadas cargadas. "
        "La vista está preparada para mostrar hasta las últimas 5 automáticamente."
    )
