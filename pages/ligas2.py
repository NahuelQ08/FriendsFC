import streamlit as st
import pandas as pd
import altair as alt
import base64
import os

def load_logos_as_base64(folder="assets/Escudos"):
    logos = {}

    for file in os.listdir(folder):
        if file.endswith(".png") or file.endswith(".jpg"):
            team_name = os.path.splitext(file)[0]
            with open(os.path.join(folder, file), "rb") as f:
                b64 = base64.b64encode(f.read()).decode()
                logos[team_name] = f"data:image/png;base64,{b64}"

    return logos

logos = load_logos_as_base64()

st.set_page_config(page_title="Ligas", page_icon="🏆", layout="wide", initial_sidebar_state="collapsed")

st.title("🏆 Ligas")


df = pd.read_parquet("LigaArgentina2024.parquet")

# st.dataframe(df)

tab1, tab2, tab3 = st.tabs(["Estadísticas", "Equipos", "Historial"])

with tab1:
    st.subheader("⚙️ Equipos por Liga")
    st.write("Tabla o filtros de equipos participantes.")

# with tab1:
#     st.subheader("📊 Estadísticas de Liga")

#     # =======================
#     # FILTROS EN LA MISMA FILA
#     # =======================
#     col1, col2, col3 = st.columns(3)

#     with col1:
#         ligas = sorted(df["competition"].dropna().unique())
#         liga_sel = st.selectbox("🏆 Competición", ligas)

#     with col2:
#         años = sorted(df[df["competition"] == liga_sel]["season"].dropna().unique())
#         año_sel = st.selectbox("📅 Año", años)

#     with col3:
#         fases = sorted(
#             df[
#                 (df["competition"] == liga_sel) &
#                 (df["season"] == año_sel)
#             ]["phase"].dropna().unique()
#         )
#         fase_sel = st.selectbox("📄 Fase", fases)

#     # =======================
#     # FILTRO PRINCIPAL
#     # =======================
#     df_filt = df[
#         (df["competition"] == liga_sel) &
#         (df["season"] == año_sel) &
#         (df["phase"] == fase_sel)
#     ]

#     st.markdown("---")

#     # =======================
#     # MÉTRICAS SUPERIORES
#     # =======================
#     col1, col2 = st.columns(2)

#     goles_totales = df_filt["Goal"].sum() if "Goal" in df_filt else 0
#     pj_total = df_filt["PJ"].sum() if "PJ" in df_filt else 1
#     prom_goles_por_partido = goles_totales / pj_total

#     col1.metric("⚽ Goles por partido", round(prom_goles_por_partido, 2))

#     if "points" in df_filt.columns:
#         col2.metric("📈 Puntos por partido", round(df_filt["points"].sum() / df_filt["PJ"].sum(), 2))
#     else:
#         col2.metric("📈 Puntos por partido", "N/D")

#     st.markdown("---")

#     # =======================
#     # GRÁFICOS SUPERIORES
#     # DUEL0S PROMEDIO POR EQUIPO
#     # =======================
#     if "Aerial" in df_filt.columns:
#         df_duelos = df_filt.groupby("team")["Aerial"].mean().reset_index()
#         df_duelos = df_duelos.sort_values("Aerial", ascending=False)

#         duelos_chart = (
#             alt.Chart(df_duelos)
#             .mark_line(point=True)
#             .encode(
#                 x=alt.X("team:N", sort="-y", title="Equipo"),
#                 y=alt.Y("Aerial:Q", title="Promedio de Duelos Ganados"),
#                 tooltip=["team", "Aerial"]
#             )
#             .properties(height=350)
#         )
#     else:
#         duelos_chart = alt.Chart(pd.DataFrame({"team": [], "Aerial": []}))

#     # =======================
#     # REMATES POR EQUIPO
#     # =======================
#     if "Miss" in df_filt.columns:
#         df_shots = df_filt.groupby("team")["Miss"].sum().reset_index()
#         df_shots = df_shots.sort_values("Miss", ascending=False)

#         shots_chart = (
#             alt.Chart(df_shots)
#             .mark_bar()
#             .encode(
#                 x=alt.X("team:N", sort="-y", title="Equipo"),
#                 y=alt.Y("Miss:Q", title="Remates"),
#                 tooltip=["team", "Miss"]
#             )
#             .properties(height=350)
#         )
#     else:
#         shots_chart = alt.Chart(pd.DataFrame({"team": [], "Miss": []}))

#     col_a, col_b = st.columns(2)
#     with col_a:
#         st.altair_chart(duelos_chart, use_container_width=True)
#     with col_b:
#         st.altair_chart(shots_chart, use_container_width=True)

#     st.markdown("---")

#     # =======================
#     # SCATTER PLOT xG vs GOLES
#     # =======================

#     # Encontrar la columna de xG automáticamente
#     xg_column = None
#     for c in df_filt.columns:
#         if "xg" in c.lower() or "expected" in c.lower():
#             xg_column = c
#             break

#     if xg_column is None:
#         st.error("⚠ No encontré columna de xG en el parquet.")
#     else:
#         df_xg = df_filt.groupby("team")[[xg_column, "Goal"]].sum().reset_index()

#         scatter = (
#             alt.Chart(df_xg)
#             .mark_circle(size=140)
#             .encode(
#                 x=alt.X(f"{xg_column}:Q", title="xG (Esperados)"),
#                 y=alt.Y("Goal:Q", title="Goles"),
#                 color="team:N",
#                 tooltip=["team", xg_column, "Goal"]
#             )
#             .properties(height=400)
#         )

#         st.subheader("🎯 xG vs Goles (por equipo)")
#         st.altair_chart(scatter, use_container_width=True)

#     st.markdown("---")
#     st.subheader("🥅 Remates al arco por equipo (Scatter Plot)")

#     # ===============================
#     # SCATTER SHOTS-SAVED vs GOALS (CON ESCUDOS)
#     # ===============================

#     st.subheader("📌 Remates atajados vs Goles (con escudos)")

#     if "AttemptSaved" not in df_filt.columns:
#         st.error("⚠ No encontré 'AttemptSaved' en el parquet. Asegurate de que el ETL esté sumando typeId 15.")

#     elif "Goal" not in df_filt.columns:
#         st.error("⚠ No encontré 'Goal' en el parquet. Asegurate de que el ETL incluye typeId 16.")

#     else:
#         # Agrupamos remates al arco (atajados) y goles por equipo
#         df_scatter = (
#             df_filt.groupby("team")[["AttemptSaved", "Goal"]]
#             .sum()
#             .reset_index()
#             .rename(columns={
#                 "AttemptSaved": "ShotsSaved",
#                 "Goal": "Goals"
#             })
#         )

#         # Agregar logos
#         df_scatter["logo"] = df_scatter["team"].map(logos)

#         # Avisar si falta algún escudo
#         if df_scatter["logo"].isna().any():
#             equipos_sin_logo = df_scatter[df_scatter["logo"].isna()]["team"].tolist()
#             st.warning(f"⚠ Faltan logos para: {equipos_sin_logo}")

#         # Crear scatter con imágenes
#         scatter_shots_vs_goals = (
#             alt.Chart(df_scatter)
#             .mark_image(width=45, height=45)
#             .encode(
#                 x=alt.X("ShotsSaved:Q", title="Remates atajados"),
#                 y=alt.Y("Goals:Q", title="Goles convertidos"),
#                 url="logo:N",
#                 tooltip=["team", "ShotsSaved", "Goals"]
#             )
#             .properties(width="container", height=450)
#         )

#         st.altair_chart(scatter_shots_vs_goals, use_container_width=True)

#     st.markdown("---")
#     st.subheader("🎯 Efectividad de pases por equipo")

#     # =======================
#     # EFECTIVIDAD DE PASES
#     # =======================

#     # Validaciones
#     if "Pass" not in df_filt.columns:
#         st.error("⚠ La columna 'Pass' no está en el parquet. Revisa el ETL.")
#     elif "Pass_success" not in df_filt.columns:
#         st.error("⚠ Falta 'Pass_success'. Asegurate de haber actualizado el ETL.")
#     else:

#         # -------------------------
#         # Preparar DataFrame grafico de efectividad de pase
#         # -------------------------
#         df_pass = (
#             df_filt.groupby("team")[["Pass", "Pass_success"]]
#             .sum()
#             .reset_index()
#             .rename(columns={
#                 "Pass": "PasesTotales",
#                 "Pass_success": "PasesAcertados"
#             })
#         )

#         # Evitar división por cero
#         df_pass["Efectividad"] = df_pass.apply(
#             lambda row: (row["PasesAcertados"] / row["PasesTotales"]) * 100
#             if row["PasesTotales"] > 0 else 0,
#             axis=1
#         )

#         # Ordenar descendente por efectividad
#         df_pass = df_pass.sort_values("Efectividad", ascending=False)

#         # -------------------------
#         # Gráfico
#         # -------------------------
#         chart_pass = (
#             alt.Chart(df_pass)
#             .mark_bar(size=22)
#             .encode(
#                 x=alt.X("Efectividad:Q",
#                         title="Efectividad de pase (%)",
#                         scale=alt.Scale(domain=[0, 100])),
#                 y=alt.Y("team:N", sort="-x", title="Equipo"),
#                 tooltip=[
#                     alt.Tooltip("team", title="Equipo"),
#                     alt.Tooltip("PasesTotales", title="Total de pases"),
#                     alt.Tooltip("PasesAcertados", title="Pases acertados"),
#                     alt.Tooltip("Efectividad", format=".2f", title="Efectividad (%)")
#                 ],
#                 color=alt.Color("Efectividad:Q", scale=alt.Scale(scheme="greens"))
#             )
#             .properties(height=550)
#         )

#         st.altair_chart(chart_pass, use_container_width=True)

with tab2:
    st.subheader("⚙️ Equipos por Liga")
    st.write("Tabla o filtros de equipos participantes.")

with tab3:
    st.subheader("📅 Historial de Ligas")
    st.write("Comparativas por temporadas.")
