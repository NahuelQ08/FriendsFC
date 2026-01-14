# ==================================================
# utils/visualizaciones.py
# Funciones de visualización para análisis de jugadores
# ==================================================

import pandas as pd
import matplotlib.pyplot as plt
from mplsoccer import Pitch
from utils.loader import load_json

# ==================================================
# ESTILOS Y COLORES
# ==================================================

bg_color = '#15242e'
line_color = '#cfcfcf'
shot_color = '#f39c12'


# ==================================================
# EXTRACCIÓN DE DISPAROS (EVENTOS)
# ==================================================

def extract_player_shots(match_file_path, player_id):
    """
    Extrae todos los disparos del jugador de un partido.
    Incluye typeId, outcome, coordenadas y qualifiers.
    
    Parameters:
    -----------
    match_file_path : Path
        Ruta al archivo JSON del partido
    player_id : str
        ID del jugador
    
    Returns:
    --------
    pd.DataFrame
        DataFrame con disparos (typeId 13, 15, 16)
        - 13: Miss (Disparo errado)
        - 15: Attempt Saved (Disparo atajado)
        - 16: Goal (Gol)
    """
    data = load_json(match_file_path)
    live_data = data.get("liveData", {})
    events = live_data.get("event", [])
    
    shots = []
    SHOT_TYPE_IDS = [13, 15, 16]
    
    for event in events:
        type_id = event.get("typeId")
        
        if type_id not in SHOT_TYPE_IDS:
            continue
        
        if event.get("playerId") != player_id:
            continue
        
        shot_data = {
            "playerId": event.get("playerId"),
            "playerName": event.get("playerName"),
            "contestantId": event.get("contestantId"),
            "periodId": event.get("periodId"),
            "timeMin": event.get("timeMin"),
            "timeSec": event.get("timeSec"),
            "x": float(event.get("x", 0)),
            "y": float(event.get("y", 0)),
            "outcome": event.get("outcome"),
            "eventId": event.get("eventId"),
            "typeId": type_id,
            "qualifiers": event.get("qualifier", [])
        }
        
        shots.append(shot_data)
    
    return pd.DataFrame(shots)


def extract_all_season_shots(match_dir, player_id):
    """
    Extrae todos los disparos de la temporada.
    
    Parameters:
    -----------
    match_dir : Path
        Ruta a la carpeta de matches
    player_id : str
        ID del jugador
    
    Returns:
    --------
    pd.DataFrame
        DataFrame con todos los disparos de la temporada
    """
    all_shots = []
    
    for match_file in match_dir.glob("*.json"):
        df_match_shots = extract_player_shots(match_file, player_id)
        if not df_match_shots.empty:
            all_shots.append(df_match_shots)
    
    if all_shots:
        return pd.concat(all_shots, ignore_index=True)
    else:
        return pd.DataFrame()


# ==================================================
# VISUALIZACIÓN DE DISPAROS (EVENTOS)
# ==================================================

def plot_shots_events(df_shots, player_name, figsize=(8, 10)):
    """
    Visualiza los disparos como eventos en la cancha UEFA.
    
    Convierte coordenadas Opta (0-100) a UEFA (0-105 x 0-68)
    
    Parameters:
    -----------
    df_shots : pd.DataFrame
        DataFrame con datos de disparos
    player_name : str
        Nombre del jugador
    figsize : tuple
        Tamaño de la figura
    
    Markers:
    --------
    * (estrella) = Gol (typeId 16)
    + (más) = Miss (typeId 13)
    o (círculo) = Attempt Saved (typeId 15)
    """
    
    if df_shots.empty:
        return None
    
    fig, ax = plt.subplots(figsize=figsize, facecolor=bg_color)
    
    pitch = Pitch(
        pitch_type='uefa',
        corner_arcs=True,
        pitch_color=bg_color,
        line_color=line_color,
        line_zorder=2,
        linewidth=2
    )
    
    pitch.draw(ax=ax)
    ax.set_xlim(-2, 107)
    ax.set_ylim(-5, 73)
    
    # Contadores
    goals_count = 0
    misses_count = 0
    attempts_count = 0
    
    # Iterar y plotear cada disparo
    for idx, row in df_shots.iterrows():
        x_conv = row['x'] * 1.05
        y_conv = row['y'] * 0.68
        type_id = row['typeId']
        
        if type_id == 16:  # Gol
            ax.scatter(x_conv, y_conv, s=250, c='#2ecc71', lw=2.5, edgecolor='#2ecc71', marker='*', hatch='/////', zorder=3)
            goals_count += 1
        elif type_id == 13:  # Miss
            ax.scatter(x_conv, y_conv, s=250, color='#e74c3c', linewidths=3.5, marker='+',zorder=3
            )
            misses_count += 1
        elif type_id == 15:  # Attempt Saved
            ax.scatter(x_conv, y_conv, s=250, c='None', lw=2.5, edgecolor='#3498db', marker='o', hatch='/////', zorder=3)
            attempts_count += 1
    
    # Leyenda en la parte inferior
    legend_y = -3
    legend_x = 2
    
    ax.scatter(legend_x, legend_y, s=150, c='#2ecc71', lw=2.5, edgecolor='#2ecc71', marker='*', hatch='/////')
    ax.text(legend_x + 5, legend_y, f"Gol: {goals_count}", color=shot_color, ha='left', va='center', fontsize=11)
    
    ax.scatter(legend_x + 20, legend_y, s=150, c='None', lw=3.5, edgecolor='#e74c3c', marker='+', hatch='/////')
    ax.text(legend_x + 25, legend_y, f"Miss: {misses_count}", color=shot_color, ha='left', va='center', fontsize=11)
    
    ax.scatter(legend_x + 40, legend_y, s=150, c='None', lw=2.5, edgecolor='#3498db', marker='o', hatch='/////')
    ax.text(legend_x + 45, legend_y, f"Atajado: {attempts_count}", color=shot_color, ha='left', va='center', fontsize=11)
    
    # Título
    ax.set_title(f"{player_name} - Disparos", color=shot_color, fontsize=18, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    return fig