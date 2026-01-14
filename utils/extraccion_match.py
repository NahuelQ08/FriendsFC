# ==================================================
# utils/extraccion_match.py
# Funciones para extracción de datos de partidos y jugadores
# ==================================================

import pandas as pd
from datetime import datetime
from utils.loader import load_json


# ==================================================
# EXTRACCIÓN DE ESTADÍSTICAS POR PARTIDO
# ==================================================

def extract_player_matchstats(matchstats_dir, player_id):
    """
    Extrae las estadísticas de cada partido del jugador.
    Busca en liveData.lineUp[].player[] los datos del jugador.
    
    Parameters:
    -----------
    matchstats_dir : Path
        Ruta a la carpeta de matchstats
    player_id : str
        ID del jugador
    
    Returns:
    --------
    pd.DataFrame
        DataFrame con estadísticas por partido
    """
    rows = []
    
    for file in matchstats_dir.glob("*.json"):
        data = load_json(file)
        match_info = data.get("matchInfo", {})
        live_data = data.get("liveData", {})
        line_ups = live_data.get("lineUp", [])
        
        # Buscar el jugador en todos los line-ups
        player_found = False
        
        for lineup in line_ups:
            players = lineup.get("player", [])
            
            for p in players:
                if p.get("playerId") != player_id:
                    continue
                
                player_found = True
                
                # Extraer stats
                stats = {}
                for s in p.get("stat", []):
                    try:
                        stats[s["type"]] = int(s["value"])
                    except:
                        stats[s["type"]] = 0
                
                rows.append({
                    "match_id": match_info.get("id"),
                    "date": match_info.get("localDate"),
                    "match": match_info.get("description"),
                    "minutes": stats.get("minsPlayed", 0),
                    "goals": stats.get("goals", 0),
                    "assists": stats.get("goalAssist", 0),
                    "yellow": stats.get("yellowCard", 0),
                    "red": stats.get("redCard", 0),
                    "started": stats.get("gameStarted", 0),
                })
                
                break
            
            if player_found:
                break
    
    df = pd.DataFrame(rows)
    
    # Ordenar por fecha (más recientes primero)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date", ascending=False).reset_index(drop=True)
    
    return df


# ==================================================
# EXTRACCIÓN DE PARTIDOS DEL JUGADOR
# ==================================================

def extract_player_matches(match_dir, player_id):
    """
    Extrae los partidos en los que participó el jugador.
    
    Parameters:
    -----------
    match_dir : Path
        Ruta a la carpeta de matches
    player_id : str
        ID del jugador
    
    Returns:
    --------
    pd.DataFrame
        DataFrame con información de partidos
    """
    matches = []

    for file in match_dir.glob("*.json"):
        data = load_json(file)

        match_info = data.get("matchInfo", {})
        live_data = data.get("liveData", {})
        events = live_data.get("event", [])

        # ✅ DETECCIÓN: el jugador aparece en algún evento
        played = any(
            ev.get("playerId") == player_id
            for ev in events
        )

        if not played:
            continue

        # INFORMACIÓN DEL PARTIDO
        match_id = match_info.get("id")
        
        date_raw = match_info.get("localDate")
        try:
            match_date = datetime.strptime(date_raw, "%Y-%m-%d").date()
        except:
            match_date = None

        contestants = match_info.get("contestant", [])
        home = next(
            (c["name"] for c in contestants if c.get("position") == "home"), "—"
        )
        away = next(
            (c["name"] for c in contestants if c.get("position") == "away"), "—"
        )

        scores = live_data.get("matchDetails", {}).get("scores", {})
        ft = scores.get("ft", {})
        score = f"{ft.get('home', 0)}–{ft.get('away', 0)}"

        matches.append({
            "match_id": match_id,
            "date": match_date,
            "home": home,
            "away": away,
            "score": score,
            "label": f"{match_date} • {home} {score} {away}",
            "file": file.name
        })

    return pd.DataFrame(matches)