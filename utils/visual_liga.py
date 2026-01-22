import json
from pathlib import Path
from collections import defaultdict
import pandas as pd
from collections import Counter


# ----------------------------------------------------------------------
# Extracción de duelos por equipo y jornada
# ----------------------------------------------------------------------
DUEL_METRICS = {
    44: "Duelo aéreo",
    45: "Duelo"
}


def extract_duel_timeseries(matches_path: Path):
    """
    Extrae duelos (44, 45) por equipo y jornada desde la carpeta matches/
    """

    rows = []

    for file in matches_path.glob("*.json"):
        with open(file, encoding="utf-8") as f:
            data = json.load(f)

        match_info = data.get("matchInfo", {})
        live_data = data.get("liveData", {})
        events = live_data.get("event", [])

        week = int(match_info.get("week", 0))
        date = match_info.get("localDate")
        match_id = match_info.get("id")

        # Map contestantId → nombre equipo
        team_map = {
            c["id"]: c["name"]
            for c in match_info.get("contestant", [])
        }

        # stats[week][team][metric]
        stats = defaultdict(lambda: defaultdict(lambda: {
            "duelos": 0,
            "duelos_ganados": 0,
            "duelos_aereos": 0,
            "duelos_aereos_ganados": 0
        }))

        for ev in events:
            type_id = ev.get("typeId")
            contestant_id = ev.get("contestantId")
            outcome = ev.get("outcome", 0)

            if type_id not in (44, 45):
                continue

            if contestant_id not in team_map:
                continue

            team = team_map[contestant_id]

            if type_id == 45:
                stats[week][team]["duelos"] += 1
                if outcome == 1:
                    stats[week][team]["duelos_ganados"] += 1

            if type_id == 44:
                stats[week][team]["duelos_aereos"] += 1
                if outcome == 1:
                    stats[week][team]["duelos_aereos_ganados"] += 1

        # Aplanar resultados
        for week, teams in stats.items():
            for team, s in teams.items():
                total = s["duelos"] + s["duelos_aereos"]
                ganados = s["duelos_ganados"] + s["duelos_aereos_ganados"]

                rows.append({
                    "Jornada": week,
                    "Fecha": date,
                    "MatchId": match_id,
                    "Equipo": team,
                    "Duelos": s["duelos"],
                    "Duelos ganados": s["duelos_ganados"],
                    "Duelos aéreos": s["duelos_aereos"],
                    "Duelos aéreos ganados": s["duelos_aereos_ganados"],
                    "Efectividad duelos (%)": round(
                        (ganados / total) * 100, 2
                    ) if total > 0 else 0
                })

    return rows


# ----------------------------------------------------------------------
# Extracción de nacionalidades desde squads/
# ----------------------------------------------------------------------


def get_league_nationalities_from_squads(squads_file: Path) -> pd.DataFrame:
    """
    Extrae nacionalidades de jugadores desde squads.json de una temporada.
    """

    if not squads_file.exists():
        return pd.DataFrame()

    with open(squads_file, encoding="utf-8") as f:
        data = json.load(f)

    nationality_counter = Counter()

    squads = data.get("squad", [])

    for squad in squads:
        players = squad.get("person", [])

        for p in players:
            if p.get("type") != "player":
                continue

            nationality = p.get("nationality")
            if nationality:
                nationality_counter[nationality] += 1

    df = pd.DataFrame(
        [
            {"Nacionalidad": nat, "Jugadores": count}
            for nat, count in nationality_counter.items()
        ]
    ).sort_values("Jugadores", ascending=False)

    return df
