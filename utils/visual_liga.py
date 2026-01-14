import json
from pathlib import Path
from collections import defaultdict
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


def get_league_nationalities(squads_path: Path):
    """
    Extrae nacionalidades de jugadores desde la carpeta squads/
    """

    nationality_counter = Counter()

    for file in squads_path.glob("*.json"):
        with open(file, encoding="utf-8") as f:
            data = json.load(f)

        squads = data.get("squad", [])

        for squad in squads:
            players = squad.get("person", [])

            for p in players:
                if p.get("type") != "player":
                    continue

                nationality = p.get("nationality")

                if nationality:
                    nationality_counter[nationality] += 1

    rows = [
        {
            "Nacionalidad": nat,
            "Jugadores": count
        }
        for nat, count in nationality_counter.items()
    ]

    return rows

def get_league_nationalities_last_n_seasons(
    base_path: Path,
    continente: str,
    pais: str,
    liga: str,
    temporadas: list[str]
):
    """
    Devuelve el total de jugadores por nacionalidad
    considerando varias temporadas
    """

    nationality_counter = Counter()

    for temporada in temporadas:
        squads_path = (
            base_path
            / continente
            / pais
            / liga
            / temporada
            / "squads"
        )

        if not squads_path.exists():
            continue

        for file in squads_path.glob("*.json"):
            with open(file, encoding="utf-8") as f:
                data = json.load(f)

            for squad in data.get("squad", []):
                for p in squad.get("person", []):
                    if p.get("type") != "player":
                        continue

                    nationality = p.get("nationality")
                    if nationality:
                        nationality_counter[nationality] += 1

    return [
        {"Nacionalidad": nat, "Jugadores": count}
        for nat, count in nationality_counter.items()
    ]

def get_nationalities_by_season(
    base_path: Path,
    continente: str,
    pais: str,
    liga: str,
    temporadas: list[str]
):
    """
    Devuelve cantidad de nacionalidades distintas por temporada
    """

    rows = []

    for temporada in temporadas:
        squads_path = (
            base_path
            / continente
            / pais
            / liga
            / temporada
            / "squads"
        )

        if not squads_path.exists():
            continue

        nationalities = set()

        for file in squads_path.glob("*.json"):
            with open(file, encoding="utf-8") as f:
                data = json.load(f)

            for squad in data.get("squad", []):
                for p in squad.get("person", []):
                    if p.get("type") == "player" and p.get("nationality"):
                        nationalities.add(p["nationality"])

        rows.append({
            "Temporada": temporada,
            "Nacionalidades": len(nationalities)
        })

    return rows