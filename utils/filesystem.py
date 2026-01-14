from pathlib import Path

BASE_PATH = Path("scrap/data")

def get_continentes():
    if not BASE_PATH.exists():
        return []
    return sorted(
        p.name for p in BASE_PATH.iterdir() if p.is_dir()
    )

def get_paises(continente):
    if not continente:
        return []

    path = BASE_PATH / continente
    if not path.exists():
        return []

    return sorted(
        p.name for p in path.iterdir() if p.is_dir()
    )

def get_competiciones(continente, pais):
    if not continente or not pais:
        return []

    path = BASE_PATH / continente / pais
    if not path.exists():
        return []

    return sorted(
        p.name for p in path.iterdir() if p.is_dir()
    )

def get_temporadas(continente, pais, competicion):
    if not continente or not pais or not competicion:
        return []

    path = BASE_PATH / continente / pais / competicion
    if not path.exists():
        return []

    return sorted(
        p.name for p in path.iterdir() if p.is_dir()
    )
