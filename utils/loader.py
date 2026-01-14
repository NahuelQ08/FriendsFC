import json
from pathlib import Path

def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def list_entities(path_temporada: Path):
    """
    Devuelve qué hay disponible en la temporada
    """
    return sorted(p.name for p in path_temporada.iterdir())
