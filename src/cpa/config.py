"""Carga de config/settings.toml.

La raíz del repo se toma de CPA_ROOT si está definida (útil en Colab) y si no,
de la ubicación de este archivo. CPA_SETTINGS permite apuntar a otra copia
del archivo de configuración sin editar la versionada.
"""
import os
import tomllib
from pathlib import Path

ROOT = Path(os.environ.get("CPA_ROOT", Path(__file__).resolve().parents[2]))


def load_settings(path: str | None = None) -> dict:
    path = Path(path or os.environ.get("CPA_SETTINGS", ROOT / "config" / "settings.toml"))
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    cfg["paths"] = {
        name: value if os.path.isabs(value) else str(ROOT / value)
        for name, value in cfg["paths"].items()
    }
    return cfg
