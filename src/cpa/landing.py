"""Preparación y control de la zona Landing.

Landing es inmutable: se extrae del zip de la cátedra, se deja en solo lectura
y se compara contra un manifiesto con el SHA-256 de cada archivo. Si un archivo
cambia, el pipeline no debería seguir.

Uso:
    python -m cpa.landing              extrae (si hace falta) y verifica
    python -m cpa.landing --manifest   regenera el manifiesto (solo la primera vez)
"""
import argparse
import csv
import hashlib
import os
import stat
import sys
import zipfile
from pathlib import Path

from cpa.config import load_settings

PREFIX = "datalake/landing/"


def extract(zip_path: str, landing_dir: str) -> int:
    """Extrae del zip solo lo que está bajo datalake/landing/. Devuelve cuántos archivos escribió."""
    landing = Path(landing_dir)
    written = 0
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            if member.is_dir() or not member.filename.startswith(PREFIX):
                continue
            target = landing / member.filename[len(PREFIX):]
            if target.exists():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(zf.read(member))
            os.chmod(target, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            written += 1
    return written


def build_manifest(landing_dir: str) -> list[dict]:
    landing = Path(landing_dir)
    rows = []
    for path in sorted(p for p in landing.rglob("*") if p.is_file()):
        rows.append({
            "path": path.relative_to(landing).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
    return rows


def verify(landing_dir: str, manifest_path: str) -> list[str]:
    """Devuelve la lista de diferencias contra el manifiesto. Vacía = Landing intacta."""
    with open(manifest_path, newline="") as f:
        expected = {r["path"]: r["sha256"] for r in csv.DictReader(f)}
    actual = {r["path"]: r["sha256"] for r in build_manifest(landing_dir)}
    problems = [f"falta: {p}" for p in expected.keys() - actual.keys()]
    problems += [f"sobra: {p}" for p in actual.keys() - expected.keys()]
    problems += [f"modificado: {p}" for p in expected.keys() & actual.keys() if expected[p] != actual[p]]
    return sorted(problems)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", action="store_true", help="regenerar data/landing_manifest.csv")
    args = parser.parse_args()
    paths = load_settings()["paths"]

    n = extract(paths["raw_zip"], paths["landing"])
    print(f"Landing: {n} archivos extraídos en {paths['landing']}")

    if args.manifest:
        rows = build_manifest(paths["landing"])
        with open(paths["manifest"], "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["path", "bytes", "sha256"])
            w.writeheader()
            w.writerows(rows)
        print(f"Manifiesto escrito: {len(rows)} archivos")
        return 0

    problems = verify(paths["landing"], paths["manifest"])
    if problems:
        print("Landing NO coincide con el manifiesto:", *problems, sep="\n  ")
        return 1
    print("Landing coincide con el manifiesto")
    return 0


if __name__ == "__main__":
    sys.exit(main())
