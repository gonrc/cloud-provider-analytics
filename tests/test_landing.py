import csv

from cpa.landing import build_manifest, extract, verify


def _write_manifest(landing, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["path", "bytes", "sha256"])
        w.writeheader()
        w.writerows(build_manifest(landing))


def test_landing_detecta_archivo_modificado(tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "a.csv").write_text("x,y\n1,2\n")
    manifest = tmp_path / "m.csv"
    _write_manifest(landing, manifest)
    assert verify(landing, manifest) == []

    (landing / "a.csv").write_text("x,y\n1,3\n")
    assert verify(landing, manifest) == ["modificado: a.csv"]


def test_extract_no_pisa_lo_existente(tmp_path):
    import zipfile

    zpath = tmp_path / "ds.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("README.txt", "fuera de landing")
        zf.writestr("datalake/landing/a.csv", "x\n1\n")
    landing = tmp_path / "landing"
    assert extract(zpath, landing) == 1
    assert extract(zpath, landing) == 0
    assert not (landing / "README.txt").exists()


def test_metadatos_del_sistema_no_cuentan(tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "a.csv").write_text("x,y\n1,2\n")
    manifest = tmp_path / "m.csv"
    _write_manifest(landing, manifest)

    (landing / ".DS_Store").write_bytes(b"\x00finder")
    assert verify(landing, manifest) == []

    (landing / "b.csv").write_text("x\n1\n")
    assert verify(landing, manifest) == ["sobra: b.csv"]
