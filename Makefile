# Requiere Java 17 (o 11) en JAVA_HOME. En macOS con Homebrew:
#   export JAVA_HOME=$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home
PY ?= .venv/bin/python
export PYTHONPATH := src

.PHONY: setup landing explore test clean pdf

setup:
	uv venv --python 3.11 .venv
	uv pip install --python $(PY) -r requirements-dev.txt

landing:
	$(PY) -m cpa.landing

explore: landing
	$(PY) -m nbconvert --to notebook --execute --inplace notebooks/01_exploracion_landing.ipynb

test:
	$(PY) -m pytest -q tests

# Borra todo lo derivado. Landing se vuelve a extraer del zip con `make landing`.
clean:
	chmod -R u+w datalake 2>/dev/null || true
	rm -rf datalake spark-warehouse metastore_db derby.log

# Regenera docs/diseno_v1.pdf después de editar el Markdown. Necesita Chrome y conexión.
pdf:
	$(PY) scripts/build_pdf.py docs/diseno_v1.md
