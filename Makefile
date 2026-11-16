# Requiere Java 17 (o 21) en JAVA_HOME. En macOS con Homebrew:
#   export JAVA_HOME=$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home
# Serving: Cassandra escuchando en 127.0.0.1:9042 (ver README, sección Serving).
PY ?= .venv/bin/python
CASSANDRA_HOME ?= $(HOME)/opt/apache-cassandra-5.0.9
CQLSH = $(PY) $(CASSANDRA_HOME)/bin/cqlsh.py
export PYTHONPATH := src

.PHONY: setup landing explore bronze silver stream gold serve pipeline pipeline-sin-serving \
        experimentos idempotencia cqlsh test clean reset pdf

setup:
	uv venv --python 3.11 .venv
	uv pip install --python $(PY) -r requirements-dev.txt

landing:
	$(PY) -m cpa.landing

explore: landing
	$(PY) -m nbconvert --to notebook --execute --inplace notebooks/01_exploracion_landing.ipynb

# Pasos sueltos, en el orden del pipeline.
bronze: landing
	$(PY) -m cpa.bronze

silver:
	$(PY) -m cpa.silver

stream:
	$(PY) -m cpa.streaming

gold:
	$(PY) -m cpa.gold

serve:
	$(PY) -m cpa.serving --consultas

# Todo junto: Landing, Bronze batch, Silver de maestros, streaming con capa speed,
# capa batch, Gold, anomalías y Cassandra con las dos consultas.
pipeline:
	$(PY) -m cpa.pipeline

# Igual, sin Cassandra (por ejemplo en Colab).
pipeline-sin-serving:
	$(PY) -m cpa.pipeline --sin-serving

experimentos:
	$(PY) scripts/experimentos_entrega2.py

# Re-ejecuta el pipeline dos veces (mismo checkpoint y checkpoint borrado) y compara conteos.
idempotencia:
	$(PY) scripts/idempotencia.py

cqlsh:
	$(CQLSH)

test:
	$(PY) -m pytest -q tests

# Borra lo derivado del Data Lake (Bronze, Silver, Gold, quarantine, checkpoints).
# Landing queda: es la fuente y se verifica contra el manifiesto.
reset:
	rm -rf datalake/bronze datalake/silver datalake/gold datalake/quarantine datalake/_checkpoints datalake/_experimentos

# Borra todo lo derivado, Landing incluida. Se vuelve a extraer del zip con `make landing`.
clean:
	chmod -R u+w datalake 2>/dev/null || true
	rm -rf datalake spark-warehouse metastore_db derby.log

# Regenera el PDF de un documento después de editar el Markdown. Necesita Chrome y conexión.
DOC ?= docs/diseno_v1.md
pdf:
	$(PY) scripts/build_pdf.py $(DOC)
