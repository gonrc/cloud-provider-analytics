"""Esquemas explícitos de las fuentes tal como llegan a Landing.

No se usa inferSchema en ningún punto del pipeline. Dos criterios:

- Los campos que el dataset documenta como ambiguos se leen como texto y se
  castean después con fallback (ver casting.py). En los eventos, leer `value`
  como double hace que Spark marque como corruptos los 1.309 eventos donde
  llega entre comillas; ver notebooks/01_exploracion_landing.ipynb.
- Cada esquema lleva `_corrupt_record` para que las filas que no parsean vayan
  a quarantine en lugar de perderse en silencio.
"""
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CORRUPT = StructField("_corrupt_record", StringType())


def _s(*fields: tuple[str, object]) -> StructType:
    return StructType([StructField(name, dtype) for name, dtype in fields] + [CORRUPT])


CUSTOMERS_ORGS = _s(
    ("org_id", StringType()),
    ("org_name", StringType()),
    ("industry", StringType()),
    ("hq_region", StringType()),
    ("plan_tier", StringType()),
    ("is_enterprise", BooleanType()),
    ("signup_date", DateType()),
    ("sales_rep", StringType()),
    ("lifecycle_stage", StringType()),
    ("marketing_source", StringType()),
    ("nps_score", DoubleType()),
)

USERS = _s(
    ("user_id", StringType()),
    ("org_id", StringType()),
    ("email", StringType()),
    ("role", StringType()),
    ("active", BooleanType()),
    ("created_at", DateType()),
    ("last_login", DateType()),
)

RESOURCES = _s(
    ("resource_id", StringType()),
    ("org_id", StringType()),
    ("service", StringType()),
    ("region", StringType()),
    ("created_at", DateType()),
    ("state", StringType()),
    ("tags_json", StringType()),
)

SUPPORT_TICKETS = _s(
    ("ticket_id", StringType()),
    ("org_id", StringType()),
    ("category", StringType()),
    ("severity", StringType()),
    ("created_at", DateType()),
    ("resolved_at", DateType()),
    ("csat", DoubleType()),
    ("sla_breached", BooleanType()),
)

MARKETING_TOUCHES = _s(
    ("touch_id", StringType()),
    ("org_id", StringType()),
    ("campaign", StringType()),
    ("channel", StringType()),
    ("timestamp", DateType()),
    ("clicked", BooleanType()),
    ("converted", BooleanType()),
)

NPS_SURVEYS = _s(
    ("org_id", StringType()),
    ("survey_date", DateType()),
    ("nps_score", DoubleType()),
    ("comment", StringType()),
)

BILLING_MONTHLY = _s(
    ("invoice_id", StringType()),
    ("org_id", StringType()),
    ("month", DateType()),
    ("subtotal", DoubleType()),
    ("credits", DoubleType()),
    ("taxes", DoubleType()),
    ("currency", StringType()),
    ("exchange_rate_to_usd", DoubleType()),
)

# Unión de v1 y v2. En v1 no existen carbon_kg ni genai_tokens: quedan nulos.
USAGE_EVENTS = _s(
    ("event_id", StringType()),
    ("timestamp", TimestampType()),
    ("org_id", StringType()),
    ("resource_id", StringType()),
    ("service", StringType()),
    ("region", StringType()),
    ("metric", StringType()),
    ("value", StringType()),  # llega como número o como texto
    ("unit", StringType()),
    ("cost_usd_increment", DoubleType()),
    ("schema_version", IntegerType()),
    ("carbon_kg", DoubleType()),
    ("genai_tokens", LongType()),
)


# Registro de fuentes: ruta relativa a Landing, formato, esquema y clave natural.
SOURCES = {
    "customers_orgs": ("customers_orgs.csv", "csv", CUSTOMERS_ORGS, ["org_id"]),
    "users": ("users.csv", "csv", USERS, ["user_id"]),
    "resources": ("resources.csv", "csv", RESOURCES, ["resource_id"]),
    "support_tickets": ("support_tickets.csv", "csv", SUPPORT_TICKETS, ["ticket_id"]),
    "marketing_touches": ("marketing_touches.csv", "csv", MARKETING_TOUCHES, ["touch_id"]),
    "nps_surveys": ("nps_surveys.csv", "csv", NPS_SURVEYS, ["org_id", "survey_date"]),
    "billing_monthly": ("billing_monthly.csv", "csv", BILLING_MONTHLY, ["invoice_id"]),
    "usage_events": ("usage_events_stream", "json", USAGE_EVENTS, ["event_id"]),
}


def read_landing(spark, landing_dir: str, name: str):
    """Lectura batch de una fuente de Landing con su esquema explícito."""
    rel, fmt, schema, _ = SOURCES[name]
    reader = spark.read.schema(schema).option("mode", "PERMISSIVE").option(
        "columnNameOfCorruptRecord", "_corrupt_record"
    )
    path = f"{landing_dir}/{rel}"
    if fmt == "csv":
        # tags_json trae comillas escapadas duplicándolas ("[""env:prod""]"), como pide
        # RFC 4180. El escape por defecto de Spark es la barra invertida: sin esta
        # opción, 211 de las 400 filas de resources quedan como corruptas.
        return (
            reader.option("header", True)
            .option("escape", '"')
            .option("dateFormat", "yyyy-MM-dd")
            .csv(path)
        )
    return reader.json(f"{path}/*.jsonl")
