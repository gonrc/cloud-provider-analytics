-- Keyspace y tablas de serving (D-09). Modelado query-first: una tabla por consulta,
-- con la clave de partición en lo que filtra la consulta y las columnas de clustering
-- en el orden en que se recorren. Ninguna consulta usa ALLOW FILTERING.
-- Local con un solo nodo: SimpleStrategy y factor de replicación 1. En un clúster real
-- iría NetworkTopologyStrategy con factor 3.

CREATE KEYSPACE IF NOT EXISTS cloud_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Consulta 1: costos y requests diarios de una organización, por servicio, en un rango
-- de fechas. Partición = organización (80 particiones, unas 140 filas cada una);
-- clustering por fecha descendente para leer primero lo más reciente.
CREATE TABLE IF NOT EXISTS cloud_analytics.org_daily_usage_by_service (
  org_id            text,
  usage_date        date,
  service           text,
  daily_cost_usd    double,
  requests          double,
  cpu_hours         double,
  storage_gb_hours  double,
  genai_tokens      bigint,
  carbon_kg         double,
  events            int,
  late_events       int,
  plan_tier         text,
  industry          text,
  run_id            text,
  loaded_at         timestamp,
  PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- Consulta 2: top-N servicios por costo acumulado en los últimos 14 días de una
-- organización. Misma partición, pero clustering por servicio y después fecha, para
-- que Cassandra pueda sumar por servicio (GROUP BY sobre el prefijo de clustering)
-- leyendo un rango de fechas dentro de cada servicio.
CREATE TABLE IF NOT EXISTS cloud_analytics.org_service_daily_cost (
  org_id          text,
  service         text,
  usage_date      date,
  daily_cost_usd  double,
  run_id          text,
  loaded_at       timestamp,
  PRIMARY KEY ((org_id), service, usage_date)
) WITH CLUSTERING ORDER BY (service ASC, usage_date DESC);
