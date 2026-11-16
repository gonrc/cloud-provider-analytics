-- Las dos consultas mínimas de la entrega 2 sobre Cassandra (consigna 6.2 y 7.4).
-- La organización de ejemplo es la de mayor costo total (org_53lc58dr, 6 servicios).
-- Los datos de muestra terminan el 31/08/2025: "últimos 14 días" es del 18 al 31 de agosto.
-- Ninguna usa ALLOW FILTERING: cada una lee una sola partición (org_id).

-- Consulta 1 (7.4.1): costos y requests diarios por organización y servicio en un rango
-- de fechas. Lee la partición de la organización y un rango contiguo del clustering.
SELECT usage_date, service, daily_cost_usd, requests, events
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = 'org_53lc58dr'
  AND usage_date >= '2025-08-25' AND usage_date <= '2025-08-31';

-- Consulta 2 (7.4.2): top-N servicios por costo acumulado en los últimos 14 días para una
-- organización. Cassandra suma por servicio (GROUP BY sobre el prefijo de clustering); el
-- IN es necesario para poder restringir usage_date, la columna que sigue a service.
-- CQL no ordena por un agregado: con 6 servicios, el orden descendente se aplica al leer.
SELECT service, SUM(daily_cost_usd) AS cost_last_14d, COUNT(*) AS days
FROM cloud_analytics.org_service_daily_cost
WHERE org_id = 'org_53lc58dr'
  AND service IN ('analytics', 'compute', 'database', 'genai', 'networking', 'storage')
  AND usage_date >= '2025-08-18'
GROUP BY service;

-- Control: filas cargadas en cada tabla (debe coincidir con las filas de Gold).
SELECT COUNT(*) AS filas_org_daily_usage_by_service FROM cloud_analytics.org_daily_usage_by_service;
SELECT COUNT(*) AS filas_org_service_daily_cost FROM cloud_analytics.org_service_daily_cost;
