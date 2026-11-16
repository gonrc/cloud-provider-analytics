# Backlog para el MVP final

Estado al 16/11/2026, después de la entrega 2. Las categorías siguen la consigna (6.2): lo obligatorio para la entrega final del 07/12/2026, lo deseable si no pone en riesgo lo obligatorio, y lo que queda fuera de alcance.

## Obligatorio

| # | Ítem | Por qué | Base que ya existe |
|---|---|---|---|
| 1 | Mart `revenue_by_org_month` y consulta 4 en Cassandra | Consulta mínima 4 (7.4) | Silver de facturación con D-10 aplicado y `revenue_usd` por factura |
| 2 | Silver de `support_tickets` (CSAT fuera de rango a nulo, abiertos sin `resolved_at`), mart `tickets_by_org_date` y consulta 3 | Consulta mínima 3 | Bronze de tickets; hallazgos de calidad del notebook 01 |
| 3 | Mart `genai_tokens_by_org_date` con los dos costos de D-12 y consulta 5 | Consulta mínima 5 | `genai_tokens` en Silver y Gold; precio en `config/settings.toml` |
| 4 | Una tabla query-first por consulta nueva, con su CQL y su comparación contra el lake | Evidencia obligatoria de 7.4 | Patrón de D-14 y experimento E5 |
| 5 | `cost_anomaly_mart` en Cassandra y métricas del componente analítico (estabilidad del umbral, revisión manual de los días marcados) | Analítica integrada con métricas y limitaciones (7.2.6) | Mart y experimento E3 |
| 6 | Validar el Quickstart desde un entorno limpio, local y en Colab | Criterio 7.6 | README y `make pipeline` |
| 7 | Test de integración: el pipeline completo sobre una muestra chica, con conteos esperados | Pruebas (7.2.8) | 10 tests unitarios |
| 8 | Presentación ejecutiva, video y guion de la defensa con participación de los cuatro | Artefactos finales (7.5) | Informe de la entrega 2 y experimentos |
| 9 | Diagrama y documentación finales consistentes con el código | Criterio 7.6 | Diagrama v2, `gobierno.md`, `DECISIONS.md` |

## Deseable

| # | Ítem | Motivo |
|---|---|---|
| 10 | Compactar quarantine de eventos | Hoy son 58 archivos de 4,5 KB (experimento E4) |
| 11 | Partición de Cassandra por organización y mes | Con más historia, una partición por organización crece sin límite |
| 12 | Alertas sobre las métricas del streaming (`inputRowsPerSecond` mayor a `processedRowsPerSecond`, filas en estado) | Observabilidad que pide la clase de Structured Streaming |
| 13 | Usuario de solo lectura en Cassandra (`PasswordAuthenticator`) | Separar el acceso de consumo del de carga |
| 14 | Demo con `trigger(processingTime=...)` además de `availableNow` | Mostrar el streaming en modo continuo |
| 15 | Tablero de visualización sobre Cassandra | Consumo por FinOps |

## Fuera de alcance

| Ítem | Motivo |
|---|---|
| Delta Lake, Iceberg | La consigna pide Parquet y la idempotencia ya está resuelta sin `MERGE` (D-08) |
| Kafka u otro broker | La fuente del caso es un directorio de archivos |
| Clúster multinodo de Spark o Cassandra | El volumen de la muestra no lo justifica; el diseño ya es distribuido |
| Real-Time Mode, CDC, transformWithState | No los necesita ninguna consulta del caso |
