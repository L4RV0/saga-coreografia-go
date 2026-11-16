-- ════════════════════════════════════════════════════════════════════════
-- Consultas de la INVENTORY DB (contenedor inventory-db, volumen inventory-data)
-- Ejecutar:  .\scripts\consultar.ps1 inventory
-- ════════════════════════════════════════════════════════════════════════

\echo '== 1. Tablas de la Inventory DB =='
\dt

\echo '== 2. Stock disponible por producto =='
SELECT product_id, stock FROM products ORDER BY product_id;

\echo '== 3. Últimas 10 reservas =='
SELECT order_id, product_id, quantity, status, created_at, updated_at
FROM reservations
ORDER BY created_at DESC
LIMIT 10;

\echo '== 4. Reservas por estado =='
-- RESERVED = el pedido siguió adelante · RELEASED = se compensó (stock devuelto)
SELECT status, COUNT(*) AS reservas, SUM(quantity) AS unidades
FROM reservations
GROUP BY status
ORDER BY status;

\echo '== 5. Compensaciones: reservas liberadas porque fallo el pago =='
SELECT order_id, product_id, quantity, updated_at AS liberada_en
FROM reservations
WHERE status = 'RELEASED'
ORDER BY updated_at DESC
LIMIT 10;

\echo '== 6. Outbox: eventos publicados por Inventory =='
SELECT id, event->>'type' AS evento, event->>'order_id' AS pedido, topic,
       published_at IS NOT NULL AS publicado
FROM outbox
ORDER BY id DESC
LIMIT 10;

\echo '== 7. Eventos procesados (idempotencia) =='
SELECT COUNT(*) AS eventos_procesados FROM processed_events;
