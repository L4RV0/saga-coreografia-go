-- ════════════════════════════════════════════════════════════════
-- Consultas de la ORDER DB (contenedor order-db, volumen order-data)
-- Ejecutar:  .\scripts\consultar.ps1 order
-- ════════════════════════════════════════════════════════════════

\echo '== 1. Tablas de la Order DB =='
\dt

\echo '== 2. Catálogo de productos y precios =='
SELECT product_id, price FROM catalog ORDER BY product_id;

\echo '== 3. Últimos 10 pedidos =='
SELECT id, customer_id, product_id, quantity, amount, status, reason
FROM orders
ORDER BY created_at DESC
LIMIT 10;

\echo '== 4. Pedidos por estado (resultado de las sagas) =='
SELECT status, COUNT(*) AS pedidos, SUM(amount) AS monto_total
FROM orders
GROUP BY status
ORDER BY status;

\echo '== 5. Pedidos cancelados y su motivo (compensaciones) =='
SELECT id, customer_id, product_id, reason
FROM orders
WHERE status = 'CANCELLED'
ORDER BY created_at DESC
LIMIT 10;

\echo '== 6. Sagas que siguen en curso (deberia estar vacio) =='
SELECT id, customer_id, status, created_at FROM orders WHERE status IN ('PENDING', 'CANCELLING');

\echo '== 6b. Historial de estados: últimas 15 transacciones =='
SELECT id, created_at::timestamp(0) AS fecha, order_id AS pedido, from_status AS de, to_status AS a,
       kind AS tipo, event_type AS evento, detail AS detalle
FROM order_status_history
ORDER BY id DESC
LIMIT 15;

\echo '== 6c. Transacciones compensatorias en Order: cancelaciones (C1) =='
SELECT id, order_id AS pedido, from_status AS de, to_status AS a, detail AS detalle
FROM order_status_history
WHERE kind = 'COMPENSACION'
ORDER BY id DESC;

\echo '== 7. Outbox: eventos publicados por Order =='
SELECT id, event->>'type' AS evento, event->>'order_id' AS pedido, topic,
       published_at IS NOT NULL AS publicado
FROM outbox
ORDER BY id DESC
LIMIT 10;

\echo '== 8. Eventos recibidos y procesados (idempotencia) =='
SELECT COUNT(*) AS eventos_procesados FROM processed_events;
