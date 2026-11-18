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

\echo '== 6. MOVIMIENTOS DE INVENTARIO (kardex): últimas 15 transacciones =='
-- quantity: negativo = sale stock, positivo = entra, 0 = rechazo sin cambio
SELECT id, created_at::timestamp(0) AS fecha, movement_type AS tipo, product_id AS producto,
       order_id AS pedido, customer_id AS cliente, quantity AS cantidad,
       stock_before AS stock_antes, stock_after AS stock_despues, event_type AS evento
FROM inventory_movements
ORDER BY id DESC
LIMIT 15;

\echo '== 7. Detalle de cada movimiento =='
SELECT id, movement_type AS tipo, order_id AS pedido, detail AS detalle
FROM inventory_movements
ORDER BY id DESC
LIMIT 15;

\echo '== 8. Kardex de un producto (studio-buds) en orden cronológico =='
SELECT id, movement_type AS tipo, order_id AS pedido, quantity AS cantidad,
       stock_before AS antes, stock_after AS despues, detail AS detalle
FROM inventory_movements
WHERE product_id = 'studio-buds'
ORDER BY id;

\echo '== 9. Resumen de movimientos por tipo =='
SELECT movement_type AS tipo, COUNT(*) AS movimientos, SUM(quantity) AS unidades
FROM inventory_movements
GROUP BY movement_type
ORDER BY movement_type;

\echo '== 10. Cuadre: el stock actual debe ser igual a la suma de sus movimientos =='
SELECT p.product_id AS producto, p.stock AS stock_actual,
       COALESCE(SUM(m.quantity), 0) AS suma_movimientos,
       p.stock = COALESCE(SUM(m.quantity), 0) AS cuadra
FROM products p
LEFT JOIN inventory_movements m ON m.product_id = p.product_id
WHERE p.product_id IN ('mystery-box', 'runner-pro', 'studio-buds')
GROUP BY p.product_id, p.stock
ORDER BY p.product_id;

\echo '== 11. Outbox: eventos publicados por Inventory =='
SELECT id, event->>'type' AS evento, event->>'order_id' AS pedido, topic,
       published_at IS NOT NULL AS publicado
FROM outbox
ORDER BY id DESC
LIMIT 10;

\echo '== 12. Eventos procesados (idempotencia) =='
SELECT COUNT(*) AS eventos_procesados FROM processed_events;
