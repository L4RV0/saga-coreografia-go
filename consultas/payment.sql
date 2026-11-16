-- ════════════════════════════════════════════════════════════════════
-- Consultas de la PAYMENT DB (contenedor payment-db, volumen payment-data)
-- Ejecutar:  .\scripts\consultar.ps1 payment
-- ════════════════════════════════════════════════════════════════════

\echo '== 1. Tablas de la Payment DB =='
\dt

\echo '== 2. Saldo de cada cliente =='
SELECT customer_id, balance
FROM accounts
WHERE customer_id IN ('Jeremy Delgado', 'Angel Alvarez', 'Erick Cevallos')
ORDER BY customer_id;

\echo '== 3. Últimos 10 pagos =='
SELECT order_id, customer_id, amount, status, reason, created_at
FROM payments
ORDER BY created_at DESC
LIMIT 10;

\echo '== 4. Pagos por estado =='
SELECT status, COUNT(*) AS pagos, SUM(amount) AS monto
FROM payments
GROUP BY status
ORDER BY status;

\echo '== 5. Pagos rechazados (los que dispararon compensaciones) =='
SELECT order_id, customer_id, amount, reason
FROM payments
WHERE status = 'FAILED'
ORDER BY created_at DESC
LIMIT 10;

\echo '== 6. Outbox: eventos publicados por Payment =='
SELECT id, event->>'type' AS evento, event->>'order_id' AS pedido, topic,
       published_at IS NOT NULL AS publicado
FROM outbox
ORDER BY id DESC
LIMIT 10;

\echo '== 7. Eventos procesados (idempotencia) =='
SELECT COUNT(*) AS eventos_procesados FROM processed_events;
