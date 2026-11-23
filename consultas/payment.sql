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

\echo '== 6. MOVIMIENTOS DE DINERO: últimas 15 transacciones =='
-- amount: negativo = sale dinero (CARGO), positivo = entra (REEMBOLSO), 0 = rechazo
SELECT id, created_at::timestamp(0) AS fecha, movement_type AS tipo, kind AS clase, customer_id AS cliente,
       order_id AS pedido, amount AS monto, balance_before AS saldo_antes, balance_after AS saldo_despues,
       event_type AS evento
FROM account_movements
ORDER BY id DESC
LIMIT 15;

\echo '== 7. Transacciones compensatorias en Payment: reembolsos (C3) =='
SELECT id, order_id AS pedido, customer_id AS cliente, amount AS monto, detail AS detalle
FROM account_movements
WHERE kind = 'COMPENSACION'
ORDER BY id DESC;

\echo '== 8. Cuadre: el saldo actual debe ser igual a la suma de sus movimientos =='
SELECT a.customer_id AS cliente, a.balance AS saldo, SUM(m.amount) AS suma_movimientos,
       a.balance = SUM(m.amount) AS cuadra
FROM accounts a
JOIN account_movements m USING (customer_id)
WHERE a.customer_id IN ('Jeremy Delgado', 'Angel Alvarez', 'Erick Cevallos')
GROUP BY a.customer_id, a.balance
ORDER BY 1;

\echo '== 9. Outbox: eventos publicados por Payment =='
SELECT id, event->>'type' AS evento, event->>'order_id' AS pedido, topic,
       published_at IS NOT NULL AS publicado
FROM outbox
ORDER BY id DESC
LIMIT 10;

\echo '== 10. Eventos procesados (idempotencia) =='
SELECT COUNT(*) AS eventos_procesados FROM processed_events;
