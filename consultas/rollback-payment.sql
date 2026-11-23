-- ROLLBACK local en la PAYMENT DB.
-- Se puede ejecutar en VS Code (conexión Payment) o con:
--   .\scripts\consultar.ps1 payment -Sql "\i /consultas/rollback-payment.sql"

SELECT '1) Antes: saldos actuales' AS paso;
SELECT customer_id AS cliente, balance AS saldo FROM accounts
WHERE customer_id IN ('Jeremy Delgado', 'Angel Alvarez') ORDER BY 1;

-- A) ROLLBACK manual: se cobra y se registra un pago, y luego se deshace.
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE customer_id = 'Jeremy Delgado';
INSERT INTO payments (order_id, customer_id, amount, status) VALUES ('PRUEBA-ROLLBACK', 'Jeremy Delgado', 100, 'PAID');

SELECT '2) Dentro de la transacción: Jeremy tiene $100 menos y existe el pago de prueba' AS paso;
SELECT customer_id AS cliente, balance AS saldo FROM accounts WHERE customer_id = 'Jeremy Delgado';
SELECT order_id AS pedido, amount AS monto, status AS estado FROM payments WHERE order_id = 'PRUEBA-ROLLBACK';
ROLLBACK;

SELECT '3) Después del ROLLBACK: el saldo volvió y el pago de prueba no existe' AS paso;
SELECT customer_id AS cliente, balance AS saldo FROM accounts WHERE customer_id = 'Jeremy Delgado';
SELECT COUNT(*) AS pagos_de_prueba FROM payments WHERE order_id = 'PRUEBA-ROLLBACK';

-- B) ROLLBACK por error: se descuenta a Jeremy y luego se intenta cobrar a
--    Angel más de lo que tiene; viola CHECK (balance >= 0).
SELECT '4) Transacción con error: -$50 a Jeremy y luego -$1000 a Angel (que tiene $20)' AS paso;
BEGIN;
UPDATE accounts SET balance = balance - 50 WHERE customer_id = 'Jeremy Delgado';
UPDATE accounts SET balance = balance - 1000 WHERE customer_id = 'Angel Alvarez';
ROLLBACK;

SELECT '5) A Jeremy tampoco se le descontó nada (atomicidad)' AS paso;
SELECT customer_id AS cliente, balance AS saldo FROM accounts
WHERE customer_id IN ('Jeremy Delgado', 'Angel Alvarez') ORDER BY 1;
