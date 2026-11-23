-- ROLLBACK local en la ORDER DB.
-- Se puede ejecutar en VS Code (conexión Order) o con:
--   .\scripts\consultar.ps1 order -Sql "\i /consultas/rollback-order.sql"

SELECT '1) Antes: cantidad de pedidos' AS paso;
SELECT COUNT(*) AS pedidos FROM orders;

-- A) ROLLBACK manual: se crea un pedido, se confirma y se deshace todo.
BEGIN;
INSERT INTO orders (id, customer_id, product_id, quantity, amount, status)
VALUES ('PRUEBA-ROLLBACK', 'Jeremy Delgado', 'runner-pro', 1, 59, 'PENDING');
UPDATE orders SET status = 'CONFIRMED' WHERE id = 'PRUEBA-ROLLBACK';

SELECT '2) Dentro de la transacción: el pedido de prueba existe y está CONFIRMED' AS paso;
SELECT id, customer_id AS cliente, status AS estado FROM orders WHERE id = 'PRUEBA-ROLLBACK';
ROLLBACK;

SELECT '3) Después del ROLLBACK: el pedido de prueba ya no existe' AS paso;
SELECT COUNT(*) AS pedidos_de_prueba FROM orders WHERE id = 'PRUEBA-ROLLBACK';
SELECT COUNT(*) AS pedidos FROM orders;

-- B) ROLLBACK por error: el segundo INSERT tiene cantidad 0 y viola
--    CHECK (quantity > 0), así que el primero tampoco se guarda.
SELECT '4) Transacción con error: un pedido válido y luego uno con cantidad 0' AS paso;
BEGIN;
INSERT INTO orders (id, customer_id, product_id, quantity, amount, status)
VALUES ('PRUEBA-OK', 'Jeremy Delgado', 'runner-pro', 1, 59, 'PENDING');
INSERT INTO orders (id, customer_id, product_id, quantity, amount, status)
VALUES ('PRUEBA-MALA', 'Jeremy Delgado', 'runner-pro', 0, 0, 'PENDING');
ROLLBACK;

SELECT '5) Ninguno de los dos pedidos quedó guardado (atomicidad)' AS paso;
SELECT COUNT(*) AS pedidos_de_prueba FROM orders WHERE id LIKE 'PRUEBA-%';
