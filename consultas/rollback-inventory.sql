-- ROLLBACK local en la INVENTORY DB.
-- Dentro de UNA base de datos, una transacción es todo o nada (ACID).
-- Se puede ejecutar en VS Code (conexión Inventory) o con:
--   .\scripts\consultar.ps1 inventory -Sql "\i /consultas/rollback-inventory.sql"

SELECT '1) Antes: stock actual' AS paso;
SELECT product_id AS producto, stock FROM products WHERE product_id IN ('mystery-box', 'runner-pro') ORDER BY 1;

-- A) ROLLBACK manual: se hace un cambio y se deshace.
BEGIN;
UPDATE products SET stock = stock - 10 WHERE product_id = 'mystery-box';
INSERT INTO inventory_movements (movement_type, product_id, quantity, stock_before, stock_after, detail)
SELECT 'AJUSTE', product_id, -10, stock + 10, stock, 'Prueba de rollback (no debe quedar guardado)'
FROM products WHERE product_id = 'mystery-box';

SELECT '2) Dentro de la transacción: mystery-box bajó 10 y hay un movimiento nuevo' AS paso;
SELECT product_id AS producto, stock FROM products WHERE product_id = 'mystery-box';
SELECT COUNT(*) AS movimientos_de_prueba FROM inventory_movements WHERE detail LIKE 'Prueba de rollback%';
ROLLBACK;

SELECT '3) Después del ROLLBACK: el stock y el kardex volvieron como estaban' AS paso;
SELECT product_id AS producto, stock FROM products WHERE product_id = 'mystery-box';
SELECT COUNT(*) AS movimientos_de_prueba FROM inventory_movements WHERE detail LIKE 'Prueba de rollback%';

-- B) ROLLBACK por error: el segundo cambio viola CHECK (stock >= 0),
--    así que el primero tampoco se guarda.
SELECT '4) Transacción con error: se descuenta 1 de mystery-box y luego runner-pro queda en -5 (no permitido)' AS paso;
BEGIN;
UPDATE products SET stock = stock - 1 WHERE product_id = 'mystery-box';
UPDATE products SET stock = -5 WHERE product_id = 'runner-pro';
ROLLBACK;

SELECT '5) Ninguno de los dos cambios quedó guardado (atomicidad)' AS paso;
SELECT product_id AS producto, stock FROM products WHERE product_id IN ('mystery-box', 'runner-pro') ORDER BY 1;
