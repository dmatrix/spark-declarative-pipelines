CREATE MATERIALIZED VIEW cancelled_orders_mv AS
SELECT * FROM orders_mv
WHERE status = 'cancelled';
