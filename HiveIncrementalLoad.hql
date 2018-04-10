set hive.execution.engine=mr;
--Staging incremental update script
DROP VIEW IF EXISTS qastage.reconcile_view;
CREATE VIEW qastage.reconcile_view AS
SELECT t1.* FROM
(SELECT * FROM qastage.order_detail_base_t7
UNION
SELECT * FROM qastage.order_detail_incr_t7) t1
JOIN
(SELECT order_id, max(last_update) max_last_update FROM
(SELECT * FROM qastage.order_detail_base_t7
UNION
SELECT * FROM qastage.order_detail_incr_t7) t2
GROUP BY order_id) s
ON t1.order_id = s.order_id AND t1.last_update = s.max_last_update;
DROP TABLE IF EXISTS qastage.order_detail_incr_t7;
DROP TABLE IF EXISTS qastage.order_detail_base_t7;
CREATE TABLE qastage.order_detail_base_t7 AS
SELECT * FROM qastage.reconcile_view;
--Reporting incremental load hql
DELETE FROM qareport.fact_sales_t7 WHERE cast(load_time as date) in (current_date);
INSERT INTO qareport.fact_sales_t7
(fs_key, order_date, load_time, product_key, order_id, quantity, price, amount)
SELECT
ROW_NUMBER() OVER(order by s.order_date) as fs_key,
s.order_date order_date,
s.last_update load_time,
COALESCE(p.product_key, 0) product_key,
COALESCE(s.order_id, 0) order_id,
COALESCE(s.quantity, 0) quantity,
COALESCE(s.price, 0) price,
COALESCE((s.quantity * s.price), 0) amount
FROM qastage.order_detail_base_t7 s
LEFT JOIN qastage.dim_product_t7 p ON s.product_id = p.product_id
WHERE cast(s.last_update as date) = current_date
GROUP BY
order_date,
s.last_update,
product_key,
order_id,
quantity,
price
;
DELETE FROM qareport.fact_sales_summary_t7 WHERE cast(load_time as date) in (current_date);
INSERT INTO qareport.fact_sales_summary_t7
(fs_key, order_date, load_time, totalsale)
SELECT
fs_key,
order_date,
load_time,
SUM(amount) totalsale
FROM qareport.fact_sales_t7
WHERE cast(load_time as date) = current_date
GROUP BY
fs_key,
order_date,
load_time
;
DELETE FROM qareport.fact_sales_datamart_t7 WHERE cast(load_time as date) in (current_date);
INSERT INTO qareport.fact_sales_datamart_t7
(fs_key, load_time, mrpsales)
SELECT
fs_key,
load_time,
SUM(totalsale) mrpsales
FROM qareport.fact_sales_summary_t7
WHERE cast(load_time as date) = current_date
GROUP BY
fs_key,
load_time
;
DELETE FROM qareport.fact_sales_report_t7 WHERE cast(load_time as date) in (current_date);
INSERT INTO qareport.fact_sales_report_t7
(fs_key, load_time, totalmrpsales)
SELECT
d.fs_key,
s.load_time,
sum(s.totalsale+d.mrpsales)*2 as totalmrpsales
FROM qareport.fact_sales_summary_t7 s
INNER JOIN qastage.order_detail_base_t7 o ON o.order_date = s.order_date
INNER JOIN qareport.fact_sales_datamart_t7 d ON d.load_time = s.load_time
WHERE cast(d.load_time as date) = current_date
GROUP BY
d.fs_key,
s.load_time
;
