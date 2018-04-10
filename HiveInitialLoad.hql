set hive.execution.engine=mr;
DROP TABLE IF EXISTS qareport.fact_sales_t7;
CREATE TABLE qareport.fact_sales_t7
(
fs_key INT,
order_date DATE,
load_time TIMESTAMP,
product_key INT,
order_id INT,
quantity INT,
price DECIMAL(20,2),
amount DECIMAL(36,2)
)
clustered by (fs_key) into 3 buckets
stored AS orc
tblproperties("transactional"="true");
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
WHERE s.last_update <= current_timestamp
GROUP BY
order_date,
s.last_update,
product_key,
order_id,
quantity,
price
;
DROP TABLE IF EXISTS qareport.fact_sales_summary_t7;
CREATE TABLE qareport.fact_sales_summary_t7
(
fs_key INT,
order_date DATE,
load_time TIMESTAMP,
totalsale DECIMAL(36,2)
)
clustered by (fs_key) into 3 buckets
stored AS orc
tblproperties("transactional"="true");
INSERT INTO qareport.fact_sales_summary_t7
(fs_key, order_date, load_time, totalsale)
SELECT
fs_key,
order_date,
load_time,
SUM(amount) totalsale
FROM qareport.fact_sales_t7
WHERE load_time <= current_timestamp
GROUP BY
fs_key,
order_date,
load_time
;
DROP TABLE IF EXISTS qareport.fact_sales_datamart_t7;
CREATE TABLE qareport.fact_sales_datamart_t7
(
fs_key INT,
load_time TIMESTAMP,
mrpsales DECIMAL(36,2)
)
clustered by (fs_key) into 3 buckets
stored AS orc
tblproperties("transactional"="true");
INSERT INTO qareport.fact_sales_datamart_t7
(fs_key, load_time, mrpsales)
SELECT
fs_key,
load_time,
SUM(totalsale) mrpsales
FROM qareport.fact_sales_summary_t7
WHERE load_time <= current_timestamp
GROUP BY
fs_key,
load_time
;
DROP TABLE IF EXISTS qareport.fact_sales_report_t7;
CREATE TABLE qareport.fact_sales_report_t7
(
fs_key INT,
load_time TIMESTAMP,
totalmrpsales DECIMAL(36,2)
)
clustered by (fs_key) into 3 buckets
stored AS orc
tblproperties("transactional"="true");
INSERT INTO qareport.fact_sales_report_t7
(fs_key, load_time, totalmrpsales)
SELECT
d.fs_key,
s.load_time,
sum(s.totalsale+d.mrpsales)*2 as totalmrpsales
FROM qareport.fact_sales_summary_t7 s
INNER JOIN qastage.order_detail_base_t7 o ON o.order_date = s.order_date
INNER JOIN qareport.fact_sales_datamart_t7 d ON d.load_time = s.load_time
WHERE s.load_time <= current_timestamp
GROUP BY
d.fs_key,
s.load_time
;
