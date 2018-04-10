-------------------------------------
-- incremental load for Nagesh's tables
--------------------------------------
--Drop temp tables
drop table tmp_fact_sales_i_p;
drop table tmp_fact_sales_summary_i_p;
drop table tmp_fact_sales_u_p;
drop table tmp_fact_sales_summary_u_p;
---------------------------------------------
CREATE VOLATILE TABLE tmp_fact_sales_i_p,NO LOG 
(
order_date DATE NOT NULL,
load_date DATE NOT NULL,
product_key INTEGER,
order_id INTEGER,
quantity INTEGER,
price DECIMAL(10,2),
amount DECIMAL(10,2)
)
ON COMMIT PRESERVE ROWS;


CREATE VOLATILE TABLE tmp_fact_sales_summary_i_p,NO LOG 
(
order_date DATE NOT NULL,
load_date DATE NOT NULL,
totalsale DECIMAL(10,2)
)
ON COMMIT PRESERVE ROWS;
-------------------------------------------------

CREATE VOLATILE TABLE tmp_fact_sales_u_p,NO LOG 
(
order_date DATE NOT NULL,
load_date DATE NOT NULL,
product_key INTEGER,
order_id INTEGER,
quantity INTEGER,
price DECIMAL(10,2),
amount DECIMAL(10,2)
)
ON COMMIT PRESERVE ROWS;



CREATE VOLATILE TABLE tmp_fact_sales_summary_u_p,NO LOG 
(
order_date DATE NOT NULL,
load_date DATE NOT NULL,
totalsale DECIMAL(10,2)
)
ON COMMIT PRESERVE ROWS;
-------------------------------------
--For amount Calculation - insert--
--------------------------------------
INSERT INTO tmp_fact_sales_i_p
(order_date, load_date, product_key, order_id, quantity, price, amount)
SELECT
s.order_date order_date,
current_date load_date,
COALESCE(p.product_key, 0) product_key,
COALESCE(s.order_id, 0) order_id,
COALESCE(s.quantity, 0) quantity,
COALESCE(s.price, 0) price,
COALESCE((s.quantity * s.price), 0) amount
FROM qastage.order_detail_p s
LEFT JOIN qastage.dim_product_p p ON s.product_id = p.product_id
WHERE s.order_date = current_date
GROUP BY
order_date,
load_date,
product_key,
order_id,
quantity,
price
;
-------------------------------------
--For total sale Calculation - insert--
--------------------------------------
INSERT INTO tmp_fact_sales_summary_i_p
(order_date, load_date, totalsale)
SELECT
order_date,
load_date,
SUM(amount) totalsale
FROM tmp_fact_sales_i_p
WHERE load_date = current_date
GROUP BY
order_date,
load_date
;
-------------------------------------
--For amount Calculation - update--
--------------------------------------
INSERT INTO tmp_fact_sales_u_p
(order_date, load_date, product_key, order_id, quantity, price, amount)
SELECT
order_date,
current_date load_date,
COALESCE(p.product_key, 0) product_key,
COALESCE(s.order_id, 0) order_id,
COALESCE(s.quantity, 0) quantity,
COALESCE(s.price, 0) price,
COALESCE((s.quantity * s.price), 0) amount
FROM qastage.order_detail_p s
LEFT JOIN qastage.dim_product_p p ON s.product_id = p.product_id
WHERE s.order_date < current_date
GROUP BY
order_date,
load_date,
product_key,
order_id,
quantity,
price
;
-------------------------------------
--For total sale Calculation - update--
--------------------------------------
INSERT INTO tmp_fact_sales_summary_u_p
(order_date, load_date, totalsale)
SELECT
order_date,
load_date,
SUM(amount) totalsale
FROM tmp_fact_sales_u_p
WHERE load_date < current_date
GROUP BY
order_date,
load_date
;
-----------------------------------------------------------------
--Delete from Target table the rows for the current date
-----------------------------------------------------------------
--DELETE FROM qareport.fact_sales
--WHERE order_date = current_date;
--------------------------
--DELETE FROM qareport.fact_sales_summary
--WHERE order_date = current_date;
-------------------------------------
--update Target tables---
--------------------------------------
/*
UPDATE qareport.fact_sales
FROM
(SELECT order_date,
load_date, 
product_key, 
order_id, 
quantity, 
price, 
amount
FROM tmp_fact_sales_u) t
SET
product_key = t.product_key, 
order_id = t.order_id, 
quantity = t.quantity, 
price = t.price, 
amount = t.amount
WHERE fact_sales.load_date = t.load_date
AND fact_sales.order_date = t.order_date
AND fact_sales.product_key = t.product_key
AND fact_sales.order_id = t.order_id
;

UPDATE qareport.fact_sales_summary
FROM
(SELECT order_date, load_date,
totalsale
FROM tmp_fact_sales_summary_u) t
SET
order_date = t.order_date,
load_date = t.load_date, 
totalsale = t.totalsale
WHERE fact_sales_summary.load_date = t.load_date
AND fact_sales_summary.order_date = t.order_date
;
*/
-------------------------------------

--------------------------
--Insert into TARGET table
--------------------------
INSERT INTO qareport.fact_sales_p
(order_date, load_date, product_key, order_id, quantity, price, amount)
SELECT
order_date,
load_date,
product_key,
order_id,
quantity,
price,
amount
FROM tmp_fact_sales_i_p
;
--------------------------
INSERT INTO qareport.fact_sales_summary_p
(order_date, load_date, totalsale)
SELECT
order_date,
load_date,
totalsale
FROM tmp_fact_sales_summary_i_p
----------------------------