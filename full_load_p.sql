-------------------------------------
-- initial/full load for nagesh's tables
--------------------------------------
--drop temp tables
drop table  tmp_fact_sales_i_p;

drop table  tmp_fact_sales_summary_i_p;
--------------------------------------

CREATE VOLATILE TABLE tmp_fact_sales_i_p,NO LOG 
(
order_date DATE NOT NULL,
load_date DATE NOT NULL,
product_key INTEGER,
order_id INTEGER,
quantity INTEGER,
price DECIMAL(20,2),
amount DECIMAL(20,2)
)
ON COMMIT PRESERVE ROWS;
-----------------------------------------

CREATE VOLATILE TABLE tmp_fact_sales_summary_i_p,NO LOG 
(
order_date DATE NOT NULL,
load_date DATE NOT NULL,
totalsale DECIMAL(20,2)
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
WHERE s.order_date <= current_date
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
WHERE load_date <= current_date
GROUP BY
order_date,
load_date
;

-----------------------------------------------------------------
--Delete from Target table 
-----------------------------------------------------------------
--DELETE FROM qareport.fact_sales
--WHERE load_date <= current_date;
--------------------------
--DELETE FROM qareport.fact_sales_summary
--WHERE load_date <= current_date;
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
;
----------------------------
