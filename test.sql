WITH src AS (
  SELECT
    product_id,
    SUM(total_amount) AS rev_calc,
    SUM(quantity)     AS qty_calc,
    COUNT(*)          AS cnt_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY product_id
)
SELECT
  product_id,
  rev_calc,
  total_revenue,
  qty_calc,
  total_quantity,
  cnt_calc,
  sales_count
FROM src
JOIN clickhouse.mydb.sales_by_product USING (product_id)
WHERE rev_calc    <> total_revenue
   OR qty_calc    <> total_quantity
   OR cnt_calc    <> sales_count;

SELECT COUNT(*) AS cnt FROM clickhouse.mydb.top10_products;  -- должно быть 10

WITH src AS (
  SELECT
    p.category,
    SUM(f.total_amount) AS rev_calc
  FROM clickhouse.mydb.fact_sales f
  JOIN clickhouse.mydb.dim_product p USING (product_id)
  GROUP BY p.category
)
SELECT
  category,
  rev_calc,
  total_revenue
FROM src
JOIN clickhouse.mydb.revenue_by_category USING (category)
WHERE rev_calc <> total_revenue;

WITH src AS (
  SELECT
    product_id,
    AVG(rating)       AS avg_calc,
    SUM(review_count) AS sum_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY product_id
)
SELECT
  product_id,
  avg_calc,
  avg_rating,
  sum_calc,
  total_reviews
FROM src
JOIN clickhouse.mydb.avg_rating_reviews USING (product_id)
WHERE avg_calc <> avg_rating
   OR sum_calc <> total_reviews;

WITH src AS (
  SELECT
    product_id,
    AVG(rating)       AS avg_calc,
    SUM(review_count) AS sum_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY product_id
)
SELECT
  product_id,
  avg_calc,
  avg_rating,
  sum_calc,
  total_reviews
FROM src
JOIN clickhouse.mydb.avg_rating_reviews USING (product_id)
WHERE avg_calc <> avg_rating
   OR sum_calc <> total_reviews;

WITH src AS (
  SELECT
    customer_id,
    SUM(total_amount)       AS spent_calc,
    COUNT(DISTINCT sale_id) AS orders_calc,
    AVG(total_amount)       AS avg_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY customer_id
)
SELECT
  customer_id,
  spent_calc,
  total_spent,
  orders_calc,
  orders_count,
  avg_calc,
  avg_order_value
FROM src
JOIN clickhouse.mydb.sales_by_customer USING (customer_id)
WHERE spent_calc <> total_spent
   OR orders_calc <> orders_count
   OR avg_calc    <> avg_order_value;

SELECT COUNT(*) FROM clickhouse.mydb.top10_customers;  -- должно быть 10

WITH src AS (
  SELECT
    country,
    COUNT(DISTINCT customer_id) AS country_count
  FROM clickhouse.mydb.dim_customer
  GROUP BY country
)
SELECT
  country,
  country_count,
  customer_count
FROM src
JOIN clickhouse.mydb.customers_by_country USING (country)
WHERE country_count <> customer_count;

WITH src AS (
  SELECT
    EXTRACT(year  FROM sale_date) AS year,
    EXTRACT(month FROM sale_date) AS month,
    SUM(total_amount)             AS rev_calc,
    COUNT(*)                      AS cnt_calc,
    AVG(total_amount)             AS avg_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY 1,2
)
SELECT
  year,
  month,
  rev_calc,
  total_revenue,
  cnt_calc,
  sales_count,
  avg_calc,
  avg_order_amount
FROM src
JOIN clickhouse.mydb.sales_by_time USING (year, month)
WHERE rev_calc <> total_revenue
   OR cnt_calc <> sales_count
   OR avg_calc <> avg_order_amount;

WITH src AS (
  SELECT year, SUM(total_revenue) AS annual_calc
  FROM clickhouse.mydb.sales_by_time
  GROUP BY year
)
SELECT
  year,
  annual_calc,
  annual_revenue
FROM src
JOIN clickhouse.mydb.yearly_trends USING (year)
WHERE annual_calc <> annual_revenue;

WITH src AS (
  SELECT month, AVG(avg_order_amount) AS avg_calc
  FROM clickhouse.mydb.sales_by_time
  GROUP BY month
)
SELECT
  month,
  avg_calc,
  average_order_amount
FROM src
JOIN clickhouse.mydb.avg_order_by_month USING (month)
WHERE avg_calc <> average_order_amount;

WITH src AS (
  SELECT
    store_name,
    SUM(total_amount) AS rev_calc,
    COUNT(*)          AS cnt_calc,
    AVG(total_amount) AS avg_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY store_name
)
SELECT
  store_name,
  rev_calc,
  total_revenue,
  cnt_calc,
  sales_count,
  avg_calc,
  avg_order_amount
FROM src
JOIN clickhouse.mydb.sales_by_store USING (store_name)
WHERE rev_calc <> total_revenue
   OR cnt_calc <> sales_count
   OR avg_calc <> avg_order_amount;

SELECT COUNT(*) FROM clickhouse.mydb.top5_stores;  -- должно быть 5

WITH src AS (
  SELECT
    s.city,
    s.country,
    SUM(f.total_amount) AS rev_calc,
    COUNT(*)            AS cnt_calc
  FROM clickhouse.mydb.fact_sales f
  JOIN clickhouse.mydb.dim_store s USING (store_name)
  GROUP BY s.city, s.country
)
SELECT
  city,
  country,
  rev_calc,
  total_revenue,
  cnt_calc,
  sales_count
FROM src
JOIN clickhouse.mydb.sales_by_store_location
  ON src.city = sales_by_store_location.city
 AND src.country = sales_by_store_location.country
WHERE rev_calc <> total_revenue
   OR cnt_calc <> sales_count;

WITH src AS (
  SELECT
    supplier_name,
    SUM(total_amount) AS rev_calc,
    COUNT(*)          AS cnt_calc,
    AVG(unit_price)   AS avg_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY supplier_name
)
SELECT
  supplier_name,
  rev_calc,
  total_revenue,
  cnt_calc,
  sales_count,
  avg_calc,
  avg_unit_price
FROM src
JOIN clickhouse.mydb.sales_by_supplier USING (supplier_name)
WHERE rev_calc <> total_revenue
   OR cnt_calc <> sales_count
   OR avg_calc <> avg_unit_price;

SELECT COUNT(*) FROM clickhouse.mydb.top5_suppliers;  -- должно быть 5

WITH src AS (
  SELECT
    sup.country        AS supplier_country,
    SUM(f.total_amount) AS rev_calc,
    COUNT(*)            AS cnt_calc
  FROM clickhouse.mydb.fact_sales f
  JOIN clickhouse.mydb.dim_supplier sup USING (supplier_name)
  GROUP BY sup.country
)
SELECT
  supplier_country,
  rev_calc,
  total_revenue,
  cnt_calc,
  sales_count
FROM src
JOIN clickhouse.mydb.sales_by_supplier_country USING (supplier_country)
WHERE rev_calc <> total_revenue
   OR cnt_calc <> sales_count;

WITH corr_live AS (
  SELECT
    corr(rating, quantity)     AS corr_qty,
    corr(rating, total_amount) AS corr_rev
  FROM clickhouse.mydb.fact_sales
)
SELECT
  corr_qty,
  corr_rev,
  rating_vs_qty_corr,
  rating_vs_revenue_corr
FROM corr_live
CROSS JOIN clickhouse.mydb.rating_sales_correlation;

WITH src AS (
  SELECT
    product_id,
    AVG(rating) AS avg_calc,
    COUNT(*)     AS cnt_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY product_id
)
SELECT
  product_id,
  avg_calc,
  avg_rating,
  cnt_calc,
  sales_count
FROM src
JOIN clickhouse.mydb.products_by_rating USING (product_id)
WHERE avg_calc <> avg_rating
   OR cnt_calc <> sales_count;

SELECT COUNT(*) FROM clickhouse.mydb.top_rated_products;    -- 10  
SELECT COUNT(*) FROM clickhouse.mydb.bottom_rated_products; -- 10

WITH src AS (
  SELECT
    product_id,
    SUM(review_count) AS sum_calc
  FROM clickhouse.mydb.fact_sales
  GROUP BY product_id
)
SELECT
  product_id,
  sum_calc,
  total_reviews
FROM src
JOIN clickhouse.mydb.products_by_review_count USING (product_id)
WHERE sum_calc <> total_reviews;
