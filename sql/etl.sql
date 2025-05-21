-- 1) Размерность дат
DROP TABLE IF EXISTS clickhouse.mydb.dim_date;
CREATE TABLE clickhouse.mydb.dim_date AS
SELECT DISTINCT
  sale_date                AS date,
  year(sale_date)          AS year,
  month(sale_date)         AS month,
  day(sale_date)           AS day
FROM (
  SELECT sale_date FROM postgresql.public.mock_data
  UNION ALL
  SELECT sale_date FROM clickhouse.mydb.sales
) t;

-- 2) Размерность клиентов
DROP TABLE IF EXISTS clickhouse.mydb.dim_customer;
CREATE TABLE clickhouse.mydb.dim_customer AS
SELECT
  CAST(sale_customer_id AS BIGINT)    AS customer_id,
  any_value(customer_first_name)      AS first_name,
  any_value(customer_last_name)       AS last_name,
  any_value(customer_age)             AS age,
  any_value(customer_email)           AS email,
  any_value(customer_country)         AS country
FROM (
  SELECT * FROM postgresql.public.mock_data
  UNION ALL
  SELECT * FROM clickhouse.mydb.sales
) t
GROUP BY CAST(sale_customer_id AS BIGINT);

-- 3) Размерность продуктов
DROP TABLE IF EXISTS clickhouse.mydb.dim_product;
CREATE TABLE clickhouse.mydb.dim_product AS
SELECT
  CAST(sale_product_id AS BIGINT)     AS product_id,
  any_value(product_name)             AS product_name,
  any_value(product_category)         AS category,
  any_value(product_brand)            AS brand,
  any_value(product_material)         AS material,
  any_value(product_size)             AS size,
  any_value(product_color)            AS color
FROM (
  SELECT * FROM postgresql.public.mock_data
  UNION ALL
  SELECT * FROM clickhouse.mydb.sales
) t
GROUP BY CAST(sale_product_id AS BIGINT);

-- 4) Размерность магазинов
DROP TABLE IF EXISTS clickhouse.mydb.dim_store;
CREATE TABLE clickhouse.mydb.dim_store AS
SELECT
  store_name                          AS store_name,
  any_value(store_location)           AS location,
  any_value(store_city)               AS city,
  any_value(store_state)              AS state,
  any_value(store_country)            AS country,
  any_value(store_phone)              AS phone,
  any_value(store_email)              AS email
FROM (
  SELECT * FROM postgresql.public.mock_data
  UNION ALL
  SELECT * FROM clickhouse.mydb.sales
) t
GROUP BY store_name;

-- 5) Размерность поставщиков
DROP TABLE IF EXISTS clickhouse.mydb.dim_supplier;
CREATE TABLE clickhouse.mydb.dim_supplier AS
SELECT
  supplier_name                       AS supplier_name,
  any_value(supplier_contact)         AS contact,
  any_value(supplier_email)           AS email,
  any_value(supplier_phone)           AS phone,
  any_value(supplier_address)         AS address,
  any_value(supplier_city)            AS city,
  any_value(supplier_country)         AS country
FROM (
  SELECT * FROM postgresql.public.mock_data
  UNION ALL
  SELECT * FROM clickhouse.mydb.sales
) t
GROUP BY supplier_name;

-- 6) Фактовая таблица продаж (де-дуплируем по sale_id)
DROP TABLE IF EXISTS clickhouse.mydb.fact_sales;
CREATE TABLE clickhouse.mydb.fact_sales AS
SELECT
  sale_id,
  any_value(sale_date)     AS sale_date,
  any_value(customer_id)   AS customer_id,
  any_value(product_id)    AS product_id,
  any_value(store_name)    AS store_name,
  any_value(supplier_name) AS supplier_name,
  any_value(quantity)      AS quantity,
  any_value(unit_price)    AS unit_price,
  any_value(total_amount)  AS total_amount,
  any_value(rating)        AS rating,
  any_value(review_count)  AS review_count
FROM (
  SELECT
    CAST(id                AS BIGINT)     AS sale_id,
    sale_date                                AS sale_date,
    CAST(sale_customer_id AS BIGINT)       AS customer_id,
    CAST(sale_product_id  AS BIGINT)       AS product_id,
    store_name                               AS store_name,
    supplier_name                            AS supplier_name,
    sale_quantity                            AS quantity,
    CAST(product_price    AS DECIMAL(18,2)) AS unit_price,
    sale_total_price                         AS total_amount,
    product_rating                           AS rating,
    product_reviews                          AS review_count
  FROM postgresql.public.mock_data

  UNION ALL

  SELECT
    CAST(id                AS BIGINT)     AS sale_id,
    sale_date                                AS sale_date,
    CAST(sale_customer_id AS BIGINT)       AS customer_id,
    CAST(sale_product_id  AS BIGINT)       AS product_id,
    store_name                               AS store_name,
    supplier_name                            AS supplier_name,
    sale_quantity                            AS quantity,
    CAST(product_price    AS DECIMAL(18,2)) AS unit_price,
    sale_total_price                         AS total_amount,
    product_rating                           AS rating,
    product_reviews                          AS review_count
  FROM clickhouse.mydb.sales
) t
GROUP BY sale_id;

-- Продажи по продуктам
DROP TABLE IF EXISTS clickhouse.mydb.sales_by_product;
CREATE TABLE clickhouse.mydb.sales_by_product AS
SELECT
  p.product_id,
  p.product_name,
  SUM(f.total_amount) AS total_revenue,
  SUM(f.quantity)     AS total_quantity,
  COUNT(*)            AS sales_count
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_product p ON f.product_id = p.product_id
GROUP BY
  p.product_id,
  p.product_name;

-- Топ-10 самых продаваемых продуктов
DROP TABLE IF EXISTS clickhouse.mydb.top10_products;
CREATE TABLE clickhouse.mydb.top10_products AS
SELECT
  product_id,
  product_name,
  total_revenue
FROM clickhouse.mydb.sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;

-- Общая выручка по категориям продуктов
DROP TABLE IF EXISTS clickhouse.mydb.revenue_by_category;
CREATE TABLE clickhouse.mydb.revenue_by_category AS
SELECT
  p.category,
  SUM(f.total_amount) AS total_revenue
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_product p ON f.product_id = p.product_id
GROUP BY
  p.category;

-- Средний рейтинг и количество отзывов для каждого продукта
DROP TABLE IF EXISTS clickhouse.mydb.avg_rating_reviews;
CREATE TABLE clickhouse.mydb.avg_rating_reviews AS
SELECT
  p.product_id,
  p.product_name,
  AVG(f.rating)       AS avg_rating,
  SUM(f.review_count) AS total_reviews
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_product p ON f.product_id = p.product_id
GROUP BY
  p.product_id,
  p.product_name;

-- Продажи по клиентам
DROP TABLE IF EXISTS clickhouse.mydb.sales_by_customer;
CREATE TABLE clickhouse.mydb.sales_by_customer AS
SELECT
  c.customer_id,
  concat(c.first_name, ' ', c.last_name) AS customer_name,
  SUM(f.total_amount)                    AS total_spent,
  COUNT(DISTINCT f.sale_id)              AS orders_count,
  AVG(f.total_amount)                    AS avg_order_value
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_customer c ON f.customer_id = c.customer_id
GROUP BY
  c.customer_id,
  concat(c.first_name, ' ', c.last_name);

-- Топ-10 клиентов по общим расходам
DROP TABLE IF EXISTS clickhouse.mydb.top10_customers;
CREATE TABLE clickhouse.mydb.top10_customers AS
SELECT
  customer_id,
  customer_name,
  total_spent
FROM clickhouse.mydb.sales_by_customer
ORDER BY total_spent DESC
LIMIT 10;

-- Распределение клиентов по странам
DROP TABLE IF EXISTS clickhouse.mydb.customers_by_country;
CREATE TABLE clickhouse.mydb.customers_by_country AS
SELECT
  country,
  COUNT(DISTINCT customer_id) AS customer_count
FROM clickhouse.mydb.dim_customer
GROUP BY
  country;

-- Продажи по времени
DROP TABLE IF EXISTS clickhouse.mydb.sales_by_time;
CREATE TABLE clickhouse.mydb.sales_by_time AS
SELECT
  d.year,
  d.month,
  SUM(f.total_amount) AS total_revenue,
  COUNT(f.sale_id)    AS sales_count,
  AVG(f.total_amount) AS avg_order_amount
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_date d ON f.sale_date = d.date
GROUP BY
  d.year,
  d.month;

-- Месячные и годовые тренды
DROP TABLE IF EXISTS clickhouse.mydb.yearly_trends;
CREATE TABLE clickhouse.mydb.yearly_trends AS
SELECT
  year,
  SUM(total_revenue) AS annual_revenue
FROM clickhouse.mydb.sales_by_time
GROUP BY
  year
ORDER BY
  year;

-- Средний размер заказа по месяцам
DROP TABLE IF EXISTS clickhouse.mydb.avg_order_by_month;
CREATE TABLE clickhouse.mydb.avg_order_by_month AS
SELECT
  month,
  AVG(avg_order_amount) AS average_order_amount
FROM clickhouse.mydb.sales_by_time
GROUP BY
  month
ORDER BY
  month;

-- Продажи по магазинам
DROP TABLE IF EXISTS clickhouse.mydb.sales_by_store;
CREATE TABLE clickhouse.mydb.sales_by_store AS
SELECT
  s.store_name,
  SUM(f.total_amount) AS total_revenue,
  COUNT(f.sale_id)    AS sales_count,
  AVG(f.total_amount) AS avg_order_amount
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_store s ON f.store_name = s.store_name
GROUP BY
  s.store_name;

-- Топ-5 магазинов
DROP TABLE IF EXISTS clickhouse.mydb.top5_stores;
CREATE TABLE clickhouse.mydb.top5_stores AS
SELECT
  store_name,
  total_revenue
FROM clickhouse.mydb.sales_by_store
ORDER BY total_revenue DESC
LIMIT 5;

-- Распределение продаж по городам и странам
DROP TABLE IF EXISTS clickhouse.mydb.sales_by_store_location;
CREATE TABLE clickhouse.mydb.sales_by_store_location AS
SELECT
  s.city,
  s.country,
  SUM(f.total_amount) AS total_revenue,
  COUNT(f.sale_id)    AS sales_count
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_store s ON f.store_name = s.store_name
GROUP BY
  s.city,
  s.country;

-- Продажи по поставщикам
DROP TABLE IF EXISTS clickhouse.mydb.sales_by_supplier;
CREATE TABLE clickhouse.mydb.sales_by_supplier AS
SELECT
  sup.supplier_name,
  SUM(f.total_amount) AS total_revenue,
  COUNT(f.sale_id)    AS sales_count,
  AVG(f.unit_price)   AS avg_unit_price
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_supplier sup ON f.supplier_name = sup.supplier_name
GROUP BY
  sup.supplier_name;

-- Топ-5 поставщиков
DROP TABLE IF EXISTS clickhouse.mydb.top5_suppliers;
CREATE TABLE clickhouse.mydb.top5_suppliers AS
SELECT
  supplier_name,
  total_revenue
FROM clickhouse.mydb.sales_by_supplier
ORDER BY total_revenue DESC
LIMIT 5;

-- Витрина качества продукции
DROP TABLE IF EXISTS clickhouse.mydb.rating_sales_correlation;
CREATE TABLE clickhouse.mydb.rating_sales_correlation AS
SELECT
  corr(rating, quantity)     AS rating_vs_qty_corr,
  corr(rating, total_amount) AS rating_vs_revenue_corr
FROM clickhouse.mydb.fact_sales;

DROP TABLE IF EXISTS clickhouse.mydb.products_by_rating;
CREATE TABLE clickhouse.mydb.products_by_rating AS
SELECT
  p.product_id,
  p.product_name,
  AVG(f.rating) AS avg_rating,
  COUNT(*)      AS sales_count
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_product p ON f.product_id = p.product_id
GROUP BY
  p.product_id,
  p.product_name;

-- Топ-10 по рейтингу
DROP TABLE IF EXISTS clickhouse.mydb.top_rated_products;
CREATE TABLE clickhouse.mydb.top_rated_products AS
SELECT
  product_id,
  product_name,
  avg_rating
FROM clickhouse.mydb.products_by_rating
ORDER BY avg_rating DESC
LIMIT 10;

-- Bottom-10 по рейтингу
DROP TABLE IF EXISTS clickhouse.mydb.bottom_rated_products;
CREATE TABLE clickhouse.mydb.bottom_rated_products AS
SELECT
  product_id,
  product_name,
  avg_rating
FROM clickhouse.mydb.products_by_rating
ORDER BY avg_rating ASC
LIMIT 10;

-- Топ-10 по количеству отзывов
DROP TABLE IF EXISTS clickhouse.mydb.products_by_review_count;
CREATE TABLE clickhouse.mydb.products_by_review_count AS
SELECT
  p.product_id,
  p.product_name,
  SUM(f.review_count) AS total_reviews
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_product p ON f.product_id = p.product_id
GROUP BY
  p.product_id,
  p.product_name
ORDER BY
  total_reviews DESC
LIMIT 10;


-- Распределение продаж по странам поставщиков
DROP TABLE IF EXISTS clickhouse.mydb.sales_by_supplier_country;
CREATE TABLE clickhouse.mydb.sales_by_supplier_country AS
SELECT
  sup.country            AS supplier_country,
  SUM(f.total_amount)    AS total_revenue,
  COUNT(f.sale_id)       AS sales_count
FROM clickhouse.mydb.fact_sales f
JOIN clickhouse.mydb.dim_supplier sup
  ON f.supplier_name = sup.supplier_name
GROUP BY
  sup.country
ORDER BY
  total_revenue DESC;
