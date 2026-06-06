-------------------------------------------------
-- CASE STUDY: Exploring Olist E-Commerce  Data--
-------------------------------------------------

-- Tool used: MySQL Workbench

-------------------------------------------------
-- CASE STUDY QUESTIONS AND ANSWERS--
-------------------------------------------------

-- Database Setup
CREATE database olist;
USE olist;

-- Changing header product_category_translation table
ALTER TABLE 
	product_category_translation
RENAME COLUMN
	ï»¿product_category_name TO product_category_name;


----------------- Customer Behaviour -----------------------

-- Repeat customers vs one time buyers
with cte AS(
	SELECT 
	   customer_id,
           COUNT(order_id) AS order_count
	FROM 
    	   olist_orders_dataset
 	GROUP BY customer_id)
SELECT
    COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) AS repeat_customers,
    COUNT(DISTINCT CASE WHEN order_count = 1 THEN customer_id END) AS one_time_custmers
FROM 
    cte;

-- Average Order per customer by state
SELECT 
    c.customer_state,
    COUNT(order_id)/COUNT(DISTINCT o.customer_id) AS avg_orders_per_customer
FROM 
    olist_orders_dataset o 
JOIN
    olist_customers_dataset c
    ON o.customer_id = c.customer_id
GROUP BY 
    c.customer_state
ORDER BY avg_orders_per_customer;


-- CUSTOMER LIFETIME values
SELECT 
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price) AS total_revenue,
    SUM(oi.price) / COUNT(DISTINCT o.order_id) AS avg_order_value
FROM 
	olist_orders_dataset o
JOIN 
	olist_order_items_dataset oi 
	ON 
		o.order_id = oi.order_id
GROUP BY 
	o.customer_id
ORDER BY 
	total_revenue DESC
LIMIT 10;



-- CUSTOMER RETENTION VS CHURN RATE
WITH CTE AS (
    SELECT customer_id, COUNT(order_id) AS order_count
    FROM olist_orders_dataset
    GROUP BY customer_id
)
SELECT 
    COUNT(CASE WHEN order_count > 1 THEN customer_id END) * 100.0 / COUNT(customer_id) AS retention_rate,
    COUNT(CASE WHEN order_count = 1 THEN customer_id END) * 100.0 / COUNT(customer_id) AS churn_rate
FROM CTE;


 --------------------Sales Insight -----------------   

-- Top 5 selling products
SELECT
	p.product_category_name, 
    COUNT(oi.order_id) AS total_sales
FROM
	olist_order_items_dataset oi
join olist_products_dataset p 
	ON oi.product_id = p.product_id
GROUP BY
	p.product_category_name
ORDER BY total_sales DESC
LIMIT 5;
	

-- Top 5 cities with highest orders
SELECT 
	c.customer_city,
    COUNT(o.order_id) AS total_orders
FROM olist_customers_dataset c
JOIN olist_orders_dataset o
	ON o.customer_id = c.customer_id
GROUP BY c.customer_city
ORDER BY total_orders DESC
LIMIT 5;


-- Monthly Sales trend
SELECT
    EXTRACT(MONTH FROM order_purchase_timestamp) AS order_month,
    COUNT(o.order_id) AS total_orders,
    SUM(price) AS total_revenue
FROM 
    olist_orders_dataset o
JOIN
    olist_order_items_dataset oi
    ON o.order_id = oi.order_id
GROUP BY 
    order_month
ORDER BY 
    order_month;
    

-- High revenue generating sellers
SELECT
    s.seller_id,
    s.seller_city,
    ROUND(SUM(oi.price),2) AS total_revenue
FROM 
    olist_sellers_dataset s
JOIN 
    olist_order_items_dataset oi
   ON s.seller_id = oi.seller_id
GROUP BY 
    s.seller_id,
    s.seller_city
ORDER BY
    total_revenue DESC;


-- PRODUCTS GENERATING HIGHEST REVENUE
SELECT 
	p.product_id, 
    pct.product_category_name_english, 
    ROUND(SUM(oi.price), 2) AS total_revenue
FROM 
	olist_order_items_dataset oi
JOIN 
	olist_products_dataset p 
    ON 
		oi.product_id = p.product_id
JOIN 
	product_category_translation pct 
    ON 
		p.product_category_name = pct.product_category_name
GROUP BY 
	p.product_id, pct.product_category_name_english
ORDER BY 
	total_revenue DESC
LIMIT 10;


-- TOP 10 BEST SELLING PRODUCTS CATEGORIES
SELECT 
	pct.product_category_name_english, 
    COUNT(*) AS total_sold
FROM 
	olist_order_items_dataset oi
JOIN 
	olist_products_dataset p 
    ON 
		oi.product_id = p.product_id
JOIN 
	product_category_translation pct 
	ON 
		p.product_category_name = pct.product_category_name
GROUP BY 
	pct.product_category_name_english
ORDER BY 
	total_sold DESC
LIMIT 10;



---------------- DELIVERY PERFORMACE ----------------

-- AVG DELIVERY TIME BY PRODUCT CATEGORY
SELECT
    p.product_category_name,
    AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)) AS avg_delivery_days
FROM 
    olist_orders_dataset o
JOIN
    olist_order_items_dataset oi ON o.order_id = oi.order_id
JOIN 
    olist_products_dataset p ON oi.product_id = p.product_id
WHERE 
    o.order_delivered_customer_date IS NOT NULL
    AND o.order_delivered_customer_date >= o.order_purchase_timestamp
GROUP BY
    p.product_category_name
ORDER BY
    avg_delivery_days;


-- DELIVERY PERFORMANCE BY STATE
SELECT 
    c.customer_state, 
    COUNT(o.order_id) AS total_orders,
    AVG(DATEDIFF(o.order_delivered_customer_date , o.order_estimated_delivery_date)) AS avg_delay
FROM 
	olist_orders_dataset o
JOIN 
	olist_customers_dataset c 
    ON 
		o.customer_id = c.customer_id
WHERE 
	o.order_delivered_customer_date IS NOT NULL
GROUP BY 
	c.customer_state
ORDER BY 
	avg_delay DESC;


----------- PAYMENT AND CUSTOMER PREFERENCES ---------------

-- Most popular payment method
SELECT
	payment_type,
    COUNT(*) AS popular_method
FROM 
	olist_order_payments_dataset
GROUP BY 
	payment_type
ORDER BY popular_method DESC;


-- HIGH ORDER VALUE VS LOW ORDER value
SELECT
	COUNT(CASE WHEN total_value >500 THEN order_id END) AS high_value_orders,
    COUNT(CASE WHEN total_value BETWEEN 100 AND 500 THEN order_id END) AS medium_value_orders,
    COUNT(CASE WHEN total_value < 100 THEN order_id END) AS low_value_orders
FROM(
	SELECT
		order_id,
        SUM(price) AS total_value
	FROM olist_order_items_dataset
    GROUP BY
		order_id
        ) sq;


------------ CUSTOMER FEEDBACK AND REVIEW ANALYSIS -------------------

-- AVERAGE REVIEW SCORE BY PRODUCT CATEGORY
SELECT 
    p.product_category_name, 
    AVG(r.review_score) AS avg_review_score
FROM 
	olist_order_reviews_dataset r
JOIN 
	olist_orders_dataset o 
	ON 
		r.order_id = o.order_id
JOIN 
	olist_order_items_dataset oi 
	ON 
		o.order_id = oi.order_id
JOIN 
	olist_products_dataset p 
    ON 
		oi.product_id = p.product_id
GROUP BY 
	p.product_category_name
ORDER BY 
	avg_review_score DESC;


-- PRODUCT WITH MOST 1 STAR REVIEWS
SELECT 
	p.product_id, 
    pct.product_category_name_english, 
    COUNT(*) AS one_star_count
FROM 
	olist_order_reviews_dataset r
JOIN 
	olist_orders_dataset o 
    ON 
		r.order_id = o.order_id
JOIN 
	olist_order_items_dataset oi 
    ON 
		o.order_id = oi.order_id
JOIN 
	olist_products_dataset p 
    ON 
		oi.product_id = p.product_id
JOIN 
	product_category_translation pct 
    ON 
		p.product_category_name = pct.product_category_name
WHERE 
	r.review_score = 1
GROUP BY 
	p.product_id, pct.product_category_name_english
ORDER BY 
	one_star_count DESC
LIMIT 10;


-------------- SELLER PERFORMANCE --------------------------


-- Total orders by sellers
SELECT 
	seller_id, 
    COUNT(order_id) AS total_orders
FROM 
	olist_order_items_dataset
GROUP BY 
	seller_id
ORDER BY 
	total_orders DESC
LIMIT 5;


-- SELLER WITH MOST RETURN ORDERS
SELECT 
	seller_id, 
	COUNT(o.order_id) AS returned_orders
FROM 
	olist_order_items_dataset oi
JOIN 
	olist_orders_dataset o 
	ON 
		oi.order_id = o.order_id
WHERE 
	o.order_status = 'canceled'
GROUP BY 
	seller_id
ORDER BY 
	returned_orders DESC
LIMIT 5;





