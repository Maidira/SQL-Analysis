--------------------------------------------
-- CASE STUDY: Restaurant Order Analysis --
--------------------------------------------

-- Tool used: MySQL Workbench

--------------------------------------------
-- CASE STUDY QUESTIONS AND ANSWERS--
--------------------------------------------



-------------------------------------
-- Explore the Menu Table --
-------------------------------------

-- 1. View the menu_items table.

SELECT * FROM menu_items;


-- 2. Find the number of items on the menu.

SELECT COUNT(DISTINCT menu_item_id) AS no_of_items
FROM menu_items;


-- 3. What are the least and most expensive items on the menu?

-- Least Expensive
SELECT
  item_name,
  price
FROM
  menu_items
ORDER BY
  price
LIMIT 1;

-- Most Expensive
SELECT
  item_name,
  price
FROM
  menu_items
ORDER BY
  price DESC
LIMIT 1;


-- 4. How many Italian dishes are on the menu

SELECT COUNT(category) AS no_italian_dishes
FROM menu_items
WHERE category = "Italian";


-- 5. What are the least and most expensive Italian dishes on the menu?

-- Least Expensive
SELECT
  item_name,
  price
FROM
  menu_items
WHERE category = "Italian"
ORDER BY
  price
LIMIT 1;

-- Most Expensive
SELECT
  item_name,
  price
FROM
  menu_items
WHERE category = "Italian"
ORDER BY
  price DESC
LIMIT 1;


-- 6. How many dishes are in each category?

SELECT category, COUNT(DISTINCT item_name) no_of_dishes
FROM menu_items
GROUP BY category;


-- 7. What is the average dish price within each category?

SELECT category, ROUND(AVG(price),2) AS avg_price
FROM menu_items
GROUP BY category;



-------------------------------------
-- Explore the Orders Table --
-------------------------------------

-- 8. view the order_detail table.

SELECT * FROM order_details;


-- 9. What is the data range of the table?

SELECT 
	MIN(order_date) AS min_date, MAX(order_date) AS max_date
FROM order_details;


-- 10. How many orders were made within this date range?

SELECT 
	COUNT(DISTINCT order_id) AS no_orders
FROM order_details;


-- 11. How many items were ordered within this date range?

SELECT 
	COUNT(DISTINCT order_id) AS no_orders
FROM order_details;


-- 12. Which orders had the most number of items?

WITH OrderItemCounts AS (
    SELECT order_id, COUNT(item_id) AS no_items
    FROM order_details
    GROUP BY order_id
)

SELECT order_id, no_items
FROM OrderItemCounts
WHERE no_items = (SELECT MAX(no_items) FROM OrderItemCounts);


-- 13. How many orders had more than 12 items?

SELECT COUNT(*) AS order_count
FROM
(SELECT order_id , COUNT(item_id) AS no_items
FROM order_details
GROUP BY order_id
HAVING no_items > 12) AS num_orders;



-------------------------------------
-- Analyze Customer Behaviour --
-------------------------------------

-- 14. Combine the menu_items and order_details tables into a single table.

SELECT *
FROM order_details o 
LEFT JOIN menu_items m
ON o.item_id = m.menu_item_id;


-- 15. What were the least and most ordered items? What categories were they in?

-- Least ordered
SELECT item_name, COUNT(order_details_id) AS no_orders
FROM order_details o 
LEFT JOIN menu_items m
ON o.item_id = m.menu_item_id
GROUP BY item_name
ORDER BY no_orders 
LIMIT 1;

-- Most Ordered
SELECT item_name, COUNT(order_details_id) AS no_orders
FROM order_details o 
LEFT JOIN menu_items m
ON o.item_id = m.menu_item_id
GROUP BY item_name
ORDER BY no_orders DESC
LIMIT 1;

-- Category they in?

SELECT category, item_name, COUNT(order_details_id) AS no_orders
FROM order_details o 
LEFT JOIN menu_items m
ON o.item_id = m.menu_item_id
GROUP BY item_name,category
ORDER BY no_orders 
LIMIT 1;


SELECT category, item_name, COUNT(order_details_id) AS no_orders
FROM order_details o 
LEFT JOIN menu_items m
ON o.item_id = m.menu_item_id
GROUP BY item_name,category
ORDER BY no_orders DESC
LIMIT 1;


-- 16. What were the top 5 orders that spent the most money?

 SELECT  
	o.order_id , SUM(price) AS money_spent
FROM order_details o 
LEFT JOIN menu_items m
ON 
	o.item_id = m.menu_item_id
GROUP BY o.order_id
ORDER BY money_spent DESC
LIMIT 5;


-- 17. View the details of the highest send order. What insights can you gather from the results?

SELECT  
	SUM(m.price) AS money_spent, COUNT(o.item_id) AS no_item, m.category
FROM order_details o 
LEFT JOIN menu_items m
ON 
	o.item_id = m.menu_item_id
WHERE order_ID = 440    
GROUP BY  m.category
ORDER BY money_spent DESC;


-- 18. View the details of the to 5 highest spend orders. What insights can you gather from the results?

SELECT  
	SUM(m.price) AS money_spent, COUNT(o.item_id) AS no_item, m.category
FROM order_details o 
LEFT JOIN menu_items m
ON 
	o.item_id = m.menu_item_id
WHERE order_ID IN (440, 2075, 1957, 330, 2675)    
GROUP BY  o.order_id, m.category
ORDER BY money_spent DESC;


















