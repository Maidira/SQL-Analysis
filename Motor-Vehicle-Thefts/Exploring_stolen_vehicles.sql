------------------------------------------
-- CASE STUDY: Exploring Stolen Vehicles--
------------------------------------------

-- Tool used: MySQL Workbench

-----------------------------------------
-- CASE STUDY QUESTIONS AND ANSWERS--
-----------------------------------------


-- 1. View the stolen vehicles table.

SELECT * FROM stolen_vehicles;

-- 2. The number of vehicles stolen.

SELECT COUNT(vehicle_id) AS no_of_vehicle 
FROM stolen_vehicles;


-- 3. Type of vehicles stolen.

SELECT DISTINCT vehicle_type AS types_of_vehicle
FROM stolen_vehicles;


-- 4. The maximum number of vehicles stolen as per color.

SELECT color, COUNT(color) AS no_of_vehicles
FROM stolen_vehicles
GROUP BY color;


-- 5. The oldest model-year car was stolen.

SELECT MIN(model_year)
FROM stolen_vehicles;


-- 6. Top 5 model_year vehicles stolen.

SELECT model_year, COUNT(model_year) AS Vehicles_stolen
FROM stolen_vehicles
GROUP BY model_year
ORDER BY vehicles_stolen DESC
LIMIT 5;


-- 7. Top 5 number of vehicles mostly stolen as per vehicle description.

SELECT vehicle_desc , COUNT(vehicle_desc) AS no_of_vehicle
FROM stolen_vehicles
GROUP BY vehicle_desc
ORDER BY no_of_vehicle DESC
LIMIT 5;


-- 8. Top 5 Number of vehicles least stolen as per vehicle description.

SELECT vehicle_desc , COUNT(vehicle_desc) AS no_of_vehicle
FROM stolen_vehicles
GROUP BY vehicle_desc
ORDER BY no_of_vehicle 
LIMIT 5;


-- 9. In which month maximum number of vehicles stolen

SELECT COUNT(vehicle_id) AS no_of_vehicle, MONTHNAME(date_stolen) AS stolen_month
FROM stolen_vehicles
GROUP BY stolen_month
ORDER BY no_of_vehicle DESC
LIMIT 1;


-- 10. In which month Least number of vehicles stolen

SELECT COUNT(vehicle_id) AS no_of_vehicle, MONTHNAME(date_stolen) AS stolen_month
FROM stolen_vehicles
GROUP BY stolen_month
ORDER BY no_of_vehicle 
LIMIT 1;


-- 11. What day of the week are vehicles most often and least often stolen?

-- most often stolen
SELECT DAYNAME(date_stolen) AS day_of_week, COUNT(vehicle_id) AS no_of_vehicle
FROM stolen_vehicles
GROUP BY day_of_week
ORDER BY no_of_vehicle DESC
LIMIT 1;

-- least often stolen
SELECT DAYNAME(date_stolen) AS day_of_week, COUNT(vehicle_id) AS no_of_vehicle
FROM stolen_vehicles
GROUP BY day_of_week
ORDER BY no_of_vehicle
LIMIT 1;


