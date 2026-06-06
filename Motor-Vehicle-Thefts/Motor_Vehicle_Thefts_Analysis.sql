------------------------------------------
-- CASE STUDY: Motor Theft Vehicles--
------------------------------------------

-- Tool used: MySQL Workbench

-----------------------------------------
-- CASE STUDY QUESTIONS AND ANSWERS--
-----------------------------------------

-- 1. What is the age of the vehicles that are stolen? 

SELECT model_year, date_stolen,
      YEAR(date_stolen) - model_year AS age_at_theft
FROM stolen_vehicles;
    
    
-- 2. What is the average age of the vehicles that are stolen? Does this vary based on the vehicle type?

SELECT
    AVG(YEAR(date_stolen) - model_year) AS average_age
FROM stolen_vehicles;


-- 3. What is the average age of the vehicles that are stolen based on Vehicle Types?

SELECT vehicle_type,
      AVG(YEAR(date_stolen) - model_year) AS average_age
FROM stolen_vehicles
GROUP BY vehicle_type
ORDER BY average_age DESC;


-- 4. Which regions have the most and least number of stolen vehicles? What are the characteristics of the regions?
     
SELECT  l.region, l.population, l.density,COUNT(DISTINCT vehicle_id) AS no_of_vehicles
FROM   stolen_vehicles s 
LEFT JOIN locations l 
ON s.location_id = l.location_id
GROUP BY l.region
ORDER BY no_of_vehicles DESC;


-- 5. On which date most vehicles were stolen and from which region

SELECT s.date_stolen, COUNT(vehicle_id), l.region
FROM   stolen_vehicles s 
LEFT JOIN locations l 
ON s.location_id = l.location_id
GROUP BY s.date_stolen, l.region
ORDER BY COUNT(vehicle_id) DESC;


-- 6. On which date most number of vehicles were stolen for each region

with cte AS (
SELECT s.date_stolen, COUNT(s.vehicle_id) AS vehicle_count, l.region
FROM stolen_vehicles s
LEFT JOIN locations l 
ON s.location_id = l.location_id
GROUP BY s.date_stolen, l.region
)

SELECT date_stolen, MAX(vehicle_count) AS max_vehicle_count, region
FROM cte
GROUP BY region;


 -- 7. Numbers of vehicles stolen as per make_name

 SELECT m.make_name, COUNT(s.vehicle_id) AS no_of_vehicle
 FROM stolen_vehicles s
 JOIN make_details m
 ON s.make_id = m.make_id
 GROUP BY m.make_name
 ORDER BY no_of_vehicle DESC;


