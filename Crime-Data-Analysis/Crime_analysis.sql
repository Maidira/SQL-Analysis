-------------------------------------------------
-- CASE STUDY: Exploring Crime Data--
-------------------------------------------------

-- Tool used: MySQL Workbench

-------------------------------------------------
-- CASE STUDY QUESTIONS AND ANSWERS--
-------------------------------------------------

-- Database Setup
CREATE DATABASE Crime_Dataset;
USE Crime_Dataset;



-- Top 5 most common crimes
SELECT 
`Crm Cd Desc` AS Crime_Type,
COUNT(*) AS Total_Incidents
FROM crime_data
GROUP BY `Crm Cd Desc`
ORDER BY Total_Incidents DESC
LIMIT 5;


-- Crimes trend over time
SELECT 
    YEAR(STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %r')) AS Year,
    MONTH(STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %r')) AS Month,
    COUNT(*) AS Total_Crimes
FROM crime_data
WHERE STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %r') IS NOT NULL
GROUP BY 
    YEAR(STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %r')),
    MONTH(STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %r'))
ORDER BY 
    Year, Month
LIMIT 0, 50000;



-- Arrest Rate by Crime Type
SELECT 
    `Crm Cd Desc`,
    COUNT(*) AS Total_Cases,
    SUM(CASE WHEN `Status Desc` LIKE '%Arrest%' THEN 1 ELSE 0 END) AS Arrests,
    ROUND(100.0 * SUM(CASE WHEN `Status Desc` LIKE '%Arrest%' THEN 1 ELSE 0 END) / COUNT(*), 2) AS Arrest_Rate
FROM crime_data
GROUP BY `Crm Cd Desc`
ORDER BY Arrest_Rate DESC;


-- Day of Week Analysis
SELECT 
    DAYNAME(STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %h:%i:%s %p')) AS Day_of_Week,
    COUNT(*) AS Total_Crimes,
    ROUND(AVG(COUNT(*)) OVER (), 0) AS Avg_Daily_Crimes,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS Percentage_of_Total
FROM crime_data
WHERE STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %h:%i:%s %p') IS NOT NULL
GROUP BY 
    DAYNAME(STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %h:%i:%s %p')),
    DAYOFWEEK(STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %h:%i:%s %p'))
ORDER BY 
    DAYOFWEEK(STR_TO_DATE(`DATE OCC`, '%m/%d/%Y %h:%i:%s %p'));


-- Crime Hotspots by Area
SELECT 
   `AREA NAME`,
    COUNT(*) AS Total_Crimes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS Percentage,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS Area_Rank
FROM crime_data
GROUP BY `AREA NAME`
ORDER BY Total_Crimes DESC;


-- Geospatial Clustering
WITH base_counts AS (
    SELECT 
        ROUND(LAT, 3) AS Lat_Cluster,
        ROUND(LON, 3) AS Lon_Cluster,
        `Crm Cd Desc`,
        `Premis Desc`,
        COUNT(*) AS Crime_Count
    FROM crime_data
    GROUP BY ROUND(LAT, 3), ROUND(LON, 3), `Crm Cd Desc`, `Premis Desc`
),
ranked_crimes AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Lat_Cluster, Lon_Cluster 
            ORDER BY Crime_Count DESC
        ) AS Crime_Rank
    FROM base_counts
)

SELECT 
    Lat_Cluster,
    Lon_Cluster,
    SUM(Crime_Count) AS Total_Crimes,
    MAX(CASE WHEN Crime_Rank = 1 THEN `Crm Cd Desc` END) AS Most_Common_Crime,
    MAX(CASE WHEN Crime_Rank = 1 THEN `Premis Desc` END) AS Most_Common_Premise
FROM ranked_crimes
GROUP BY Lat_Cluster, Lon_Cluster
HAVING Total_Crimes > 10
ORDER BY Total_Crimes DESC;


-- Geospatial Clustering
WITH base_counts AS (
    SELECT 
        ROUND(LAT, 3) AS Lat_Cluster,
        ROUND(LON, 3) AS Lon_Cluster,
        `Crm Cd Desc`,
        `Premis Desc`,
        COUNT(*) AS Crime_Count
    FROM crime_data
    GROUP BY ROUND(LAT, 3), ROUND(LON, 3), `Crm Cd Desc`, `Premis Desc`
),
ranked_crimes AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Lat_Cluster, Lon_Cluster 
            ORDER BY Crime_Count DESC
        ) AS Crime_Rank
    FROM base_counts
)

SELECT 
    Lat_Cluster,
    Lon_Cluster,
    SUM(Crime_Count) AS Total_Crimes,
    MAX(CASE WHEN Crime_Rank = 1 THEN `Crm Cd Desc` END) AS Most_Common_Crime,
    MAX(CASE WHEN Crime_Rank = 1 THEN `Premis Desc` END) AS Most_Common_Premise
FROM ranked_crimes
GROUP BY Lat_Cluster, Lon_Cluster
HAVING Total_Crimes > 10
ORDER BY Total_Crimes DESC;


-- Geospatial Clustering
WITH base_counts AS (
    SELECT 
        ROUND(LAT, 3) AS Lat_Cluster,
        ROUND(LON, 3) AS Lon_Cluster,
        `Crm Cd Desc`,
        `Premis Desc`,
        COUNT(*) AS Crime_Count
    FROM crime_data
    GROUP BY ROUND(LAT, 3), ROUND(LON, 3), `Crm Cd Desc`, `Premis Desc`
),
ranked_crimes AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Lat_Cluster, Lon_Cluster 
            ORDER BY Crime_Count DESC
        ) AS Crime_Rank
    FROM base_counts
)

SELECT 
    Lat_Cluster,
    Lon_Cluster,
    SUM(Crime_Count) AS Total_Crimes,
    MAX(CASE WHEN Crime_Rank = 1 THEN `Crm Cd Desc` END) AS Most_Common_Crime,
    MAX(CASE WHEN Crime_Rank = 1 THEN `Premis Desc` END) AS Most_Common_Premise
FROM ranked_crimes
GROUP BY Lat_Cluster, Lon_Cluster
HAVING Total_Crimes > 10
ORDER BY Total_Crimes DESC;


-- Geospatial Clustering
WITH base_counts AS (
    SELECT 
        ROUND(LAT, 3) AS Lat_Cluster,
        ROUND(LON, 3) AS Lon_Cluster,
        `Crm Cd Desc`,
        `Premis Desc`,
        COUNT(*) AS Crime_Count
    FROM crime_data
    GROUP BY ROUND(LAT, 3), ROUND(LON, 3), `Crm Cd Desc`, `Premis Desc`
),
ranked_crimes AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY Lat_Cluster, Lon_Cluster 
            ORDER BY Crime_Count DESC
        ) AS Crime_Rank
    FROM base_counts
)

SELECT 
    Lat_Cluster,
    Lon_Cluster,
    SUM(Crime_Count) AS Total_Crimes,
    MAX(CASE WHEN Crime_Rank = 1 THEN `Crm Cd Desc` END) AS Most_Common_Crime,
    MAX(CASE WHEN Crime_Rank = 1 THEN `Premis Desc` END) AS Most_Common_Premise
FROM ranked_crimes
GROUP BY Lat_Cluster, Lon_Cluster
HAVING Total_Crimes > 10
ORDER BY Total_Crimes DESC;


-- Case Clearance Rates
SELECT 
    `AREA NAME`,
    `Crm Cd Desc`,
    COUNT(*) AS Total_Cases,
    SUM(CASE WHEN `Status Desc` LIKE '%Arrest%' THEN 1 ELSE 0 END) AS Cleared_Cases,
    ROUND(100.0 * SUM(CASE WHEN `Status Desc` LIKE '%Arrest%' THEN 1 ELSE 0 END) / COUNT(*), 2) AS Clearance_Rate,
    ROUND(AVG(`Vict Age`)) AS Avg_Victim_Age
FROM crime_data
GROUP BY `AREA NAME`, `Crm Cd Desc`
HAVING COUNT(*) > 20
ORDER BY Clearance_Rate DESC;


-- Weapon Usage Trends
WITH weapon_usage AS (
    SELECT 
        `Weapon Desc`,
        `Crm Cd Desc`,
        `AREA NAME`,
        COUNT(*) AS Usage_Count
    FROM crime_data
    WHERE `Weapon Desc` IS NOT NULL
    GROUP BY `Weapon Desc`, `Crm Cd Desc`, `AREA NAME`
),
ranked_usage AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY `Weapon Desc` ORDER BY Usage_Count DESC) AS crime_rank,
        SUM(Usage_Count) OVER () AS Total_Weapons
    FROM weapon_usage
)
SELECT 
    `Weapon Desc`,
    `Crm Cd Desc` AS Most_Common_Crime,
    `AREA NAME` AS Most_Common_Area,
    Usage_Count,
    ROUND(100.0 * Usage_Count / Total_Weapons, 3) AS Percentage_of_Total
FROM ranked_usage
WHERE crime_rank = 1
ORDER BY Usage_Count DESC;


-- Weapon Effectiveness (Clearance Rates)
SELECT 
    `Weapon Desc`,
    COUNT(*) AS Total_Crimes,
    SUM(CASE WHEN `Status Desc` LIKE '%Arrest%' THEN 1 ELSE 0 END) AS Cleared_Cases,
    ROUND(100.0 * SUM(CASE WHEN `Status Desc` LIKE '%Arrest%' THEN 1 ELSE 0 END) / COUNT(*), 2) AS Clearance_Rate
FROM crime_data
WHERE `Weapon Desc` IS NOT NULL
GROUP BY `Weapon Desc`
HAVING Total_Crimes > 20
ORDER BY Clearance_Rate DESC;


