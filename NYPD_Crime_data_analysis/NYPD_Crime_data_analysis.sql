------------------------------------------
-- CREATING TABLES AND CLEANING DATA --
------------------------------------------


-- Create new Database
CREATE DATABASE nypd_crime_database;


-- DROP database nypd_crime_database;
USE nypd_crime_database;


-- Creates a temporary table to hold the raw CSV data
DROP TABLE IF EXISTS nypd_complaints_staging;


CREATE TABLE nypd_complaints_staging (
    CMPLNT_NUM VARCHAR(255),
    CMPLNT_FR_DT VARCHAR(255),
    CMPLNT_FR_TM VARCHAR(255),
    CMPLNT_TO_DT VARCHAR(255),
    CMPLNT_TO_TM VARCHAR(255),
    ADDR_PCT_CD VARCHAR(255),
    RPT_DT VARCHAR(255),
    KY_CD VARCHAR(255),
    OFNS_DESC VARCHAR(255),
    PD_CD VARCHAR(255),
    PD_DESC VARCHAR(255),
    CRM_ATPT_CPTD_CD VARCHAR(255),
    LAW_CAT_CD VARCHAR(255),
    BORO_NM VARCHAR(255),
    LOC_OF_OCCUR_DESC VARCHAR(255),
    PREM_TYP_DESC VARCHAR(255),
    JURIS_DESC VARCHAR(255),
    JURISDICTION_CODE VARCHAR(255),
    PARKS_NM VARCHAR(255),
    HADEVELOPT VARCHAR(255),
    HOUSING_PSA VARCHAR(255),
    X_COORD_CD VARCHAR(255),
    Y_COORD_CD VARCHAR(255),
    SUSP_AGE_GROUP VARCHAR(255),
    SUSP_RACE VARCHAR(255),
    SUSP_SEX VARCHAR(255),
    TRANSIT_DISTRICT VARCHAR(255),
    Latitude VARCHAR(255),
    Longitude VARCHAR(255),
    Lat_Lon VARCHAR(255),
    PATROL_BORO VARCHAR(255),
    STATION_NAME VARCHAR(255),
    VIC_AGE_GROUP VARCHAR(255),
    VIC_RACE VARCHAR(255),
    VIC_SEX VARCHAR(255)
);


-- Loads the CSV into the staging table. Update the path if yours is different.
LOAD DATA INFILE 'C:/Uploads/NYPD_Complaint_Data_Historic.csv'
INTO TABLE nypd_complaints_staging
CHARACTER SET latin1
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


-- Creates final, permanent table with the correct data types
DROP TABLE IF EXISTS nypd_complaints;

CREATE TABLE nypd_complaints (
    CMPLNT_NUM INTEGER PRIMARY KEY,
    CMPLNT_FR_DT DATE,
    CMPLNT_FR_TM TIME,
    CMPLNT_TO_DT DATE,
    CMPLNT_TO_TM TIME,
    ADDR_PCT_CD INTEGER,
    RPT_DT DATE,
    KY_CD INTEGER,
    OFNS_DESC VARCHAR(255),
    PD_CD INTEGER,
    PD_DESC VARCHAR(255),
    CRM_ATPT_CPTD_CD VARCHAR(20),
    LAW_CAT_CD VARCHAR(20),
    BORO_NM VARCHAR(50),
    LOC_OF_OCCUR_DESC VARCHAR(255),
    PREM_TYP_DESC VARCHAR(255),
    JURIS_DESC VARCHAR(255),
    JURISDICTION_CODE INTEGER,
    PARKS_NM VARCHAR(255),
    HADEVELOPT VARCHAR(255),
    HOUSING_PSA INTEGER,
    X_COORD_CD INTEGER,
    Y_COORD_CD INTEGER,
    SUSP_AGE_GROUP VARCHAR(50),
    SUSP_RACE VARCHAR(50),
    SUSP_SEX CHAR(1),
    TRANSIT_DISTRICT INTEGER,
    Latitude NUMERIC(10, 8),
    Longitude NUMERIC(11, 8),
    Lat_Lon VARCHAR(255),
    PATROL_BORO VARCHAR(255),
    STATION_NAME VARCHAR(255),
    VIC_AGE_GROUP VARCHAR(50),
    VIC_RACE VARCHAR(50),
    VIC_SEX CHAR(1)
);


-- Checking data
SELECT COUNT(*) FROM nypd_complaints;


-- Checking data
SELECT COUNT(*) FROM nypd_complaints_staging;


--  TEST query to see the output.
SELECT
    CMPLNT_NUM,
    STR_TO_DATE(NULLIF(CMPLNT_FR_DT, ''), '%m/%d/%Y') AS Converted_Complaint_Date,
    STR_TO_DATE(NULLIF(RPT_DT, ''), '%m/%d/%Y') AS Converted_Report_Date,
    NULLIF(ADDR_PCT_CD, '') AS Precinct_Code
FROM
    nypd_complaints_staging
WHERE
    CMPLNT_FR_DT IS NOT NULL AND CMPLNT_FR_DT != ''
LIMIT 10;

SELECT CMPLNT_NUM, COUNT(*)
FROM nypd_complaints_staging
GROUP BY CMPLNT_NUM
HAVING COUNT(*) > 1;


-- Deleting existing data and updating with correct format in our main table
TRUNCATE TABLE nypd_complaints;

INSERT IGNORE INTO nypd_complaints
SELECT
    NULLIF(CMPLNT_NUM, ''),
    STR_TO_DATE(NULLIF(CMPLNT_FR_DT, ''), '%m/%d/%Y'),
    NULLIF(CMPLNT_FR_TM, ''),
    STR_TO_DATE(NULLIF(CMPLNT_TO_DT, ''), '%m/%d/%Y'),
    NULLIF(CMPLNT_TO_TM, ''),
    CASE WHEN ADDR_PCT_CD REGEXP '^[0-9]+$' THEN ADDR_PCT_CD ELSE NULL END,
    STR_TO_DATE(NULLIF(RPT_DT, ''), '%m/%d/%Y'),
    CASE WHEN KY_CD REGEXP '^[0-9]+$' THEN KY_CD ELSE NULL END,
    NULLIF(OFNS_DESC, ''),
    CASE WHEN PD_CD REGEXP '^[0-9]+$' THEN PD_CD ELSE NULL END,
    NULLIF(PD_DESC, ''),
    NULLIF(CRM_ATPT_CPTD_CD, ''),
    NULLIF(LAW_CAT_CD, ''),
    NULLIF(BORO_NM, ''),
    NULLIF(LOC_OF_OCCUR_DESC, ''),
    NULLIF(PREM_TYP_DESC, ''),
    NULLIF(JURIS_DESC, ''),
    CASE WHEN JURISDICTION_CODE REGEXP '^[0-9]+$' THEN JURISDICTION_CODE ELSE NULL END,
    NULLIF(PARKS_NM, ''),
    NULLIF(HADEVELOPT, ''),
    CASE WHEN HOUSING_PSA REGEXP '^[0-9]+$' THEN HOUSING_PSA ELSE NULL END,
    CASE WHEN X_COORD_CD REGEXP '^[0-9]+$' THEN X_COORD_CD ELSE NULL END,
    CASE WHEN Y_COORD_CD REGEXP '^[0-9]+$' THEN Y_COORD_CD ELSE NULL END,
    NULLIF(SUSP_AGE_GROUP, ''),
    NULLIF(SUSP_RACE, ''),
    NULLIF(SUSP_SEX, ''),
    CASE WHEN TRANSIT_DISTRICT REGEXP '^[0--9]+$' THEN TRANSIT_DISTRICT ELSE NULL END,
    CASE WHEN Latitude REGEXP '^[0-9.-]+$' THEN Latitude ELSE NULL END,
    CASE WHEN Longitude REGEXP '^[0-9.-]+$' THEN Longitude ELSE NULL END,
    NULLIF(Lat_Lon, ''),
    NULLIF(PATROL_BORO, ''),
    NULLIF(STATION_NAME, ''),
    NULLIF(VIC_AGE_GROUP, ''),
    NULLIF(VIC_RACE, ''),
    NULLIF(VIC_SEX, '')
FROM nypd_complaints_staging;


SELECT COUNT(*) FROM nypd_complaints;


-- trimming spaces and converting all text columns to upper to avoid text mismatch issue
UPDATE nypd_complaints
SET
    OFNS_DESC = TRIM(UPPER(OFNS_DESC)),
    LAW_CAT_CD = TRIM(UPPER(LAW_CAT_CD)),
    BORO_NM = TRIM(UPPER(BORO_NM)),
    PREM_TYP_DESC = TRIM(UPPER(PREM_TYP_DESC)),
    PD_DESC = TRIM(UPPER(PD_DESC)),
    LOC_OF_OCCUR_DESC = TRIM(UPPER(LOC_OF_OCCUR_DESC)),
    SUSP_RACE = TRIM(UPPER(SUSP_RACE)),
    SUSP_SEX = TRIM(UPPER(SUSP_SEX)),
    VIC_RACE = TRIM(UPPER(VIC_RACE)),
    VIC_SEX = TRIM(UPPER(VIC_SEX)),
    SUSP_AGE_GROUP = TRIM(UPPER(SUSP_AGE_GROUP)),
    VIC_AGE_GROUP = TRIM(UPPER(VIC_AGE_GROUP));
	
    
-- Replacing null values with unknown in categorical column
UPDATE nypd_complaints
SET
    SUSP_RACE = COALESCE(SUSP_RACE, 'UNKNOWN'),
    VIC_RACE = COALESCE(VIC_RACE, 'UNKNOWN'),
    SUSP_SEX = COALESCE(SUSP_SEX, 'U'), 
    VIC_SEX = COALESCE(VIC_SEX, 'U'),
    SUSP_AGE_GROUP = COALESCE(SUSP_AGE_GROUP, 'UNKNOWN'),
    VIC_AGE_GROUP = COALESCE(VIC_AGE_GROUP, 'UNKNOWN'),
    ADDR_PCT_CD = COALESCE(ADDR_PCT_CD, NULL),
    KY_CD = COALESCE(KY_CD, NULL),
    JURISDICTION_CODE = COALESCE(JURISDICTION_CODE, NULL),
    TRANSIT_DISTRICT = COALESCE(TRANSIT_DISTRICT, NULL);


-- Extracting date and time from existing columns
ALTER TABLE nypd_complaints
ADD COLUMN complaint_year INTEGER,
ADD COLUMN complaint_month INTEGER,
ADD COLUMN complaint_day_of_week VARCHAR(20), 
ADD COLUMN complaint_hour INTEGER;

UPDATE nypd_complaints
SET
    complaint_year = YEAR(CMPLNT_FR_DT),
    complaint_month = MONTH(CMPLNT_FR_DT),
    complaint_day_of_week = DAYNAME(CMPLNT_FR_DT), 
    complaint_hour = HOUR(CMPLNT_FR_TM);
    
    
-- Add a column for incident duration
ALTER TABLE nypd_complaints 
ADD COLUMN incident_duration_minutes INT;


-- Populate the incident_duration_minutes column
UPDATE nypd_complaints
SET incident_duration_minutes =
    CASE
        WHEN CMPLNT_FR_DT IS NOT NULL 
             AND CMPLNT_TO_DT IS NOT NULL 
             AND CMPLNT_FR_TM IS NOT NULL 
             AND CMPLNT_TO_TM IS NOT NULL
        THEN TIMESTAMPDIFF(
                MINUTE,
                CAST(CONCAT(CMPLNT_FR_DT, ' ', CMPLNT_FR_TM) AS DATETIME),
                CAST(CONCAT(CMPLNT_TO_DT, ' ', CMPLNT_TO_TM) AS DATETIME)
             )
        ELSE NULL
    END;

    
    
SELECT * FROM nypd_complaints;

   
-- Creating age group for victims and suspect
UPDATE nypd_complaints
SET
    VIC_AGE_GROUP = CASE
        WHEN VIC_AGE_GROUP IN ('<18', '18-24', '25-44', '45-64', '65+')
        THEN VIC_AGE_GROUP 
        ELSE 'UNKNOWN'   
    END,

    SUSP_AGE_GROUP = CASE
        WHEN SUSP_AGE_GROUP IN ('<18', '18-24', '25-44', '45-64', '65+')
        THEN SUSP_AGE_GROUP
        ELSE 'UNKNOWN'
    END;




------------------------------------------
-- EXPLORATORY DATA ANAWLYSIS --
------------------------------------------


-- Crime trend over the years
SELECT
	complaint_year,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM 
	nypd_complaints
WHERE
	complaint_year IS NOT NULL
GROUP BY
	complaint_year
ORDER BY
	complaint_year ASC;
    
    
-- Crime by severity
SELECT
	LAW_CAT_CD AS crime_severity,
    COUNT(CMPLNT_NUM) number_of_complaints,
      (COUNT(CMPLNT_NUM) * 100.0 / (SELECT COUNT(*) FROM nypd_complaints)) AS percentage
FROM
    nypd_complaints
WHERE
    LAW_CAT_CD IS NOT NULL AND LAW_CAT_CD != ''
GROUP BY
    LAW_CAT_CD
ORDER BY
    number_of_complaints DESC;
    
    
-- Which town have the most reported crime
SELECT 
	BORO_NM AS borough,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM 
	nypd_complaints
WHERE
	BORO_NM IS NOT NULL
    AND
    BORO_NM != ""
GROUP BY
	BORO_NM
ORDER BY
	number_of_complaints DESC;
    
    
-- Top 10 most common offenses
SELECT 
	OFNS_DESC AS offense_description,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM 
	nypd_complaints
WHERE 
	OFNS_DESC IS NOT NULL
    AND
    OFNS_DESC != ""
GROUP BY
	OFNS_DESC
ORDER BY
	number_of_complaints DESC
LIMIT 10;
    
    
 -- At what time of day most crime occurs
 SELECT
    complaint_hour,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM
    nypd_complaints
WHERE
    complaint_hour IS NOT NULL
GROUP BY
    complaint_hour
ORDER BY
    complaint_hour ASC;
    
    
-- Crime severity break down by town    
SELECT
	BORO_NM AS borugh,
    LAW_CAT_CD AS crime_severiity,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM 
	nypd_complaints
WHERE
	BORO_NM IS NOT NULL AND BORO_NM != ""
    AND 
    LAW_CAT_CD IS NOT NULL AND LAW_CAT_CD != ""
GROUP BY
	LAW_CAT_CD, BORO_NM
ORDER BY
	BORO_NM, number_of_complaints DESC;
    
    
--   Top 5 offenses in Brooklyn
SELECT 
	OFNS_DESC AS offense_description,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM
	nypd_complaints
WHERE
	BORO_NM  = "BROOKLYN"
    AND
    OFNS_DESC IS NOT NULL AND OFNS_DESC != ""
GROUP BY
	offense_description
ORDER BY
	number_of_complaints DESC;
    
    
--  Crime pattern Weekend vs weekdays
SELECT
	CASE
		WHEN complaint_day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
	END AS day_type,
    LAW_CAT_CD AS crime_severity,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM 
	nypd_complaints
WHERE
	complaint_day_of_week IS NOT NULL
    AND
    LAW_CAT_CD IS NOT NULL AND LAW_CAT_CD != ""
GROUP BY
	day_type, crime_severity
ORDER BY
	day_type, number_of_complaints DESC;
    
    
-- Hourly pattern for Grand Larceny
SELECT
	complaint_hour,
    COUNT(CMPLNT_NUM) AS number_of_larcenies
FROM
	nypd_complaints
WHERE
	OFNS_DESC = "GRAND LARCENY"
    AND
    CMPLNT_NUM IS NOT NULL
GROUP BY
	complaint_hour
ORDER BY
	complaint_hour ASC;
    
    
-- Most common victim age group from ROBBERY     
SELECT
	VIC_AGE_GROUP,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM 
	nypd_complaints
WHERE 
	OFNS_DESC = "ROBBERY"
    AND
    CMPLNT_NUM IS NOT NULL 
    AND
    VIC_AGE_GROUP IS NOT NULL
GROUP BY
	VIC_AGE_GROUP
ORDER BY
	number_of_complaints;
    
    
-- Most common offenses in each town    
WITH offenseCounts AS (
	SELECT
		BORO_NM,
        OFNS_DESC,
        COUNT(CMPLNT_NUM) as number_of_complaints
FROM
	nypd_complaints
WHERE
	BORO_NM IS NOT NULL AND BORO_NM != ""
GROUP BY
	BORO_NM, OFNS_DESC
),
rankedOffenses AS (
	SELECT
	BORO_NM,
    OFNS_DESC,
    number_of_complaints,
    RANK() OVER(PARTITION BY BORO_NM ORDER BY number_of_complaints DESC) AS offense_rank
    FROM offenseCounts
)
SELECT 
	BORO_NM AS borough,
    OFNS_DESC as offense,
    number_of_complaints,
    offense_rank
FROM
	rankedOffenses
WHERE
	offense_rank <=3
ORDER BY
	borough,offense_rank;
    
    
-- Crime count change year over year   
WITH yearlyCounts AS(
	SELECT
		complaint_year,
        COUNT(CMPLNT_NUM) AS number_of_complaints
	FROM 
		nypd_complaints
	WHERE 
		complaint_year IS NOT NULL AND complaint_year BETWEEN 2006 AND 2024
	GROUP BY
		complaint_year
),
yoyComparison AS(
	SELECT
		complaint_year,
        number_of_complaints,
        LAG(number_of_complaints,1,0) OVER(ORDER BY complaint_year) AS previous_year_complaints
	FROM
		yearlyCounts
)
SELECT 
	complaint_year,
    number_of_complaints,
	previous_year_complaints,
    CASE
		WHEN previous_year_complaints = 0 THEN NULL
        ELSE (number_of_complaints - previous_year_complaints) * 100.0/ previous_year_complaints
	END AS percentage_change
FROM 
	yoyComparison
ORDER BY 
	complaint_year ASC;
    
        
-- Monthly running total of complaints for the covid19 start year(2020)
WITH MonthlyCounts AS (
    SELECT
        complaint_month,
        COUNT(CMPLNT_NUM) AS monthly_complaints
    FROM
        nypd_complaints
    WHERE
        complaint_year = 2020 
    GROUP BY
        complaint_month
)
SELECT
    complaint_month,
    monthly_complaints,
    SUM(monthly_complaints) OVER (ORDER BY complaint_month ASC) AS running_total_complaints
FROM
    MonthlyCounts
ORDER BY
    complaint_month;
    
    
-- Crime patterns by time of day segments
WITH complaintTimeSegment AS (
	SELECT
		CMPLNT_NUM,
        CASE
			WHEN complaint_hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN complaint_hour BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN complaint_hour BETWEEN 17 AND 20 THEN 'Evening'
            WHEN complaint_hour IS NULL THEN 'unknown'
            ELSE 'Night'
		END AS time_of_day
	FROM
		nypd_complaints
)
SELECT
	time_of_day,
    COUNT(CMPLNT_NUM) AS number_of_complaints
FROM
	complaintTimeSegment
GROUP BY
	time_of_day
ORDER BY number_of_complaints;
    
    
--  Average time between report and occurrence
SELECT
	LAW_CAT_CD,
    AVG(DATEDIFF(RPT_DT, CMPLNT_FR_DT)) AS avg_days_of_report
FROM
	nypd_complaints
WHERE
	RPT_DT >= CMPLNT_FR_DT
GROUP BY
	LAW_CAT_CD 
ORDER BY
	avg_days_of_report DESC;
    






















