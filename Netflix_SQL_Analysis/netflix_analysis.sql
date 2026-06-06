------------------------------------------
-- CASE STUDY: Exploring Netflix Data--
------------------------------------------

-- Tool used: PostgreSQL

-----------------------------------------
-- CASE STUDY QUESTIONS AND ANSWERS--
------------------------------------

--1. Count the number of Movies and TV shows
SELECT 
	type,
	COUNT(type)
FROM netflix
GROUP BY type;


--2. Find the most common rating for movies and TV shows
SELECT
	type,
	rating
FROM
	(
	SELECT
		type,
		rating,
		COUNT(*) AS rating_count,
		RANK() OVER(PARTITION BY type ORDER BY COUNT(*) DESC) AS rk
	FROM netflix
	GROUP BY type, rating
	) AS t1
WHERE rk =1;


--3. List all movies released in a specific year
SELECT 
	release_year,
	COUNT(type)
FROM netflix
WHERE type = 'Movie'
GROUP BY release_year
ORDER BY 2 DESC;


--4. Find the top 5 countries with the most content on Netflix
SELECT 
	TRIM(UNNEST(STRING_TO_ARRAY(country, ','))) AS new_country,
	COUNT(show_id) as total_content
FROM netflix
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;


--5. Identify the longest movie
SELECT * 
FROM
 (SELECT DISTINCT title AS movie,
  split_part(duration,' ',1):: numeric AS duration 
	FROM netflix
  WHERE TYPE ='Movie') AS subquery
WHERE 
	duration = (SELECT MAX(split_part(duration,' ',1):: numeric ) FROM netflix)


--6. Find Content added in the last 5 years
SELECT *
FROM netflix
WHERE 
	TO_DATE(date_added, 'Month DD, YYYY') >= CURRENT_DATE - INTERVAL '5 years';


--7. Find all the movies/TV shows by director "Martin Scorsese"
SELECT * 
FROM netflix 
WHERE director ILIKE '%Martin Scorsese%';


--8. List all TV shows with more than 5 seasons
SELECT *
FROM netflix
WHERE 
	type = 'TV Show'
	AND SPLIT_PART(duration,' ',1):: numeric > 5


--9. Count the number of content items in each genre
SELECT 
	TRIM(UNNEST(STRING_TO_ARRAY(listed_in, ','))) AS genre,
	COUNT(show_id) AS total_content
FROM netflix
GROUP BY 1
ORDER BY 2 DESC;


--10. Find the number of content released in India on Netflix each year. Return top 5 years.
SELECT
	EXTRACT(YEAR FROM TO_DATE(date_added,'Month DD, YYYY')) AS year,
	COUNT(*)
FROM netflix
WHERE country = 'India'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;


--11. List the number of documentaries
SELECT *
FROM netflix
WHERE listed_in ILIKE '%Documentaries%';


--12. Find all the content without Director.
SELECT *
FROM netflix
WHERE director IS NULL


--13. Find how many movies actor 'Leonardo Dicaprio' appeared in last 10 years
SELECT *
FROM netflix
WHERE 
	casts ILIKE '%Leonardo Dicaprio%'
	AND
	release_year > EXTRACT(YEAR FROM CURRENT_DATE) - 10


--14.Top 10 Actors who have appeared in the highest number of movies released in India
SELECT
	TRIM(UNNEST(STRING_TO_ARRAY(casts,','))) AS actor,
	COUNT(*) AS total_content
FROM netflix
WHERE country ILIKE '%India%'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10 ;


--15. Catetgorise the content based on the presence of the keywords "Kill" or "Violence" in the description field. Label these content as PG and all others as "family". Count how many movies fall into each category.
WITH cte AS 
(SELECT 
	*,
	CASE 
		WHEN description ILIKE '%Kill%' OR description ILIKE '%violence%' THEN 'PG'
		ELSE 'Family'
	End category
FROM netflix)

SELECT
	category,
	COUNT(*) as total_content
FROM cte
GROUP BY 1;
