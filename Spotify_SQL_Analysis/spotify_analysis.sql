------------------------------------------
-- CASE STUDY: Exploring Spotify Data--
------------------------------------------

-- Tool used: PostgreSQL

-----------------------------------------
-- CASE STUDY QUESTIONS AND ANSWERS--
------------------------------------

-- Number of Artists on Spotify
SELECT COUNT(DISTINCT artist)
FROM spotify;


-- Number of albums avialable
SELECT COUNT(DISTINCT album)
FROM spotify;


-- Types of albums available to stream
SELECT DISTINCT album_type
FROM spotify;


-- Highest and Lowest duration
SELECT MAX(duration_min)
FROM spotify;


SELECT MIN(duration_min)
FROM spotify;


SELECT * FROM spotify
WHERE duration_min = 0;


DELETE FROM spotify
WHERE duration_min = 0;


-- Type of channels
SELECT DISTINCT channel
FROM spotify;


--1. Retrieve the name of all tracks that have more than 1 billion streams and views.
SELECT * 
FROM spotify
WHERE 
	stream >= 1000000000
	AND
	views >= 1000000000;


--2. List all albums along with their respective artists.
SELECT 
	DISTINCT album,
	artist
FROM spotify
ORDER BY 1;


--3. Get the total number of comments or tracks where licensed is equal to TRUE.
SELECT
	SUM(comments)
FROM spotify
WHERE licensed = 'true';


--4. Find all tracks that belong to albums type single
SELECT * 
FROM spotify
WHERE album_type ='single';


--5. Count the total number of tracks by each artist.
SELECT
	artist,
	COUNT(*) as total_songs
FROM spotify
GROUP BY artist
ORDER BY 2 DESC;


--6. Calculate the average danceability of tracks in each album.
SELECT 
	album,
	avg(danceability) AS avg_danceability
FROM spotify
GROUP BY 1
ORDER BY 2 DESC;

--7. Find the top 5 tracks with highest energy values.
SELECT
	track,
	MAX(energy)
FROM spotify
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;


--8. List all tracks along with their views and likes where official_video = True.
SELECT
	track,
	SUM(views) AS total_views,
	SUM(likes) AS total_likes
FROM spotify
WHERE official_video = 'true'
GROUP BY 1;


--9. For each album, Calculate the total views of all associated tracks.
SELECT 
	album,
	track,
	SUM(views) AS total_view
FROM spotify
GROUP BY 1,2;


--10. Retrieve the track names that have been streamed on spotify more than Youtube.
WITH cte AS(
	SELECT
		track,
		COALESCE(SUM(CASE WHEN most_played_on = 'Youtube' THEN stream END),0) AS streamed_on_youtube,
		COALESCE(SUM(CASE WHEN most_played_on = 'Spotify' THEN stream END),0) AS streamed_on_spotify
	FROM spotify
	GROUP BY 1
)

SELECT *
FROM cte 
WHERE
	streamed_on_spotify > streamed_on_youtube
	AND 
	streamed_on_youtube <> 0;


--11. Find the top 3 most viewed tracks of each artist.
WITH cte AS(
	SELECT
		artist,
		track,
		SUM(views) AS total_views,
		DENSE_RANK() OVER(PARTITION BY artist ORDER BY SUM(views) DESC) AS rk
	FROM spotify
	GROUP BY 1,2
	ORDER BY 1,3 DESC
)

SELECT *
FROM cte 
WHERE rk <=3;


--12. Write a query to find tracks where the liveness score is above the average.
SELECT 
	track,
	artist,
	liveness
FROM spotify
WHERE
	liveness > (
		SELECT AVG(liveness)
		FROM spotify
	)


--13. Calculate the highest and lowest energy values for tracks in each album.
WITH cte AS(
	SELECT
		album,
		MAX(energy) AS highest_energy,
		MIN(energy) AS lowest_energy
	FROM spotify
	GROUP BY 1
)

SELECT 
	album,
	highest_energy - lowest_energy AS energy_diff
FROM cte
ORDER BY 2 DESC;


--14. Find the tracks where energy too liveness ratio is greater than 1.2.
SELECT * FROM Spotify ;

SELECT 
	track,
	energy / liveness AS energy_to_liveness_ratio
FROM Spotify 
	WHERE energy / liveness > 1.2 ;


--15. Calcualte the cummulative sum of likes for tracks ordered by the number of views.
SELECT * FROM Spotify ;

SELECT 
	track,
	SUM(likes) OVER (ORDER BY views) AS cumulative_sum
FROM Spotify
	ORDER BY SUM(likes) OVER (ORDER BY views) DESC ;
