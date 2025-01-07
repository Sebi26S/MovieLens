--Vizualization

--Average Rating by Day of Week
SELECT 
    d.dayOfWeekAsString, 
    AVG(f.rating) AS avg_rating
FROM Fact_Table f
JOIN DIM_time d ON f.DIM_time_timeID = d.dim_timeID
GROUP BY d.dayOfWeekAsString, DATE_PART('dow', d.date)
ORDER BY DATE_PART('dow', d.date);

--Top 10 Users by Number of Ratings
SELECT 
    f.DIM_user_userid, 
    COUNT(f.factid) AS rating_count
FROM Fact_Table f
GROUP BY f.DIM_user_userid
ORDER BY rating_count DESC
LIMIT 10;

--Hourly Rating Activity
SELECT 
    d.hour, 
    COUNT(f.factid) AS rating_count
FROM Fact_Table f
JOIN DIM_time d ON f.DIM_time_timeID = d.dim_timeID
GROUP BY d.hour
ORDER BY d.hour;


-- Total Ratings by Month
SELECT 
    d.month, 
    COUNT(f.factid) AS total_ratings
FROM Fact_Table f
JOIN DIM_time d ON f.DIM_time_timeID = d.dim_timeID
GROUP BY d.month
ORDER BY d.month;

--Ratings Trend by Year
SELECT 
    d.year, 
    COUNT(f.factid) AS total_ratings
FROM Fact_Table f
JOIN DIM_time d ON f.DIM_time_timeID = d.dim_timeID
GROUP BY d.year
ORDER BY d.year;

-- Most rated movies
SELECT 
    m.title AS movie_title,
    COUNT(f.factid) AS rating_count
FROM Fact_Table f
JOIN DIM_movie m ON f.DIM_movie_movieid = m.movieid
GROUP BY m.title
ORDER BY rating_count DESC
LIMIT 10;