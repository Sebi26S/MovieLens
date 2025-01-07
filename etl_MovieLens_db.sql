CREATE DATABASE IF NOT EXISTS FOX_MovieLens_DB;
USE DATABASE FOX_MovieLens_DB;

CREATE SCHEMA IF NOT EXISTS FOX_MovieLens_DB.STAGING;
USE SCHEMA FOX_MovieLens_DB.STAGING;

CREATE OR REPLACE STAGE FOX_MovieLens_DB_STAGE

-- 1) movie.csv
CREATE OR REPLACE TABLE movie_staging (
    movieId   NUMBER,
    title VARCHAR,
    genres  VARCHAR
);

-- 2) tag.csv
CREATE OR REPLACE TABLE tag_staging (
    userid   NUMBER,
    movieid  NUMBER,
    tag  VARCHAR,
    timestamp TIMESTAMP_NTZ
);



-- 3) rating.csv
CREATE OR REPLACE TABLE rating_staging (
    userid   NUMBER,
    movieid  NUMBER,
    rating  NUMBER,
    timestamp TIMESTAMP_NTZ
);

-- 4) genome_tags.csv
CREATE OR REPLACE TABLE genome_tags_staging (
    tagId  NUMBER,
    tag  VARCHAR
);

-- 5) genome_scores.csv
CREATE OR REPLACE TABLE genome_scores_staging (
    movieId  NUMBER,
    tagId  NUMBER,
    relevance FLOAT
);

--  copy data to movie
COPY INTO movie_staging
FROM @FOX_MovieLens_DB_STAGE/movie.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ESCAPE_UNENCLOSED_FIELD = '\\' 
  FIELD_DELIMITER = ','
)
ON_ERROR = 'CONTINUE';

SELECT COUNT(*) FROM movie_staging;
SELECT * FROM movie_staging LIMIT 10;

--  copy data to tag
COPY INTO tag_staging
FROM @FOX_MovieLens_DB_STAGE/tag.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  FIELD_DELIMITER = ','
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';


SELECT COUNT(*) FROM tag_staging;
SELECT * FROM tag_staging LIMIT 10;

--  copy data to rating
COPY INTO rating_staging
FROM @FOX_MovieLens_DB_STAGE/rating.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

SELECT COUNT(*) FROM rating_staging;
SELECT * FROM rating_staging LIMIT 10;

--  copy data to genome_tags
COPY INTO genome_tags_staging
FROM @FOX_MovieLens_DB_STAGE/genome_tags.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

SELECT COUNT(*) FROM genome_tags_staging;
SELECT * FROM genome_tags_staging LIMIT 10;

--  copy data to genome_scores
COPY INTO genome_scores_staging
FROM @FOX_MovieLens_DB_STAGE/genome_scores.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

SELECT COUNT(*) FROM genome_scores_staging;
SELECT * FROM genome_scores_staging LIMIT 10;


-- DIM_movie
CREATE OR REPLACE TABLE DIM_movie AS
SELECT DISTINCT
  movieId   AS movieid,
  title     AS title,
  genres    AS genres
FROM movie_staging;

-- DIM_tag
CREATE OR REPLACE TABLE DIM_tag AS
SELECT DISTINCT
  tagId AS tagid,
  tag   AS tag
FROM genome_tags_staging;

-- DIM_user
CREATE OR REPLACE TABLE DIM_user AS
SELECT DISTINCT
  userid AS userid
FROM rating_staging;
-- DIM_time
CREATE OR REPLACE TABLE DIM_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(timestamp AS DATE)) AS dim_timeID,
    CAST(timestamp AS DATE) AS date,
    DATE_PART('day', timestamp) AS day,
    DATE_PART('dow', timestamp) + 1 AS dayOfWeek,
    CASE DATE_PART('dow', timestamp) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS dayOfWeekAsString,
    DATE_PART('month', timestamp) AS month,
    DATE_PART('year', timestamp) AS year,
    DATE_PART('quarter', timestamp) AS quarter,
    DATE_PART('hour', timestamp) AS hour,
    DATE_PART('minute', timestamp) AS minute,
    DATE_PART('second', timestamp) AS second
FROM rating_staging;


-- Fact_Table
CREATE OR REPLACE TABLE Fact_Table AS
SELECT
  ROW_NUMBER() OVER (ORDER BY r.movieid, r.userid) AS factid,
  r.movieid               AS DIM_movie_movieid,
  r.userid                AS DIM_user_userid,
  gt.tagId                AS DIM_tag_tagid,
  r.rating                AS rating,
  gs.relevance            AS relevance,
  d.dim_timeID            AS DIM_time_timeID
FROM rating_staging r
LEFT JOIN genome_scores_staging gs
  ON r.movieid = gs.movieId
LEFT JOIN genome_tags_staging gt
  ON gs.tagId = gt.tagId
JOIN DIM_time d
  ON CAST(r.timestamp AS DATE) = d.date;

  SELECT 
    factid,
    DIM_movie_movieid,
    DIM_user_userid,
    DIM_tag_tagid,
    rating,
    relevance,
    DIM_time_timeID
FROM Fact_Table
LIMIT 10;