-- Active: 1720233756371@@localhost@5432@algoritma
DROP TABLE IF EXISTS dim_movie_year_released;
CREATE TABLE dim_movie_year_released AS
SELECT Month(date_published) AS month_released,
       Count(id)             AS number_of_movies
FROM   movie
GROUP  BY month_released
ORDER  BY month_released;


