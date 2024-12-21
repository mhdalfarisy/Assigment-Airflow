
DROP TABLE IF EXISTS dim_movie_year;
CREATE TABLE dim_movie_year AS
SELECT year, Count(id) AS number_of_movies
FROM   movie
GROUP  BY year;


