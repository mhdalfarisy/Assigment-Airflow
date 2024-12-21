-- Active: 1720233756371@@localhost@5432@algoritma
DROP TABLE IF EXISTS genre;
CREATE TABLE genre AS
SELECT 
movie_id,
genre 
FROM rsvs_movie.genre;
-- rsvs_movie