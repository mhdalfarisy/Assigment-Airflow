-- dwh_rsvs_movie.

DROP TABLE IF EXISTS names;
CREATE TABLE names AS
SELECT 
id, 
name, 
height, 
date_of_birth, 
known_for_movies 
FROM rsvs_movie.names
-- rsvs_movie.