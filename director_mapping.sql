DROP TABLE IF EXISTS director_mapping;

CREATE TABLE director_mapping AS
SELECT
    movie_id,
    name_id
FROM rsvs_movie.director_mapping;


