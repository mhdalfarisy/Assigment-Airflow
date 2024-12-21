
DROP TABLE IF EXISTS role_mapping;
CREATE TABLE role_mapping AS
SELECT 
    movie_id, 
    name_id, 
    category
FROM rsvs_movie.role_mapping
