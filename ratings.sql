DROP TABLE IF EXISTS ratings;
CREATE TABLE ratings AS
SELECT 
    movie_id, 
    avg_rating, 
    total_votes, 
    median_rating
FROM rsvs_movie.ratings