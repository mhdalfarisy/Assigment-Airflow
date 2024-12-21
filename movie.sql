DROP TABLE IF EXISTS movie;

CREATE TABLE movie AS 
SELECT 
    id, 
    title, 
    year, 
    date_published, 
    duration, 
    country, 
    worlwide_gross_income, 
    languages, 
    production_company 
FROM rsvs_movie.movie