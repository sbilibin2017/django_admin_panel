CREATE SCHEMA IF NOT EXISTS content;


SET search_path TO content,public;


CREATE TABLE IF NOT EXISTS film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE ,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone NOT NULL,
    modified timestamp with time zone NOT NULL
); 
CREATE INDEX film_work_creation_date_idx ON film_work(creation_date); 


CREATE TABLE IF NOT EXISTS genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created timestamp with time zone,
    modified timestamp with time zone        
);


CREATE TABLE IF NOT EXISTS person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone        
);


CREATE TABLE IF NOT EXISTS person_film_work (
    id uuid PRIMARY KEY,
    person_id uuid NOT NULL REFERENCES person (id),
    film_work_id uuid NOT NULL REFERENCES film_work (id),    
    role TEXT NOT NULL,
    created timestamp with time zone
);
CREATE UNIQUE INDEX film_work_person_idx ON person_film_work (film_work_id, person_id);


CREATE TABLE IF NOT EXISTS genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL REFERENCES genre (id),
    film_work_id uuid NOT NULL REFERENCES film_work (id),
    created timestamp with time zone
);
CREATE UNIQUE INDEX genre_film_work_idx ON genre_film_work (film_work_id, genre_id);