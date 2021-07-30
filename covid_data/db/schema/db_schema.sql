CREATE EXTENSION IF NOT EXISTS postgis;

DO $$ BEGIN
    CREATE TYPE case_type AS ENUM (
		'confirmed',
		'dead',
		'recovered'
	);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


CREATE TABLE IF NOT EXISTS countries (
	id 			    SERIAL PRIMARY KEY,
	name		    VARCHAR NOT NULL,
	alpha2 		  	VARCHAR(2) NOT NULL,
	alpha3		  	VARCHAR(3) NOT NULL,
	location	  	GEOGRAPHY(POINT) NOT NULL
);

CREATE TABLE IF NOT EXISTS provinces (
	id			    SERIAL PRIMARY KEY,
	name		    VARCHAR NOT NULL,
	location	  	GEOGRAPHY(POINT) NOT NULL,
  	code        	VARCHAR(3),
	country_id		INT REFERENCES countries (id)
);

CREATE TABLE IF NOT EXISTS counties (
	id			    SERIAL PRIMARY KEY,
	name		    VARCHAR NOT NULL,
	location	  	GEOGRAPHY(POINT) NOT NULL,
  	code        	VARCHAR(3),
	province_id		INT REFERENCES countries (id)
);

CREATE TABLE IF NOT EXISTS cases (
	id			    SERIAL PRIMARY KEY,
	type		    case_type,
	amount		  	BIGINT,
	date		    DATE,
	country_id		INT REFERENCES countries (id),
	province_id 	INT REFERENCES provinces (id),
	county_id		INT REFERENCES counties (id)
);

CREATE UNIQUE INDEX cases_unique_case ON cases (type, date, country_id, COALESCE(province_id, -1), COALESCE(county_id, -1))

