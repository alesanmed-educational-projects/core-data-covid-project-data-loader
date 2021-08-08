CREATE EXTENSION IF NOT EXISTS postgis;
DO $$ BEGIN CREATE TYPE case_type AS ENUM ('confirmed', 'dead', 'recovered');
EXCEPTION
WHEN duplicate_object THEN null;
END $$;
CREATE TABLE IF NOT EXISTS countries (
	id SERIAL PRIMARY KEY,
	name VARCHAR NOT NULL,
	alpha2 VARCHAR(2) UNIQUE,
	alpha3 VARCHAR(3) UNIQUE,
	location GEOGRAPHY(POINT) NOT NULL,
	geom geometry(Polygon, 4326),
	CONSTRAINT one_alpha_code CHECK (
		alpha2 IS NOT NULL
		OR alpha3 IS NOT NULL
	)
);
CREATE INDEX idx_countries_alpha2 ON countries (alpha2);
CREATE INDEX idx_countries_alpha3 ON countries (alpha3);
CREATE INDEX idx_countries_geom ON countries USING GIST (geom);
CREATE TABLE IF NOT EXISTS provinces (
	id SERIAL PRIMARY KEY,
	name VARCHAR NOT NULL,
	location GEOGRAPHY(POINT) NOT NULL,
	geom geometry(Polygon, 4326),
	code VARCHAR(3),
	country_id INT REFERENCES countries (id) NOT NULL,
	UNIQUE(name, code, country_id)
);
CREATE INDEX idx_provinces_code ON provinces (code);
CREATE INDEX idx_provinces_geom ON provinces USING GIST (geom);
CREATE TABLE IF NOT EXISTS counties (
	id SERIAL PRIMARY KEY,
	name VARCHAR NOT NULL,
	location GEOGRAPHY(POINT) NOT NULL,
	geom geometry(Polygon, 4326),
	code VARCHAR(3),
	province_id INT REFERENCES provinces (id),
	UNIQUE(name, code, province_id)
);
CREATE INDEX idx_counties_code ON counties (code);
CREATE INDEX idx_counties_geom ON counties USING GIST (geom);
CREATE TABLE IF NOT EXISTS cases (
	id SERIAL PRIMARY KEY,
	type case_type,
	amount BIGINT,
	date DATE,
	country_id INT REFERENCES countries (id),
	province_id INT REFERENCES provinces (id),
	county_id INT REFERENCES counties (id)
);
CREATE UNIQUE INDEX cases_unique_case ON cases (
	type,
	date,
	country_id,
	COALESCE(province_id, -1),
	COALESCE(county_id, -1)
);
CREATE INDEX idx_cases_country ON cases (country_id);
CREATE INDEX idx_cases_province ON cases (province_id);
CREATE INDEX idx_cases_county ON cases (county_id);