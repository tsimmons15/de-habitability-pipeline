show server_version;

create table tsimmons.usgs_raw (
	mag FLOAT,
	place VARCHAR(50),
	time TIMESTAMPTZ,
	updated TIMESTAMPTZ,
	tz VARCHAR(50),
	felt VARCHAR(50),
	cdi VARCHAR(50),
	mmi VARCHAR(50),
	alert VARCHAR(50),
	status VARCHAR(50),
	tsunami INT,
	sig INT,
	net VARCHAR(50),
	code VARCHAR(50),
	nst INT,
	dmin FLOAT,
	rms FLOAT,
	gap INT,
	"magType" VARCHAR(50),
	type VARCHAR(50),
	title VARCHAR(50),
	latitude FLOAT,
	longitude FLOAT,
	update_tm TIMESTAMPTZ default now(),
	primary key (place, time)
);
select * from tsimmons.usgs_raw;
create table tsimmons.usgs_insert (
	mag FLOAT,
	place VARCHAR(50),
	time TIMESTAMPTZ,
	updated TIMESTAMPTZ,
	tz VARCHAR(50),
	felt VARCHAR(50),
	cdi VARCHAR(50),
	mmi VARCHAR(50),
	alert VARCHAR(50),
	status VARCHAR(50),
	tsunami INT,
	sig INT,
	net VARCHAR(50),
	code VARCHAR(50),
	nst INT,
	dmin FLOAT,
	rms FLOAT,
	gap INT,
	"magType" VARCHAR(50),
	type VARCHAR(50),
	title VARCHAR(50),
	latitude FLOAT,
	longitude FLOAT,
	primary key (place, time)
);

create table tsimmons.census_raw (
	name VARCHAR(50),
	pop INT,
	hisp INT,
	state INT,
	county INT,
	update_tm TIMESTAMPTZ default now(),
	primary key (name)
);
select * from tsimmons.census_raw;
create table tsimmons.census_insert ( 
	name VARCHAR(50),
	pop INT,
	hisp INT,
	state INT,
	county INT,
	primary key (name)
);

create table tsimmons.geocode_raw (
	name VARCHAR(50),
	lat FLOAT,
	lon FLOAT,
	country VARCHAR(50),
	state VARCHAR(50),
	update_tm TIMESTAMPTZ default now(),
	primary key (lat, lon)
);
select * from tsimmons.geocode_raw;
create table tsimmons.geocode_insert (
	name VARCHAR(50),
	lat FLOAT,
	lon FLOAT,
	country VARCHAR(50),
	state VARCHAR(50),
	primary key (lat, lon)
);

create table tsimmons.weather_raw (
	lat FLOAT,
	lon FLOAT,
	tz VARCHAR(25),
	date TIMESTAMPTZ,
	units VARCHAR(15),
	cloud_cover FLOAT,
	humidity FLOAT,
	precipitation FLOAT,
	temperature_min FLOAT,
	temperature_max FLOAT,
	pressure FLOAT,
	wind FLOAT,
	update_tm TIMESTAMPTZ default now(),
	PRIMARY KEY (lat, lon, date)
);
select * from tsimmons.weather_raw;
create table tsimmons.weather_insert (
	lat FLOAT,
	lon FLOAT,
	tz VARCHAR(25),
	date TIMESTAMPTZ,
	units VARCHAR(15),
	cloud_cover FLOAT,
	humidity FLOAT,
	precipitation FLOAT,
	temperature_min FLOAT,
	temperature_max FLOAT,
	pressure FLOAT,
	wind FLOAT,
	primary key (lat, lon, date)
);

CREATE OR REPLACE PROCEDURE truncate_insert_tables()
LANGUAGE plpgsql
AS $$
BEGIN
	SET CONSTRAINTS ALL IMMEDIATE;
    truncate tsimmons.usgs_insert;
	truncate tsimmons.geocode_insert;
	truncate tsimmons.census_insert;
	truncate tsimmons.weather_insert;
EXCEPTION WHEN OTHERS THEN
	RAISE;
END;
$$;


CREATE OR REPLACE PROCEDURE merge_insert_raw()
LANGUAGE plpgsql
AS $$
BEGIN
	SET CONSTRAINTS ALL IMMEDIATE;

	MERGE INTO tsimmons.weather_raw AS raw
	USING tsimmons.weather_insert AS src
	ON raw.lat = src.lat and raw.lon = src.lon and raw.date = src.date
	WHEN MATCHED THEN
    	UPDATE SET lat = src.lat, lon = src.lon, tz = src.tz,
							date = src.date, units = src.units, cloud_cover = src.cloud_cover,
							humidity = src.humidity, precipitation = src.precipitation, 
							temperature_min = src.temperature_min, temperature_max = src.temperature_max,
							pressure = src.pressure, wind = src.wind
	WHEN NOT MATCHED THEN
    	INSERT (lat, lon, tz, date, units, cloud_cover, humidity, precipitation,
				temperature_min, temperature_max, pressure, wind) VALUES 
			(src.lat, src.lon, src.tz, src.date, src.units, src.cloud_cover, src.humidity, src.precipitation,
			src.temperature_min, src.temperature_max, src.pressure, src.wind);

	MERGE INTO tsimmons.geocode_raw AS raw
	USING tsimmons.geocode_insert AS src
	ON raw.lat = src.lat and raw.lon = src.lon
	WHEN MATCHED THEN
    	UPDATE SET name = src.name, lat = src.lat, lon = src.lon, country = src.country, state = src.state
	WHEN NOT MATCHED THEN
    	INSERT (name, lat, lon, country, state) VALUES (src.name, src.lat, src.lon, src.country, src.state);

	MERGE INTO tsimmons.census_raw AS raw
	USING tsimmons.census_insert AS src
	ON raw.name = src.name
	WHEN MATCHED THEN
    	UPDATE SET name = src.name, pop = src.pop, hisp = src.hisp, state = src.state, county = src.county
	WHEN NOT MATCHED THEN
    	INSERT (name, pop, hisp, state, county) VALUES (src.name, src.pop, src.hisp, src.state, src.county);

	MERGE INTO tsimmons.usgs_raw AS raw
	USING tsimmons.usgs_insert AS src
	ON raw.place = src.place and raw.time = src.time
	WHEN MATCHED THEN
    	UPDATE SET mag = src.mag, place = src.place, time = src.time, updated = src.updated, tz = src.tz, 
					felt = src.felt, cdi = src.cdi, mmi = src.mmi, alert = src.alert, status = src.status, 
					tsunami = src.tsunami, sig = src.sig, net = src.net, code = src.code, nst = src.nst, 
					dmin = src.dmin, rms = src.rms, gap = src.gap, "magType" = src."magType", type = src.type, 
					title = src.title, latitude = src.latitude, longitude = src.longitude
	WHEN NOT MATCHED THEN
    	INSERT (mag, place, time, updated, tz, felt, cdi, mmi, alert, status, tsunami, sig, net, code, nst, dmin, rms, gap, "magType", 
			type, title, latitude, longitude) VALUES (src.mag, src.place, src.time, src.updated, src.tz, src.felt, src.cdi, src.mmi, 
			src.alert, src.status, src.tsunami, src.sig, src.net, src.code, src.nst, src.dmin, src.rms, src.gap, src."magType", 
			src.type, src.title, src.latitude, src.longitude);
EXCEPTION WHEN OTHERS THEN
	RAISE;
END;
$$;

