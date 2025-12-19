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
	ids VARCHAR(50),
	sources VARCHAR(50),
	types VARCHAR(50),
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
	ids VARCHAR(50),
	sources VARCHAR(50),
	types VARCHAR(50),
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
    
    -- Commit the transaction
    COMMIT;
EXCEPTION WHEN OTHERS THEN
	ROLLBACK;
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
    	UPDATE SET raw.lat = src.lat, raw.lon = src.lon, raw.tz = src.tz,
							raw.date = src.date, raw.units = src.units, raw.cloud_cover = src.cloud_cover,
							raw.humidity = src.humidity, raw.precipitation = src.precipitation, 
							raw.temperature_min = src.temperature_min, raw.temperature_max = src.temperature_max,
							raw.pressure = src.pressure, raw.wind = src.wind
	WHEN NOT MATCHED THEN
    	INSERT (lat, lon, tz, date, units, cloud_cover, humidity, precipitation,
				temperature_min, temperature_max, pressure, wind) VALUES 
			(src.lat, src.lon, src.tz, src.date, src.units, src.cloud_cover, src.humidity, src.precipitation,
			src.temperature_min, src.temperature_max, src.pressure, src.wind);

	MERGE INTO tsimmons.geocode_raw AS raw
	USING tsimmons.geocode_insert AS src
	ON raw.lat = src.lat and raw.lon = src.lon
	WHEN MATCHED THEN
    	UPDATE SET raw.name = src.name, raw.lat = src.lat, raw.lon = src.lon, 
				raw.country = src.country, raw.state = src.state
	WHEN NOT MATCHED THEN
    	INSERT (name, lat, lon, country, state) VALUES (src.name, src.lat,
									src.lon, src.country, src.state);

	MERGE INTO tsimmons.census_raw AS raw
	USING tsimmons.census_insert AS src
	ON raw.name = src.name
	WHEN MATCHED THEN
    	UPDATE SET raw.name = src.name, raw.pop = src.pop, raw.hisp = src.hisp, raw.state = src.state,
							raw.county = src.county
	WHEN NOT MATCHED THEN
    	INSERT (name, pop, hisp, state, county) VALUES (src.name, src.pop, src.hisp, 
								src.state, src.county);

	MERGE INTO tsimmons.usgs_raw AS raw
	USING tsimmons.usgs_insert AS src
	ON raw.place = src.place and raw.time = src.time
	WHEN MATCHED THEN
    	UPDATE SET raw.mag = src.mag, raw.place = src.place, raw.time = src.time, raw.updated = src.updated,
								raw.tz = src.tz, raw.felt = src.felt, raw.cdi = src.cdi, raw.mmi = src.mmi, raw.alert = src.alert,
								raw.status = src.status, raw.tsunami = src.tsunami, raw.sig = src.sig, raw.net = src.net, raw.code = src.code,
								raw.ids = src.ids, raw.sources = src.sources, raw.types = src.types, raw.nst = src.nst, raw.dmin = src.dmin, 
								raw.rms = src.rms, raw.gap = src.gap, raw."magType" = src."magType", raw.type = src.type, raw.title = src.title, raw.latitude = src.latitude,
								raw.longitude = src.longitude
	WHEN NOT MATCHED THEN
    	INSERT (mag, place, time, updated, tz, felt, cdi, mmi, alert, status, tsunami, sig, net, code, ids, sources, types, nst,
dmin, rms, gap, "magType", type, title, latitude, longitude) VALUES (src.mag, src.place, src.time, src.updated, src.tz, src.felt, src.cdi, 
			src.mmi, src.alert, src.status, src.tsunami, src.sig, src.net, src.code, src.ids, src.sources, src.types, src.nst,
			src.dmin, src.rms, src.gap, src."magType", src.type, src.title, src.latitude, src.longitude);

    -- Commit the transaction
    COMMIT;
EXCEPTION WHEN OTHERS THEN
	ROLLBACK;
	RAISE;
END;
$$;

