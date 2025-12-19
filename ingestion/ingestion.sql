create table tsimmons.usgs_raw (
	usgs_raw_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY key,
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
	update_tm TIMESTAMPTZ default now()
);
select * from tsimmons.usgs_raw;
create table tsimmons.usgs_insert (
	usgs_insert_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY key,
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
	longitude FLOAT
);

create table tsimmons.census_raw (
	census_raw_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY key, 
	name VARCHAR(50),
	pop INT,
	hisp INT,
	state INT,
	county INT,
	update_tm TIMESTAMPTZ default now()
);
select * from tsimmons.census_raw;
create table tsimmons.census_insert (
	census_insert_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY key, 
	name VARCHAR(50),
	pop INT,
	hisp INT,
	state INT,
	county INT
);

create table tsimmons.geocode_raw (
	geocode_raw_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY key,
	name VARCHAR(50),
	lat FLOAT,
	lon FLOAT,
	country VARCHAR(50),
	state VARCHAR(50),
	update_tm TIMESTAMPTZ default now()
);
select * from tsimmons.geocode_raw;
create table tsimmons.geocode_insert (
	geocode_raw_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY key,
	name VARCHAR(50),
	lat FLOAT,
	lon FLOAT,
	country VARCHAR(50),
	state VARCHAR(50)
);


create table tsimmons.weather_raw (
	weather_raw_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY key,
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
	update_tm TIMESTAMPTZ default now()
);
select * from tsimmons.weather_raw;
create table tsimmons.weather_insert (
	weather_raw_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY key,
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
	wind FLOAT
);
