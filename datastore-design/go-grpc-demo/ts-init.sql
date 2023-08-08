CREATE EXTENSION postgis;

CREATE TABLE time_series (
	id INTEGER PRIMARY KEY,
	station_id TEXT NOT NULL,
	param_id TEXT NOT NULL,
	UNIQUE (station_id, param_id),
	pos GEOGRAPHY(Point) NOT NULL,
	other1 TEXT, -- additional metadata to be defined
	other2 TEXT, -- ----''----
	other3 TEXT  -- ----''----
    -- ...
);

CREATE TABLE observations (
	ts_id integer REFERENCES time_series(id) ON DELETE CASCADE,
	tstamp timestamp, -- obs time (NOT NULL, but implied by being part of PK)
	value double precision, -- obs value
	PRIMARY KEY (ts_id, tstamp),
	field1 TEXT, -- additional metadata to be defined
	field2 TEXT -- ----''----
);

SELECT create_hypertable(
	'observations', 'tstamp', chunk_time_interval => INTERVAL '1 hour'
);
