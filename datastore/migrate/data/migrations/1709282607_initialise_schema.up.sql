CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE time_series (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

	-- UMC = part of the unique_main constraint

	-- --- BEGIN metadata fields that usually don't vary with obs time ---

	-- --- BEGIN non-reflectable metadata -----------------
	link_href TEXT[],
	link_rel TEXT[],
	link_type TEXT[],
	link_hreflang TEXT[],
	link_title TEXT[],
	-- --- END non-reflectable metadata -----------------

	-- --- BEGIN reflectable metadata (of type int64/BIGINT or string/TEXT) -----------------
	version TEXT NOT NULL,
	type TEXT NOT NULL,
	title TEXT,
	summary TEXT NOT NULL,
	keywords TEXT NOT NULL,
	keywords_vocabulary TEXT NOT NULL,
	license TEXT NOT NULL,
	conventions TEXT NOT NULL,
	naming_authority TEXT NOT NULL, -- UMC
	creator_type TEXT,
	creator_name TEXT,
	creator_email TEXT,
	creator_url TEXT,
	institution TEXT,
	project TEXT,
	source TEXT,
	platform TEXT NOT NULL, -- UMC
	platform_vocabulary TEXT NOT NULL,
	platform_name TEXT,
	standard_name TEXT NOT NULL, -- UMC
	unit TEXT,
	level BIGINT NOT NULL, -- UMC
	function TEXT NOT NULL, -- UMC
	period BIGINT NOT NULL, -- UMC
	instrument TEXT NOT NULL, -- UMC
	instrument_vocabulary TEXT NOT NULL,
	parameter_name TEXT NOT NULL,

    -- 1-1 relationship with the columns of the unique_main constraint (typically a hash of those)
	timeseries_id TEXT NOT NULL,
	-- --- END reflectable metadata -----------------

	-- --- END metadata fields that usually don't vary with obs time ---

	CONSTRAINT unique_main UNIQUE NULLS NOT DISTINCT (naming_authority, platform, standard_name,
		level, function, period, instrument)
);

CREATE TABLE geo_point (
	id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	point GEOGRAPHY(Point, 4326) NOT NULL UNIQUE
);

CREATE INDEX geo_point_idx ON geo_point USING GIST(point);

CREATE TABLE observation (
	ts_id BIGINT NOT NULL REFERENCES time_series(id) ON DELETE CASCADE,

	-- --- BEGIN metadata fields that usually vary with obs time ---

	-- --- BEGIN non-reflectable metadata -----------------
	-- Refer to geometry via a foreign key to ensure that each distinct geometry is
	-- stored only once in the geo_* table, thus speeding up geo search.
	geo_point_id BIGINT NOT NULL REFERENCES geo_point(id) ON DELETE CASCADE,

	-- --- BEGIN for now support only a single instant for obs time ---------
	obstime_instant timestamptz, -- NOT NULL, but implied by being part of PK; obs time variant 1: single instant
	-- --- END for now support only a single instant for obs time ---------

	pubtime timestamptz NOT NULL, -- required
	-- --- END non-reflectable metadata -----------------

	-- --- BEGIN reflectable metadata of type string -----------------
	id TEXT NOT NULL, -- required
	data_id TEXT NOT NULL, -- required
	history TEXT,
	processing_level TEXT,
	-- --- END reflectable metadata of type string -----------------

	value TEXT NOT NULL, -- obs value (not metadata in a strict sense)

	-- --- END metadata fields that usually vary with obs time ---

	PRIMARY KEY (ts_id, obstime_instant)
);
