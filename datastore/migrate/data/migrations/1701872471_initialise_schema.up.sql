CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE time_series (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

	-- --- BEGIN metadata fields that usually don't vary with obs time ---

	-- --- BEGIN non-string metadata -----------------
	link_href TEXT[],
	link_rel TEXT[],
	link_type TEXT[],
	link_hreflang TEXT[],
	link_title TEXT[],
	-- --- END non-string metadata -----------------

	-- --- BEGIN string metadata (handleable with reflection) -----------------
	version TEXT NOT NULL, -- required
	type TEXT NOT NULL, -- required
	title TEXT,
	summary TEXT NOT NULL, -- required
	keywords TEXT NOT NULL, -- required
	keywords_vocabulary TEXT NOT NULL, -- required
	license TEXT NOT NULL, -- required
	conventions TEXT NOT NULL, -- required
	naming_authority TEXT NOT NULL, -- required
	creator_type TEXT,
	creator_name TEXT,
	creator_email TEXT,
	creator_url TEXT,
	institution TEXT,
	project TEXT,
	source TEXT,
	platform TEXT NOT NULL, -- required
	platform_vocabulary TEXT NOT NULL, -- required
	standard_name TEXT,
	unit TEXT,
	instrument TEXT NOT NULL,
	instrument_vocabulary TEXT NOT NULL,
	-- --- END string metadata -----------------

	-- --- END metadata fields that usually don't vary with obs time ---

	CONSTRAINT unique_main UNIQUE NULLS NOT DISTINCT (version, type, title, summary, keywords,
	keywords_vocabulary, license, conventions, naming_authority, creator_type, creator_name,
	creator_email, creator_url, institution, project, source, platform, platform_vocabulary,
	standard_name, unit, instrument, instrument_vocabulary, link_href, link_rel, link_type,
	link_hreflang, link_title)
);

CREATE TABLE geo_point (
	id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	point GEOGRAPHY(Point, 4326) NOT NULL UNIQUE
);

CREATE INDEX geo_point_idx ON geo_point USING GIST(point);

CREATE TABLE observation (
	ts_id BIGINT NOT NULL REFERENCES time_series(id) ON DELETE CASCADE,

	-- --- BEGIN metadata fields that usually vary with obs time ---

	-- --- BEGIN non-string metadata -----------------
	-- Refer to geometry via a foreign key to ensure that each distinct geometry is
	-- stored only once in the geo_* table, thus speeding up geo search.
	geo_point_id BIGINT NOT NULL REFERENCES geo_point(id) ON DELETE CASCADE,

	-- --- BEGIN for now support only a single instant for obs time ---------
	obstime_instant timestamptz, -- NOT NULL, but implied by being part of PK; obs time variant 1: single instant
	-- --- END for now support only a single instant for obs time ---------

	pubtime timestamptz NOT NULL, -- required
	-- --- END non-string metadata -----------------

	-- --- BEGIN string metadata (handleable with reflection) -----------------
	id TEXT NOT NULL, -- required
	data_id TEXT NOT NULL, -- required
	history TEXT,
	metadata_id TEXT NOT NULL, -- required
	processing_level TEXT,
	-- --- END string metadata -----------------

	value TEXT NOT NULL, -- obs value (not metadata in a strict sense)

	-- --- END metadata fields that usually vary with obs time ---

	PRIMARY KEY (ts_id, obstime_instant)
);
