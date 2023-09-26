CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE time_series (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,

	-- --- BEGIN metadata fields that usually don't vary with obs time ---
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
	link_href TEXT[],
	link_rel TEXT[],
	link_type TEXT[],
	link_hreflang TEXT[],
	link_title TEXT[],
	-- --- END metadata fields that usually don't vary with obs time ---

	UNIQUE NULLS NOT DISTINCT (version, type, title, summary, keywords, keywords_vocabulary,
	license, conventions, naming_authority, creator_type, creator_name, creator_email,
	creator_url, institution, project, source, platform, platform_vocabulary, standard_name,
	unit, instrument, instrument_vocabulary, link_href, link_rel, link_type, link_hreflang,
	link_title),

	-- Time of most recent observation associated with this time series.
	-- Used for quickly identifying time series that have become too old and can be deleted:
	--     DELETE FROM time_series WHERE latest_obs_time < oldest_allowable_obs_time
	latest_obs_time timestamptz NOT NULL
);

CREATE TABLE geo_point (
	id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	point GEOGRAPHY(Point, 4326) NOT NULL UNIQUE,

	-- Time of most recent observation associated with this point.
	-- Used for quickly identifying points that have become too old and can be deleted:
	--     DELETE FROM geo_point WHERE latest_obs_time < oldest_allowable_obs_time
	latest_obs_time timestamptz NOT NULL
);

CREATE INDEX geo_point_idx ON geo_point USING GIST(point);

-- not supported yet
-- CREATE TABLE geo_polygon (
-- 	id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
-- 	polygon GEOGRAPHY(Polygon, 4326) NOT NULL
-- )
--
-- CREATE INDEX geo_polygon_idx ON geo_polygon USING GIST(polygon);

CREATE TABLE observation (
	ts_id BIGINT NOT NULL REFERENCES time_series(id) ON DELETE CASCADE,

	-- --- BEGIN metadata fields that usually vary with obs time ---
	id TEXT NOT NULL, -- required

	-- Refer to geometry via a foreign key to ensure that each distinct geometry is
	-- stored only once in the geo_* table, thus speeding up geo search.
	geo_point_id BIGINT NOT NULL REFERENCES geo_point(id) ON DELETE CASCADE,
	-- geo_polygon_id integer NOT NULL REFERENCES geo_polygon(id) ON DELETE CASCADE, -- not supported yet

	pubtime timestamptz NOT NULL, -- required
	data_id TEXT NOT NULL, -- required
	history TEXT,
	metadata_id TEXT NOT NULL, -- required

	-- --- BEGIN for now support only a single instant for obs time ---------
	obstime_instant timestamptz, -- NOT NULL, but implied by being part of PK
	-- --- END for now support only a single instant for obs time ---------

	-- --- BEGIN support both single instant and interval for obs time ---------
	-- obstime_instant timestamptz, -- obs time variant 1: single instant
	-- obstime_start timestamptz,   -- obs time variant 2: interval
	-- obstime_end timestamptz,
	-- CHECK ( -- ensure exactly one of [1] obstime_instant and [2] obstime_start/-end is defined
	-- 	((obstime_instant IS NOT NULL) AND (obstime_start IS NULL) AND (obstime_end IS NULL)) OR
	-- 	((obstime_instant IS NULL) AND (obstime_start IS NOT NULL) AND (obstime_end IS NOT NULL))
	-- ),
	-- --- END support both single instant and interval for obs time ---------

	processing_level TEXT,
	value TEXT NOT NULL, -- obs value
	-- --- END metadata fields that usually vary with obs time ---

	PRIMARY KEY (ts_id, obstime_instant)
);
