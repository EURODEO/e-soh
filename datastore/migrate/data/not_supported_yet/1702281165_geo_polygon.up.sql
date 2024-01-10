-- not supported yet
CREATE TABLE IF NOT EXISTS geo_polygon (
	id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	polygon GEOGRAPHY(Polygon, 4326) NOT NULL
);

CREATE INDEX geo_polygon_idx ON geo_polygon USING GIST(polygon);

------- BEGIN support both single instant and interval for obs time ---------
-- TODO: Fix geo_polygon_id. How to fill the existing rows, otherwise column cannot be added
-- ALTER TABLE observation
-- 	ADD geo_polygon_id integer NOT NULL REFERENCES geo_polygon(id) ON DELETE CASCADE; -- not supported yet

ALTER TABLE observation
	ADD obstime_start timestamptz,   -- obs time variant 2: interval
	ADD obstime_end timestamptz,
    ADD CONSTRAINT observation_chk_one_obs_time
	CHECK ( -- ensure exactly one of [1] obstime_instant and [2] obstime_start/-end is defined
		((obstime_instant IS NOT NULL) AND (obstime_start IS NULL) AND (obstime_end IS NULL)) OR
		((obstime_instant IS NULL) AND (obstime_start IS NOT NULL) AND (obstime_end IS NOT NULL))
	);
------- END support both single instant and interval for obs time ---------
