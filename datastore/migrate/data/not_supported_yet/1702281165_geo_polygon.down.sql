ALTER TABLE observation
	DROP COLUMN IF EXISTS geo_polygon_id,
	DROP COLUMN IF EXISTS obstime_start,   -- obs time variant 2: interval
	DROP COLUMN IF EXISTS obstime_end,
	DROP CONSTRAINT IF EXISTS observation_chk_one_obs_time;

DROP TABLE IF EXISTS geo_polygon;
