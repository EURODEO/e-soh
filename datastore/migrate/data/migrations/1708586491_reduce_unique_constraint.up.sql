ALTER TABLE time_series
	DROP CONSTRAINT IF EXISTS unique_main,
	ADD CONSTRAINT unique_main UNIQUE NULLS NOT DISTINCT (naming_authority, platform,
		standard_name, instrument, level, period, function);
