ALTER TABLE time_series
	ADD COLUMN IF NOT EXISTS level TEXT,
	ADD COLUMN IF NOT EXISTS period TEXT,
	ADD COLUMN IF NOT EXISTS function TEXT,
	ADD COLUMN IT NOT EXISTS parameter_name TEXT,
	DROP CONSTRAINT IF EXISTS unique_main,
	ADD CONSTRAINT unique_main UNIQUE NULLS NOT DISTINCT (version, type, title, summary, keywords,
		keywords_vocabulary, license, conventions, naming_authority, creator_type, creator_name,
		creator_email, creator_url, institution, project, source, platform, platform_vocabulary,
		standard_name, unit, instrument, instrument_vocabulary, level, period, function,
		parameter_name, link_href, link_rel, link_type, link_hreflang, link_title);
