ALTER TABLE time_series
	DROP COLUMN IF EXISTS parameter_name,
	DROP COLUMN IF EXISTS function,
	DROP COLUMN IF EXISTS period,
	DROP COLUMN IF EXISTS level,
	DROP CONSTRAINT IF EXISTS unique_main,
	ADD CONSTRAINT unique_main UNIQUE NULLS NOT DISTINCT (version, type, title, summary, keywords,
		keywords_vocabulary, license, conventions, naming_authority, creator_type, creator_name,
		creator_email, creator_url, institution, project, source, platform, platform_vocabulary,
		standard_name, unit, instrument, instrument_vocabulary, link_href, link_rel, link_type,
		link_hreflang, link_title);
