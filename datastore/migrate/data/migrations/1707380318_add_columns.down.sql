ALTER TABLE time_series
	DROP COLUMN parameter_name,
	DROP COLUMN function,
	DROP COLUMN period,
	DROP COLUMN level,
	DROP CONSTRAINT unique_main,
	ADD CONSTRAINT unique_main UNIQUE NULLS NOT DISTINCT (version, type, title, summary, keywords,
		keywords_vocabulary, license, conventions, naming_authority, creator_type, creator_name,
		creator_email, creator_url, institution, project, source, platform, platform_vocabulary,
		standard_name, unit, instrument, instrument_vocabulary, link_href, link_rel, link_type,
		link_hreflang, link_title);
