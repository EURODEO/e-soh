package postgresql

import (
	"datastore/datastore"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

var (
	supAttrs map[string]struct{} // supported attributes
)

func init() {

	supAttrs = map[string]struct{}{}

	for _, sa := range []string{
		"version",
		"type",
		"title",
		"summary",
		"keywords",
		"keywords_vocabulary",
		"license",
		"conventions",
		"naming_authority",
		"creator_type",
		"creator_name",
		"creator_email",
		"creator_url",
		"institution",
		"project",
		"source",
		"platform",
		"platform_vocabulary",
		"standard_name",
		"unit",
		"instrument",
		"instrument_vocabulary",
		// TODO: support links
	} {
		supAttrs[sa] = struct{}{}
	}
}

// getTSAttrCols ... (TODO: add documentation)
func getTSAttrCols(attrs []string) ([]string, error) {
	seen := map[string]struct{}{}

	cols := []string{}

	for _, attr := range attrs {
		attr0 := strings.ToLower(strings.TrimSpace(attr))
		if _, found := seen[attr0]; found {
			return nil, fmt.Errorf("attribute >%s< specified more than once", attr0)
		}

		if _, found := supAttrs[attr0]; found {
			cols = append(cols, attr0)
			seen[attr0] = struct{}{}
		} else {
			return nil, fmt.Errorf("unsupported attribute: >%s<", attr0)
		}
	}

	return cols, nil
}

// getTSMdata ... (TODO: add documentation)
func getTSMData(m map[string]interface{}) (*datastore.TSMetadata, error) {

	tsMData := datastore.TSMetadata{}

	for k, v := range m {
		switch k {
		case "version":
			tsMData.Version = v.(string)
		case "type":
			tsMData.Type = v.(string)
		case "title":
			tsMData.Title = v.(string)
		case "summary":
			tsMData.Summary = v.(string)
		case "keywords":
			tsMData.Keywords = v.(string)
		case "keywords_vocabulary":
			tsMData.KeywordsVocabulary = v.(string)
		case "license":
			tsMData.License = v.(string)
		case "conventions":
			tsMData.Conventions = v.(string)
		case "naming_authority":
			tsMData.NamingAuthority = v.(string)
		case "creator_type":
			tsMData.CreatorType = v.(string)
		case "creator_name":
			tsMData.CreatorName = v.(string)
		case "creator_email":
			tsMData.CreatorEmail = v.(string)
		case "creator_url":
			tsMData.CreatorUrl = v.(string)
		case "institution":
			tsMData.Institution = v.(string)
		case "project":
			tsMData.Project = v.(string)
		case "source":
			tsMData.Source = v.(string)
		case "platform":
			tsMData.Platform = v.(string)
		case "platform_vocabulary":
			tsMData.PlatformVocabulary = v.(string)
		case "standard_name":
			tsMData.StandardName = v.(string)
		case "unit":
			tsMData.Unit = v.(string)
		case "instrument":
			tsMData.Instrument = v.(string)
		// TODO: support links
		default:
			return nil, fmt.Errorf("unsupported attribute: >%s<", k)
		}
	}

	return &tsMData, nil
}

// GetTSAttrCombos ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetTSAttrCombos(request *datastore.GetTSACRequest) (
	*datastore.GetTSACResponse, error) {

	cols, err := getTSAttrCols(request.Attrs)
	if err != nil {
		return nil, fmt.Errorf("getTSAttrCols() failed: %v", err)
	}

	colsS := strings.Join(cols, ",")
	query := fmt.Sprintf("SELECT DISTINCT %s FROM time_series ORDER BY %s", colsS, colsS)
	rows, err := sbe.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	matches := []*datastore.TSMetadata{}
	for rows.Next() {
		// create a slice of interface{}'s to represent each column, and a second slice to contain
		// pointers to each item in the columns slice
		cols0 := make([]interface{}, len(cols))
		colPtrs := make([]interface{}, len(cols))
		for i := range cols0 {
			colPtrs[i] = &cols0[i]
		}

		// scan the result into the columns pointers
		if err := rows.Scan(colPtrs...); err != nil {
			return nil, fmt.Errorf("rows.Scan() failed: %v", err)
		}

		// retrieve the value for each column from the pointers slice
	    m := make(map[string]interface{})
		for i, colName := range cols {
			val := colPtrs[i].(*interface{})
			m[colName] = *val
		}

		// convert to a TSMetadata object and add to final result
		tsMData, err := getTSMData(m)
		if err != nil {
			return nil, fmt.Errorf("getTSMData failed(): %v", err)
		}
		matches = append(matches, tsMData)
	}

	return &datastore.GetTSACResponse{Matches: matches}, nil
}
