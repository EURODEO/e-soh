package postgresql

import (
	"database/sql"
	"datastore/common"
	"datastore/datastore"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/lib/pq"
)

var (
	attr2col map[string]string // attribute name to database column name
	col2attr map[string]string // database column name to attribute name
)

func init() {
	attr2col = map[string]string{}
	col2attr = map[string]string{}
	for _, f := range reflect.VisibleFields(reflect.TypeOf(datastore.TSMetadata{})) {
		if f.IsExported() {
			attr := f.Name
			col := common.ToSnakeCase(attr)
			attr2col[attr] = col
			col2attr[col] = attr
		}
	}

	// ### for now don't support the 'links' attribute
	// TODO: support this!
	delete(attr2col, "Links")
	delete(col2attr, "links")
}

// getTSAttrs returns attribute names corresponding to cols.
func getTSAttrs(cols []string) ([]string, error) {
	attrs := []string{}
	for _, col := range cols {
		attr, found := col2attr[col]
		if !found {
			return nil, fmt.Errorf("col2attr: no value found for key >%s<", col)
		}
		attrs = append(attrs, attr)
	}
	return attrs, nil
}

// getTSAllCols returns names of all database columns.
func getTSAllCols() ([]string, error) {
	cols := []string{}
	for col := range col2attr {
		cols = append(cols, col)
	}
	return cols, nil
}

// getTSAttrCols returns names of database columns corresponding to attrs.
func getTSAttrCols(attrs []string) ([]string, error) {
	seen := map[string]struct{}{}
	cols := []string{}

	supAttrs := func() []string {
		attrs := []string{}
		for a := range attr2col {
			attrs = append(attrs, a)
		}
		return attrs
	}

	for _, attr := range attrs {
		col, found := attr2col[attr]
		if !found { // not an attribute name
			_, found0 := col2attr[attr]
			if !found0 { // not a column name either
				return nil, fmt.Errorf(
					"attribute not found: %s; supported attributes: %s",
					attr, strings.Join(supAttrs(), ", "))
			}
			col = attr // attr is already a column name
		}

		if _, found = seen[col]; found {
			return nil, fmt.Errorf("attribute %s specified more than once", attr)
		}

		cols = append(cols, col)
	}

	return cols, nil
}

// getTSMdata returns a TSMetadata object initialized from colVals.
func getTSMdata(colVals map[string]interface{}) (*datastore.TSMetadata, error) {

	tsMData := datastore.TSMetadata{}
	tp := reflect.ValueOf(&tsMData)

	for col, val := range colVals {
		attr, found := col2attr[col]
		if !found {
			return nil, fmt.Errorf(
				"key not found in col2attr: %s (existing contents: %v)", col, col2attr)
		}

		field := tp.Elem().FieldByName(attr)
		if !field.IsValid() {
			return nil, fmt.Errorf("invalid field (attr: %s, col: %s)", attr, col)
		}
		if !field.CanSet() {
			return nil, fmt.Errorf("unassignable field (attr: %s, col: %s)", attr, col)
		}

		switch field.Kind() {
		case reflect.String:
			val0, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf(
					"value not string: %v (type: %T; attr: %s, col: %s", val, val, attr, col)
			}
			field.SetString(val0)
		default:
			return nil, fmt.Errorf(
				"unsupported type: %v (val: %v; type: %T; attr: %s; col: %s)",
				field.Kind(), val, val, attr, col)
		}
	}

	return &tsMData, nil
}

// scanTsMdata scans column cols from current result row in rows and converts to a
// TSMetadata object.
// Returns (TSMetadata object, nil) upon success, otherwise (..., error).
func scanTsMdata(rows *sql.Rows, cols []string) (*datastore.TSMetadata, error) {
	colVals0 := make([]interface{}, len(cols))   // column values
	colValPtrs := make([]interface{}, len(cols)) // pointers to column values
	for i := range colVals0 {
		colValPtrs[i] = &colVals0[i]
	}
	// scan row into column value pointers
	if err := rows.Scan(colValPtrs...); err != nil {
		return nil, fmt.Errorf("rows.Scan() failed: %v", err)
	}
	// combine column names and -values into a map
	colVals := map[string]interface{}{}
	for i, col := range cols {
		colVals[col] = colVals0[i]
	}
	// convert to a TSMetadata object
	tsMdata, err := getTSMdata(colVals)
	if err != nil {
		return nil, fmt.Errorf("getTSMdata failed(): %v", err)
	}

	return tsMdata, nil
}

// getTSMdataEqual returns upon success (true, nil) iff tsmd1 and tsmd2 are equal wrt. attrs,
// otherwise (false, nil). If an error occurs, (..., error) is returned.
func tsMdataEqual(tsmd1, tsmd2 *datastore.TSMetadata, attrs []string) (bool, error) {
	tp1 := reflect.ValueOf(tsmd1)
	tp2 := reflect.ValueOf(tsmd2)

	for _, attr := range attrs {
		field1 := tp1.Elem().FieldByName(attr)
		if !field1.IsValid() {
			return false, fmt.Errorf("tp1.Elem().FieldByName(%s) returned invalid value", attr)
		}
		field2 := tp2.Elem().FieldByName(attr)
		if !field2.IsValid() {
			return false, fmt.Errorf("tp2.Elem().FieldByName(%s) returned invalid value", attr)
		}

		if !field1.Equal(field2) {
			return false, nil // at least one difference found
		}
	}

	return true, nil // no differences found
}

// getTSACGroupsIncInstances populates groups from cols such that each group contains all
// instances that match a unique combination of database values corresponding to cols.
// All attributes, including those in cols, are set to the actual values found in the database.
// Returns nil upon success, otherwise error.
func getTSACGroupsIncInstances(db *sql.DB, cols []string, groups *[]*datastore.TSMdataGroup) error {
	allCols, err := getTSAllCols() // get all database column names
	if err != nil {
		return fmt.Errorf("getTSAllCols() failed: %v", err)
	}

	attrs, err := getTSAttrs(cols)
	if err != nil {
		return fmt.Errorf("getTSAttrs() failed: %v", err)
	}

	// query database for all columns in time_series, ordered by cols
	allColsS := strings.Join(allCols, ",")
	colsS := strings.Join(cols, ",")
	query := fmt.Sprintf("SELECT %s FROM time_series ORDER BY %s", allColsS, colsS)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	// aggregate rows into groups
	currInstances := []*datastore.TSMetadata{} // initial current instance set
	for rows.Next() {
		// extract tsMdata from current result row
		tsMdata, err := scanTsMdata(rows, allCols)
		if err != nil {
			return fmt.Errorf("scanTsMdata() failed: %v", err)
		}

		if len(currInstances) > 0 { // check if we should create a new current instance set
			equal, err := tsMdataEqual(tsMdata, currInstances[0], attrs)
			if err != nil {
				return fmt.Errorf("tsMdataEqual() failed: %v", err)
			}

			if !equal { // ts metadata changed wrt. cols
				// add next group with current instance set
				*groups = append(*groups, &datastore.TSMdataGroup{Combos: currInstances})
				currInstances = []*datastore.TSMetadata{} // create a new current instance set
			}
		}

		currInstances = append(currInstances, tsMdata) // add tsMdata to current instance set
	}

	// assert(len(currInstances) > 0)
	// add final group with current instance set
	*groups = append(*groups, &datastore.TSMdataGroup{Combos: currInstances})

	return nil
}

// getTSACGroupsComboOnly populates groups from cols such that each group contains a single,
// unique combination of database values corresponding to cols. Other attributes than those in cols
// have the default value for the type (i.e. "" for string, etc.).
// Returns nil upon success, otherwise error.
func getTSACGroupsComboOnly(db *sql.DB, cols []string, groups *[]*datastore.TSMdataGroup) error {
	// query database for unique combinations of cols in time_series, ordered by cols
	colsS := strings.Join(cols, ",")
	query := fmt.Sprintf("SELECT DISTINCT %s FROM time_series ORDER BY %s", colsS, colsS)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	// aggregate rows into groups
	for rows.Next() {
		// extract tsMdata from current result row
		tsMdata, err := scanTsMdata(rows, cols)
		if err != nil {
			return fmt.Errorf("scanTsMdata() failed: %v", err)
		}

		// add new group with tsMData as the only item in the Combos array
		*groups = append(*groups, &datastore.TSMdataGroup{
			Combos: []*datastore.TSMetadata{tsMdata},
		})
	}

	return nil
}

// GetTSAttrCombos ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetTSAttrCombos(request *datastore.GetTSACRequest) (
	*datastore.GetTSACResponse, error) {

	cols, err := getTSAttrCols(request.Attrs) // get database column names for requested attributes
	if err != nil {
		return nil, fmt.Errorf("getTSAttrCols() failed: %v", err)
	}

	groups := []*datastore.TSMdataGroup{}

	if request.IncludeInstances {
		if err := getTSACGroupsIncInstances(sbe.Db, cols, &groups); err != nil {
			return nil, fmt.Errorf("getTSACGroupsIncInstances() failed: %v", err)
		}
	} else {
		if err := getTSACGroupsComboOnly(sbe.Db, cols, &groups); err != nil {
			return nil, fmt.Errorf("getTSACGroupsComboOnly() failed: %v", err)
		}
	}

	return &datastore.GetTSACResponse{Groups: groups}, nil
}
