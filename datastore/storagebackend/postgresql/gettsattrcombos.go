package postgresql

import (
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
func getTSMData(colVals map[string]interface{}) (*datastore.TSMetadata, error) {

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

// GetTSAttrCombos ... (see documentation in StorageBackend interface)
func (sbe *PostgreSQL) GetTSAttrCombos(request *datastore.GetTSACRequest) (
	*datastore.GetTSACResponse, error) {

	cols, err := getTSAttrCols(request.Attrs) // get database column names
	if err != nil {
		return nil, fmt.Errorf("getTSAttrCols() failed: %v", err)
	}

	// query database for unique combinations of these columns
	colsS := strings.Join(cols, ",")
	query := fmt.Sprintf("SELECT DISTINCT %s FROM time_series ORDER BY %s", colsS, colsS)
	rows, err := sbe.Db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("db.Query() failed: %v", err)
	}
	defer rows.Close()

	// aggregate rows into overall result
	combos := []*datastore.TSMetadata{}
	for rows.Next() {
		colVals0 := make([]interface{}, len(cols)) // column values

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

		// convert to a TSMetadata object and add to overall result
		tsMData, err := getTSMData(colVals)
		if err != nil {
			return nil, fmt.Errorf("getTSMData failed(): %v", err)
		}
		combos = append(combos, tsMData)
	}

	return &datastore.GetTSACResponse{Combos: combos}, nil
}
