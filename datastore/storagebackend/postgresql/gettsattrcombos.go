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
	attr2col map[string]string // attribute to database column
	col2attr map[string]string // database column to attribute
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

// getTSAttrCols returns database columns corresponding to attrs.
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

// getTSMdata returns a TSMetadata object initialized from m.
func getTSMData(m map[string]interface{}) (*datastore.TSMetadata, error) {

	tsMData := datastore.TSMetadata{}
	tp := reflect.ValueOf(&tsMData)

	for col, val := range m {
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

	combos := []*datastore.TSMetadata{}
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
		m := map[string]interface{}{}
		for i, colName := range cols {
			val := colPtrs[i].(*interface{})
			m[colName] = *val
		}

		// convert to a TSMetadata object and add to final result
		tsMData, err := getTSMData(m)
		if err != nil {
			return nil, fmt.Errorf("getTSMData failed(): %v", err)
		}
		combos = append(combos, tsMData)
	}

	return &datastore.GetTSACResponse{Combos: combos}, nil
}
