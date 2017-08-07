package internal

import "database/sql"

// Column information retrieved from MySQL's information_schema.
type Column struct {
	name     string
	charset  string
	primary  bool
	unsigned bool
}

const retrieveColumnsSQL = `
SELECT
  COLUMN_NAME,
  CHARACTER_SET_NAME,
  COLUMN_KEY = 'PRI',
  COLUMN_TYPE LIKE '%unsigned%'
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;`

func RetrieveColumns(db *sql.DB, database string, table string) ([]*Column, error) {
	rows, err := db.Query(retrieveColumnsSQL, database, table)
	if err != nil {
		return nil, err
	}

	columns := make([]*Column, 0)
	for rows.Next() {
		var column Column
		rows.Scan(&column.name, &column.charset, &column.primary, &column.unsigned)
		columns = append(columns, &column)
	}
	return columns, nil
}

// Name returns the column name.
func (col *Column) Name() string {
	return col.name
}

// Charset returns the column character set.
func (col *Column) Charset() string {
	return col.charset
}

// IsPrimary returns if this column is a part of the primary key.
func (col *Column) IsPrimary() bool {
	return col.primary
}

// IsUnsigned returns if the type of this column is an unsigned number.
func (col *Column) IsUnsigned() bool {
	return col.unsigned
}
