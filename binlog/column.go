package binlog

import "database/sql"

type column struct {
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

func retrieveColumns(db *sql.DB, database string, table string) ([]*column, error) {
	rows, err := db.Query(retrieveColumnsSQL, database, table)
	if err != nil {
		return nil, err
	}

	cols := make([]*column, 0)
	for rows.Next() {
		var col column
		rows.Scan(&col.name, &col.charset, &col.primary, &col.unsigned)
		cols = append(cols, &col)
	}
	return cols, nil
}

// Name returns the column name.
func (col *column) Name() string {
	return col.name
}

// Charset returns the column character set.
func (col *column) Charset() string {
	return col.charset
}

// IsPrimary returns if this column is a part of the primary key.
func (col *column) IsPrimary() bool {
	return col.primary
}

// IsUnsigned returns if the type of this column is an unsigned number.
func (col *column) IsUnsigned() bool {
	return col.unsigned
}
