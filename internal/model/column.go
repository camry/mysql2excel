package model

import "database/sql"

type Column struct {
    TableCatalog           string         `gorm:"column:TABLE_CATALOG"`
    TableSchema            string         `gorm:"column:TABLE_SCHEMA"`
    TableName              string         `gorm:"column:TABLE_NAME"`
    ColumnName             string         `gorm:"column:COLUMN_NAME"`
    OrdinalPosition        int            `gorm:"column:ORDINAL_POSITION"`
    ColumnDefault          sql.NullString `gorm:"column:COLUMN_DEFAULT"`
    IsNullable             string         `gorm:"column:IS_NULLABLE"`
    DataType               string         `gorm:"column:DATA_TYPE"`
    CharacterMaximumLength sql.NullInt64  `gorm:"column:CHARACTER_MAXIMUM_LENGTH"`
    CharacterOctetLength   sql.NullInt64  `gorm:"column:CHARACTER_OCTET_LENGTH"`
    NumericPrecision       sql.NullInt64  `gorm:"column:NUMERIC_PRECISION"`
    NumericScale           sql.NullInt64  `gorm:"column:NUMERIC_SCALE"`
    DatetimePrecision      sql.NullInt64  `gorm:"column:DATETIME_PRECISION"`
    CharacterSetName       sql.NullString `gorm:"column:CHARACTER_SET_NAME"`
    CollationName          sql.NullString `gorm:"column:COLLATION_NAME"`
    ColumnType             string         `gorm:"column:COLUMN_TYPE"`
    ColumnKey              string         `gorm:"column:COLUMN_KEY"`
    EXTRA                  string         `gorm:"column:EXTRA"`
    PRIVILEGES             string         `gorm:"column:PRIVILEGES"`
    ColumnComment          string         `gorm:"column:COLUMN_COMMENT"`
    GenerationExpression   string         `gorm:"column:GENERATION_EXPRESSION"`
}
