package model

import "database/sql"

type Statistic struct {
    TableCatalog string         `gorm:"column:TABLE_CATALOG"`
    TableSchema  string         `gorm:"column:TABLE_SCHEMA"`
    TableName    string         `gorm:"column:TABLE_NAME"`
    NonUnique    int64          `gorm:"column:NON_UNIQUE"`
    IndexSchema  string         `gorm:"column:INDEX_SCHEMA"`
    IndexName    string         `gorm:"column:INDEX_NAME"`
    SeqInIndex   int            `gorm:"column:SEQ_IN_INDEX"`
    ColumnName   string         `gorm:"column:COLUMN_NAME"`
    COLLATION    sql.NullString `gorm:"column:COLLATION"`
    CARDINALITY  sql.NullInt64  `gorm:"column:CARDINALITY"`
    SubPart      sql.NullInt32  `gorm:"column:SUB_PART"`
    PACKED       sql.NullString `gorm:"column:PACKED"`
    NULLABLE     string         `gorm:"column:NULLABLE"`
    IndexType    string         `gorm:"column:INDEX_TYPE"`
    COMMENT      sql.NullString `gorm:"column:COMMENT"`
    IndexComment string         `gorm:"column:INDEX_COMMENT"`
    IsVisible    sql.NullString `gorm:"column:IS_VISIBLE"`
}
