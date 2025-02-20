package model

import "database/sql"

type Table struct {
    TableCatalog   string         `gorm:"column:TABLE_CATALOG"`
    TableSchema    string         `gorm:"column:TABLE_SCHEMA"`
    TableName      string         `gorm:"column:TABLE_NAME"`
    TableType      string         `gorm:"column:TABLE_TYPE"`
    ENGINE         sql.NullString `gorm:"column:ENGINE"`
    VERSION        sql.NullInt64  `gorm:"column:VERSION"`
    RowFormat      sql.NullString `gorm:"column:ROW_FORMAT"`
    TableRows      sql.NullInt64  `gorm:"column:TABLE_ROWS"`
    AvgRowLength   sql.NullInt64  `gorm:"column:AVG_ROW_LENGTH"`
    DataLength     sql.NullInt64  `gorm:"column:DATA_LENGTH"`
    MaxDataLength  sql.NullInt64  `gorm:"column:MAX_DATA_LENGTH"`
    IndexLength    sql.NullInt64  `gorm:"column:INDEX_LENGTH"`
    DataFree       sql.NullInt64  `gorm:"column:DATA_FREE"`
    AutoIncrement  sql.NullInt64  `gorm:"column:AUTO_INCREMENT"`
    CreateTime     sql.NullTime   `gorm:"column:CREATE_TIME"`
    UpdateTime     sql.NullTime   `gorm:"column:UPDATE_TIME"`
    CheckTime      sql.NullTime   `gorm:"column:CHECK_TIME"`
    TableCollation sql.NullString `gorm:"column:TABLE_COLLATION"`
    CHECKSUM       sql.NullInt64  `gorm:"column:CHECKSUM"`
    CreateOptions  sql.NullString `gorm:"column:CREATE_OPTIONS"`
    TableComment   string         `gorm:"column:TABLE_COMMENT"`
}
