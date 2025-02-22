package diff

import (
    "github.com/camry/g/gerrors/gerror"
    "github.com/camry/g/glog"
    "github.com/samber/lo"
    "gorm.io/gorm"

    "mysql2excel/internal/model"
)

type TableDiff struct {
    sourceDb       *gorm.DB
    targetDb       *gorm.DB
    sourceDbConfig model.DbConfig
    targetDbConfig model.DbConfig
    sourceTableMap map[string]model.Table
    targetTableMap map[string]model.Table
}

func NewTableDiff(sourceDb, targetDb *gorm.DB, sourceDbConfig, targetDbConfig model.DbConfig) *TableDiff {
    return &TableDiff{
        sourceDb:       sourceDb,
        targetDb:       targetDb,
        sourceDbConfig: sourceDbConfig,
        targetDbConfig: targetDbConfig,
        sourceTableMap: make(map[string]model.Table, 1000),
        targetTableMap: make(map[string]model.Table, 1000),
    }
}

func (td *TableDiff) Run() error {
    var (
        sourceTableList []model.Table
        targetTableList []model.Table
        addTableList    []model.Table
        delTableList    []model.Table
        diffTableList   []model.Table
        err             error
    )

    err = td.sourceDb.Table("information_schema.TABLES").Where("TABLE_SCHEMA = ?", td.sourceDbConfig.Database).Find(&sourceTableList).Error
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "gdb.New sourceTableList Find Failed"))
    }
    for _, table := range sourceTableList {
        td.sourceTableMap[table.TableName] = table
    }
    err = td.targetDb.Table("information_schema.TABLES").Where("TABLE_SCHEMA = ?", td.targetDbConfig.Database).Find(&targetTableList).Error
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "gdb.New targetTableList Find Failed"))
    }
    for _, table := range targetTableList {
        td.targetTableMap[table.TableName] = table
        if !lo.HasKey(td.sourceTableMap, table.TableName) {
            delTableList = append(delTableList, table)
        }
    }
    for _, table := range sourceTableList {
        if lo.HasKey(td.targetTableMap, table.TableName) {
            diffTableList = append(diffTableList, table)
        } else {
            addTableList = append(addTableList, table)
        }
    }

    td.doAddTableList(addTableList)
    td.doDelTableList(delTableList)
    td.doDiffTableList(diffTableList)

    return nil
}
