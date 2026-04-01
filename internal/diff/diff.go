package diff

import (
    "github.com/camry/g/v2/gerrors/gerror"
    "github.com/camry/g/v2/glog"
    "github.com/samber/lo"
    "gorm.io/gorm"

    "github.com/camry/mysql2excel/internal/model"
)

type TableDiff struct {
    sourceDb         *gorm.DB
    targetDb         *gorm.DB
    sourceDbConfig   model.DbConfig
    targetDbConfig   model.DbConfig
    sourceTableMap   map[string]model.Table
    targetTableMap   map[string]model.Table
    includeTableList []string
    excludeTableList []string
}

func NewTableDiff(sourceDb, targetDb *gorm.DB, sourceDbConfig, targetDbConfig model.DbConfig, includeTableList []string, excludeTableList []string) *TableDiff {
    return &TableDiff{
        sourceDb:         sourceDb,
        targetDb:         targetDb,
        sourceDbConfig:   sourceDbConfig,
        targetDbConfig:   targetDbConfig,
        sourceTableMap:   make(map[string]model.Table, 1000),
        targetTableMap:   make(map[string]model.Table, 1000),
        includeTableList: includeTableList,
        excludeTableList: excludeTableList,
    }
}

func (td *TableDiff) Run() error {
    var (
        sourceTableList      []model.Table
        targetTableList      []model.Table
        finalSourceTableList []model.Table
        finalTargetTableList []model.Table
        addTableList         []model.Table
        delTableList         []model.Table
        diffTableList        []model.Table
        err                  error
    )
    err = td.sourceDb.Table("information_schema.TABLES").Where("TABLE_SCHEMA = ?", td.sourceDbConfig.Database).Find(&sourceTableList).Error
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "TableDiff Run sourceTableList Find Failed"))
    }
    err = td.targetDb.Table("information_schema.TABLES").Where("TABLE_SCHEMA = ?", td.targetDbConfig.Database).Find(&targetTableList).Error
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "TableDiff Run targetTableList Find Failed"))
    }
    var filterTableFunc = func(item model.Table, index int) bool {
        c1 := len(td.includeTableList) > 0
        c2 := len(td.excludeTableList) > 0
        if c1 && c2 {
            return lo.Contains(td.includeTableList, item.TableName) && !lo.Contains(td.excludeTableList, item.TableName)
        }
        if c1 {
            return lo.Contains(td.includeTableList, item.TableName)
        }
        if c2 {
            return !lo.Contains(td.excludeTableList, item.TableName)
        }
        return true
    }
    finalSourceTableList = lo.Filter(sourceTableList, filterTableFunc)
    finalTargetTableList = lo.Filter(targetTableList, filterTableFunc)
    for _, table := range finalSourceTableList {
        td.sourceTableMap[table.TableName] = table
    }
    for _, table := range finalTargetTableList {
        td.targetTableMap[table.TableName] = table
        if !lo.HasKey(td.sourceTableMap, table.TableName) {
            delTableList = append(delTableList, table)
        }
    }
    for _, table := range finalSourceTableList {
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
