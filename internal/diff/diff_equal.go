package diff

import (
    "fmt"
    "os"
    "sync"
    "time"

    "github.com/camry/fp"
    "github.com/camry/g/gerrors/gerror"
    "github.com/camry/g/glog"
    "github.com/fatih/color"
    "github.com/samber/lo"
    "github.com/vbauerster/mpb/v8"
    "github.com/vbauerster/mpb/v8/decor"
    "github.com/xuri/excelize/v2"

    "mysql2excel/internal/def"
    "mysql2excel/internal/excel"
    "mysql2excel/internal/model"
)

func (td *TableDiff) doDiffTableList(tableList []model.Table) {
    tableChunkList := lo.Chunk(tableList, 10)
    for _, tableChunk := range tableChunkList {
        errChan := make(chan error, 10)
        wg := &sync.WaitGroup{}
        progress := mpb.New(mpb.WithWaitGroup(wg))
        wg.Add(len(tableChunk))
        for _, table := range tableChunk {
            count := int64(0)
            limit := int32(10000)
            page := int32(1)

            err := td.sourceDb.Table(table.TableName).Count(&count).Error
            if err != nil {
                glog.Fatal(gerror.Wrapf(err, "doAddTableList TableName %s Count Failed", table.TableName))
            }
            if count > 0 {
                page = fp.F64FromInt64(count).DivPrecise(fp.F64FromInt32(limit)).CeilToInt()
            }

            bar := progress.AddBar(int64(page+1),
                mpb.PrependDecorators(
                    decor.Name(color.BlueString("%s", table.TableName)),
                    decor.Percentage(decor.WCSyncSpace),
                ),
                mpb.AppendDecorators(
                    decor.OnComplete(
                        decor.AverageETA(decor.ET_STYLE_GO, decor.WCSyncWidth), "DONE",
                    ),
                ),
            )
            go td.doDiffTable(bar, wg, errChan, table, limit, page)
        }
        progress.Wait()
        close(errChan)
        for errC := range errChan {
            if errC != nil {
                glog.Fatal(errC)
            }
        }
    }
}

func (td *TableDiff) doDiffTable(bar *mpb.Bar, wg *sync.WaitGroup, errChan chan error, table model.Table, limit int32, page int32) {
    var (
        sourceColumnList []model.Column
        targetColumnList []model.Column
        err              error
    )
    err = td.sourceDb.Table("information_schema.COLUMNS").Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", td.sourceDbConfig.Database, table.TableName).Order("ORDINAL_POSITION ASC").Find(&sourceColumnList).Error
    if err != nil {
        glog.Fatal(gerror.Wrapf(err, "sourceDb Table %s COLUMNS Find Failed", table.TableName))
    }
    err = td.targetDb.Table("information_schema.COLUMNS").Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", td.targetDbConfig.Database, table.TableName).Order("ORDINAL_POSITION ASC").Find(&targetColumnList).Error
    if err != nil {
        glog.Fatal(gerror.Wrapf(err, "targetDb Table %s COLUMNS Find Failed", table.TableName))
    }

    diffColumnList, diffColumnMap := td.doDiffColumn(sourceColumnList, targetColumnList)

    xlsxName := excel.FilterXlsxName(fmt.Sprintf("%s（%s）", table.TableName, table.TableComment))

    xlsxFile, err := excel.NewFile()
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "excel.NewFile Failed"))
    }
    f := xlsxFile.File

    // Sheet Title
    for _, column := range diffColumnList {
        colName, err1 := excelize.ColumnNumberToName(column.OrdinalPosition)
        if err1 != nil {
            glog.Fatal(gerror.Wrap(err1, "excelize.ColumnNumberToName Failed"))
        }
        width := float64(len(column.ColumnName)) * 1.5
        if width > 80 {
            width = 80
        }
        if width < 20 {
            width = 20
        }
        err = f.SetColWidth(def.SheetNameDefault, colName, colName, width)
        if err != nil {
            glog.Fatal(gerror.Wrap(err, "f.SetColWidth Failed"))
        }

        cell1 := fmt.Sprintf("%s1", colName)
        err = f.SetCellValue(def.SheetNameDefault, cell1, column.ColumnName)
        if err != nil {
            glog.Fatal(gerror.Wrap(err, "f.SetCellValue Failed"))
        }
        cell2 := fmt.Sprintf("%s2", colName)
        err = f.SetCellValue(def.SheetNameDefault, cell2, column.ColumnComment)
        if err != nil {
            glog.Fatal(gerror.Wrap(err, "f.SetCellValue Failed"))
        }
        if diffColumnState, ok := diffColumnMap[column.ColumnName]; ok {
            switch diffColumnState {
            case def.DiffColumnStateAdd:
                style, err2 := f.NewStyle(def.StyleAdd)
                if err2 != nil {
                    glog.Fatal(gerror.Wrap(err2, "f.NewStyle Failed"))
                }
                err2 = f.SetCellStyle(def.SheetNameDefault, cell1, cell2, style)
                if err2 != nil {
                    glog.Fatal(gerror.Wrap(err2, "f.SetCellStyle Failed"))
                }
            case def.DiffColumnStateDel:
                style, err2 := f.NewStyle(def.StyleDel)
                if err2 != nil {
                    glog.Fatal(gerror.Wrap(err2, "f.NewStyle Failed"))
                }
                err2 = f.SetCellStyle(def.SheetNameDefault, cell1, cell2, style)
                if err2 != nil {
                    glog.Fatal(gerror.Wrap(err2, "f.SetCellStyle Failed"))
                }
            }
        }
    }

    start := time.Now()
    path := fmt.Sprintf("diff/%s", td.sourceDbConfig.Database)
    _, err = os.Stat(path)
    if os.IsNotExist(err) {
        err = os.MkdirAll(path, os.ModePerm)
        if err != nil {
            glog.Fatal(gerror.Wrap(err, "os.MkdirAll Failed"))
        }
    }
    err = f.SaveAs(fmt.Sprintf("%s/DIFF %s.xlsx", path, xlsxName))
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "f.SaveAs Failed"))
    }
    bar.EwmaIncrement(time.Since(start))
}

func (td *TableDiff) doDiffColumn(sourceColumnList, targetColumnList []model.Column) ([]model.Column, map[string]def.DiffColumnState) {
    var diffColumnList []model.Column
    diffColumnMap := make(map[string]def.DiffColumnState)

    for _, sourceColumn := range sourceColumnList {
        _, ok := lo.Find(targetColumnList, func(item model.Column) bool {
            return sourceColumn.ColumnName == item.ColumnName
        })
        if ok {
            diffColumnMap[sourceColumn.ColumnName] = def.DiffColumnStateEqual
        } else {
            diffColumnMap[sourceColumn.ColumnName] = def.DiffColumnStateAdd
        }
        diffColumnList = append(diffColumnList, sourceColumn)
    }

    for _, targetColumn := range targetColumnList {
        _, ok := lo.Find(sourceColumnList, func(item model.Column) bool {
            return targetColumn.ColumnName == item.ColumnName
        })
        if !ok {
            diffColumnMap[targetColumn.ColumnName] = def.DiffColumnStateDel
            diffColumnList = append(diffColumnList, targetColumn)
        }
    }
    return diffColumnList, diffColumnMap
}
