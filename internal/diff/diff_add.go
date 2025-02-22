package diff

import (
    "fmt"
    "os"
    "sync"
    "time"

    "github.com/camry/fp"
    "github.com/camry/g/frame/g"
    "github.com/camry/g/gerrors/gerror"
    "github.com/camry/g/glog"
    "github.com/dromara/carbon/v2"
    "github.com/fatih/color"
    "github.com/samber/lo"
    "github.com/vbauerster/mpb/v8"
    "github.com/vbauerster/mpb/v8/decor"
    "github.com/xuri/excelize/v2"

    "mysql2excel/internal/def"
    "mysql2excel/internal/excel"
    "mysql2excel/internal/model"
)

func (td *TableDiff) doAddTableList(tableList []model.Table) {
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

            bar := progress.AddBar(count+1,
                mpb.PrependDecorators(
                    decor.Name(fmt.Sprintf("%s %s", color.GreenString("ADD"), color.BlueString(table.TableName))),
                    decor.Percentage(decor.WCSyncSpace),
                ),
                mpb.AppendDecorators(
                    decor.OnComplete(
                        decor.AverageETA(decor.ET_STYLE_GO, decor.WCSyncWidth), "DONE",
                    ),
                ),
            )

            go td.doAddTable(bar, wg, errChan, table, limit, page)
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

func (td *TableDiff) doAddTable(bar *mpb.Bar, wg *sync.WaitGroup, errChan chan error, table model.Table, limit, page int32) {
    defer wg.Done()

    xlsxName := excel.FilterXlsxName(fmt.Sprintf("%s（%s）", table.TableName, table.TableComment))
    sheetName := lo.Substring(table.TableName, 0, 31)
    lastCell := "A1"

    // Sheet Title
    var columnList []model.Column
    err := td.sourceDb.Table("information_schema.COLUMNS").Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", table.TableSchema, table.TableName).Order("ORDINAL_POSITION ASC").Find(&columnList).Error
    if err != nil {
        glog.Fatal(gerror.Wrapf(err, "sourceDb Table %s COLUMNS Find Failed", table.TableName))
    }

    xlsxFile, err := excel.NewFile(sheetName)
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "excel.NewFile Failed"))
    }
    f := xlsxFile.File

    for _, column := range columnList {
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
        err = f.SetColWidth(sheetName, colName, colName, width)
        if err != nil {
            glog.Fatal(gerror.Wrap(err, "f.SetColWidth Failed"))
        }

        cell1 := fmt.Sprintf("%s1", colName)
        err1 = f.SetCellValue(sheetName, cell1, column.ColumnName)
        if err1 != nil {
            glog.Fatal(gerror.Wrap(err1, "f.SetCellValue Failed"))
        }
        cell2 := fmt.Sprintf("%s2", colName)
        err1 = f.SetCellValue(sheetName, cell2, column.ColumnComment)
        if err1 != nil {
            glog.Fatal(gerror.Wrap(err1, "f.SetCellValue Failed"))
        }
        lastCell = cell2
    }

    // Sheet Data
    for curPage := int32(1); curPage <= page; curPage++ {
        offset := limit * (curPage - 1)
        var (
            results []g.MapStrAny
            err1    error
        )
        err1 = td.sourceDb.Table(table.TableName).Offset(int(offset)).Limit(int(limit)).Find(&results).Error
        if err1 != nil {
            errChan <- err1
            return
        } else {
            resultsLen := len(results)
            if resultsLen > 0 {
                for k, result := range results {
                    start := time.Now()
                    for _, column := range columnList {
                        if columnValue, ok := result[column.ColumnName]; ok {
                            colName, err2 := excelize.ColumnNumberToName(column.OrdinalPosition)
                            if err2 != nil {
                                errChan <- gerror.Wrap(err2, "excelize.ColumnNumberToName Failed")
                                return
                            }
                            if v, ok1 := columnValue.(time.Time); ok1 {
                                columnValue = carbon.CreateFromStdTime(v).ToDateTimeString()
                            }
                            cell := fmt.Sprintf("%s%d", colName, int(offset)+k+3)
                            lastCell = cell
                            err2 = f.SetCellValue(sheetName, cell, columnValue)
                            if err2 != nil {
                                errChan <- gerror.Wrap(err2, "f.SetCellValue Failed")
                                return
                            }
                        }
                    }
                    bar.EwmaIncrement(time.Since(start))
                }
            }
        }
    }
    start := time.Now()
    style, err := f.NewStyle(def.StyleAdd)
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "f.NewStyle Failed"))
    }
    err = f.SetCellStyle(sheetName, "A1", lastCell, style)
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "f.SetCellStyle Failed"))
    }
    path := fmt.Sprintf("diff/%s", td.sourceDbConfig.Database)
    _, err = os.Stat(path)
    if os.IsNotExist(err) {
        err = os.MkdirAll(path, os.ModePerm)
        if err != nil {
            glog.Fatal(gerror.Wrap(err, "os.MkdirAll Failed"))
        }
    }
    err = f.SaveAs(fmt.Sprintf("%s/ADD %s.xlsx", path, xlsxName))
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "f.SaveAs Failed"))
    }
    bar.EwmaIncrement(time.Since(start))
}
