package diff

import (
    "fmt"
    "os"
    "strings"
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

func (td *TableDiff) doDiffTableList(tableList []model.Table) {
    tableChunkList := lo.Chunk(tableList, 10)
    for _, tableChunk := range tableChunkList {
        errChan := make(chan error, 10)
        wg := &sync.WaitGroup{}
        progress := mpb.New(mpb.WithWaitGroup(wg))
        wg.Add(len(tableChunk))
        for _, table := range tableChunk {
            limit := int32(10000)
            sourceCount := int64(0)
            sourcePage := int32(1)
            targetCount := int64(0)
            targetPage := int32(1)

            err := td.sourceDb.Table(table.TableName).Count(&sourceCount).Error
            if err != nil {
                glog.Fatal(gerror.Wrapf(err, "doDiffTableList sourceDb TableName %s Count Failed", table.TableName))
            }
            if sourceCount > 0 {
                sourcePage = fp.F64FromInt64(sourceCount).DivPrecise(fp.F64FromInt32(limit)).CeilToInt()
            }
            err = td.targetDb.Table(table.TableName).Count(&targetCount).Error
            if err != nil {
                glog.Fatal(gerror.Wrapf(err, "doDiffTableList targetDb TableName %s Count Failed", table.TableName))
            }
            if targetCount > 0 {
                targetPage = fp.F64FromInt64(targetCount).DivPrecise(fp.F64FromInt32(limit)).CeilToInt()
            }

            bar := progress.AddBar(int64(sourcePage)+int64(targetPage)+sourceCount+targetCount+3,
                mpb.PrependDecorators(
                    decor.Name(fmt.Sprintf("%s %s", color.YellowString("DIFF"), color.BlueString(table.TableName))),
                    decor.Percentage(decor.WCSyncSpace),
                ),
                mpb.AppendDecorators(
                    decor.OnComplete(
                        decor.AverageETA(decor.ET_STYLE_GO, decor.WCSyncWidth), "DONE",
                    ),
                ),
            )
            go td.doDiffTable(bar, wg, errChan, table, limit, sourceCount, targetCount, sourcePage, targetPage)
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

func (td *TableDiff) doDiffTable(bar *mpb.Bar, wg *sync.WaitGroup, errChan chan error, table model.Table, limit int32, sourceCount, targetCount int64, sourcePage, targetPage int32) {
    defer wg.Done()

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
    if table.TableName == "base_scene_type" {
        glog.Error("")
    }

    diffColumnList, diffColumnMap := td.diffColumn(sourceColumnList, targetColumnList)

    xlsxName := excel.FilterXlsxName(fmt.Sprintf("%s（%s）", table.TableName, table.TableComment))
    sheetName := lo.Substring(table.TableName, 0, 31)

    xlsxFile, err := excel.NewFile(sheetName)
    if err != nil {
        glog.Fatal(gerror.Wrap(err, "excel.NewFile Failed"))
    }
    f := xlsxFile.File

    // Sheet Title
    for k, column := range diffColumnList {
        colName, err1 := excelize.ColumnNumberToName(k + 1)
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
        err = f.SetCellValue(sheetName, cell1, column.ColumnName)
        if err != nil {
            glog.Fatal(gerror.Wrap(err, "f.SetCellValue Failed"))
        }
        cell2 := fmt.Sprintf("%s2", colName)
        err = f.SetCellValue(sheetName, cell2, column.ColumnComment)
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
                err2 = f.SetCellStyle(sheetName, cell1, cell2, style)
                if err2 != nil {
                    glog.Fatal(gerror.Wrap(err2, "f.SetCellStyle Failed"))
                }
            case def.DiffColumnStateDel:
                style, err2 := f.NewStyle(def.StyleDel)
                if err2 != nil {
                    glog.Fatal(gerror.Wrap(err2, "f.NewStyle Failed"))
                }
                err2 = f.SetCellStyle(sheetName, cell1, cell2, style)
                if err2 != nil {
                    glog.Fatal(gerror.Wrap(err2, "f.SetCellStyle Failed"))
                }
            }
        }
    }

    // Sheet Data
    sourcePrimaryKeyList := td.getSourcePrimaryKeyList(table)
    targetPrimaryKeyList := td.getTargetPrimaryKeyList(table)
    sourceTableDataList := make([]g.MapStrAny, 0, sourceCount)
    targetTableDataList := make([]g.MapStrAny, 0, targetCount)
    sourceTableDataMap := make(map[string]g.MapStrAny, sourceCount)
    targetTableDataMap := make(map[string]g.MapStrAny, targetCount)

    for curPage := int32(1); curPage <= sourcePage; curPage++ {
        start := time.Now()
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
                for _, result := range results {
                    sourceTableDataList = append(sourceTableDataList, result)
                }
            }
        }
        bar.EwmaIncrement(time.Since(start))
    }
    for curPage := int32(1); curPage <= targetPage; curPage++ {
        start := time.Now()
        offset := limit * (curPage - 1)
        var (
            results []g.MapStrAny
            err1    error
        )
        err1 = td.targetDb.Table(table.TableName).Offset(int(offset)).Limit(int(limit)).Find(&results).Error
        if err1 != nil {
            errChan <- err1
            return
        } else {
            resultsLen := len(results)
            if resultsLen > 0 {
                for _, result := range results {
                    targetTableDataList = append(targetTableDataList, result)
                }
            }
        }
        bar.EwmaIncrement(time.Since(start))
    }

    sourceStart := time.Now()
    for _, sourceTableData := range sourceTableDataList {
        sourceTableDataMap[td.getPrimaryKeyData(sourcePrimaryKeyList, sourceTableData)] = sourceTableData
    }
    bar.EwmaIncrement(time.Since(sourceStart))
    targetStart := time.Now()
    for _, targetTableData := range targetTableDataList {
        targetTableDataMap[td.getPrimaryKeyData(targetPrimaryKeyList, targetTableData)] = targetTableData
    }
    bar.EwmaIncrement(time.Since(targetStart))

    // DO DIFF
    i := 0
    for _, sourceTableData := range sourceTableDataList {
        start := time.Now()
        k := td.getPrimaryKeyData(sourcePrimaryKeyList, sourceTableData)

        if _, ok := targetTableDataMap[k]; !ok {
            for kk, column := range diffColumnList {
                colName, err2 := excelize.ColumnNumberToName(kk + 1)
                if err2 != nil {
                    errChan <- err2
                    return
                }
                cell := fmt.Sprintf("%s%d", colName, i+3)
                if diffColumnMap[column.ColumnName] < def.DiffColumnStateDel {
                    value := sourceTableData[column.ColumnName]
                    if v, ok1 := value.(time.Time); ok1 {
                        value = carbon.CreateFromStdTime(v).ToDateTimeString()
                    }
                    if bytes, ok1 := value.([]byte); ok1 {
                        value = string(bytes)
                    }
                    err = f.SetCellValue(sheetName, cell, value)
                    if err != nil {
                        errChan <- gerror.Wrap(err, "f.SetCellValue Failed")
                        return
                    }
                    style, err3 := f.NewStyle(def.StyleAdd)
                    if err3 != nil {
                        errChan <- err3
                        return
                    }
                    err = f.SetCellStyle(sheetName, cell, cell, style)
                    if err != nil {
                        errChan <- gerror.Wrap(err, "f.SetCellStyle Failed")
                        return
                    }
                } else {
                    err = f.SetCellValue(sheetName, cell, "")
                    if err != nil {
                        errChan <- gerror.Wrap(err, "f.SetCellValue Failed")
                        return
                    }
                    style, err3 := f.NewStyle(def.StyleDel)
                    if err3 != nil {
                        errChan <- err3
                        return
                    }
                    err = f.SetCellStyle(sheetName, cell, cell, style)
                    if err != nil {
                        errChan <- gerror.Wrap(err, "f.SetCellStyle Failed")
                        return
                    }
                }
            }
            i++
        } else {
            isAdd := false
            for _, column := range diffColumnList {
                switch diffColumnMap[column.ColumnName] {
                case def.DiffColumnStateAdd, def.DiffColumnStateDel:
                    isAdd = true
                default:
                    sourceValue := sourceTableData[column.ColumnName]
                    targetValue := targetTableDataMap[k][column.ColumnName]
                    if sourceBytes, ok1 := sourceValue.([]byte); ok1 {
                        sourceValue = string(sourceBytes)
                    }
                    if targetBytes, ok1 := targetValue.([]byte); ok1 {
                        targetValue = string(targetBytes)
                    }
                    if sourceValue != targetValue {
                        isAdd = true
                    }
                }
            }
            if isAdd {
                for kk, column := range diffColumnList {
                    colName, err2 := excelize.ColumnNumberToName(kk + 1)
                    if err2 != nil {
                        errChan <- err2
                        return
                    }
                    cell := fmt.Sprintf("%s%d", colName, i+3)
                    switch diffColumnMap[column.ColumnName] {
                    case def.DiffColumnStateAdd:
                        value := sourceTableData[column.ColumnName]
                        if v, ok1 := value.(time.Time); ok1 {
                            value = carbon.CreateFromStdTime(v).ToDateTimeString()
                        }
                        if bytes, ok1 := value.([]byte); ok1 {
                            value = string(bytes)
                        }
                        err = f.SetCellValue(sheetName, cell, value)
                        if err != nil {
                            errChan <- gerror.Wrap(err, "f.SetCellValue Failed")
                            return
                        }
                        style, err3 := f.NewStyle(def.StyleAdd)
                        if err3 != nil {
                            errChan <- err3
                            return
                        }
                        err = f.SetCellStyle(sheetName, cell, cell, style)
                        if err != nil {
                            errChan <- gerror.Wrap(err, "f.SetCellStyle Failed")
                            return
                        }
                    case def.DiffColumnStateDel:
                        value := targetTableDataMap[k][column.ColumnName]
                        if v, ok1 := value.(time.Time); ok1 {
                            value = carbon.CreateFromStdTime(v).ToDateTimeString()
                        }
                        if bytes, ok1 := value.([]byte); ok1 {
                            value = string(bytes)
                        }
                        err = f.SetCellValue(sheetName, cell, value)
                        if err != nil {
                            errChan <- gerror.Wrap(err, "f.SetCellValue Failed")
                            return
                        }
                        style, err3 := f.NewStyle(def.StyleDel)
                        if err3 != nil {
                            errChan <- err3
                            return
                        }
                        err = f.SetCellStyle(sheetName, cell, cell, style)
                        if err != nil {
                            errChan <- gerror.Wrap(err, "f.SetCellStyle Failed")
                            return
                        }
                    default:
                        sourceValue := sourceTableData[column.ColumnName]
                        targetValue := targetTableDataMap[k][column.ColumnName]
                        if v, ok1 := sourceValue.(time.Time); ok1 {
                            sourceValue = carbon.CreateFromStdTime(v).ToDateTimeString()
                        }
                        if v, ok1 := targetValue.(time.Time); ok1 {
                            targetValue = carbon.CreateFromStdTime(v).ToDateTimeString()
                        }
                        if sourceBytes, ok1 := sourceValue.([]byte); ok1 {
                            sourceValue = string(sourceBytes)
                        }
                        if targetBytes, ok1 := targetValue.([]byte); ok1 {
                            targetValue = string(targetBytes)
                        }
                        if sourceValue != targetValue {
                            err = f.SetCellValue(sheetName, cell, fmt.Sprintf("%v←%v", sourceValue, targetValue))
                            if err != nil {
                                errChan <- gerror.Wrap(err, "f.SetCellValue Failed")
                                return
                            }
                            style, err3 := f.NewStyle(def.StyleDiff)
                            if err3 != nil {
                                errChan <- err3
                                return
                            }
                            err = f.SetCellStyle(sheetName, cell, cell, style)
                            if err != nil {
                                errChan <- gerror.Wrap(err, "f.SetCellStyle Failed")
                                return
                            }
                        } else {
                            err = f.SetCellValue(sheetName, cell, sourceTableData[column.ColumnName])
                            if err != nil {
                                errChan <- gerror.Wrap(err, "f.SetCellValue Failed")
                                return
                            }
                        }
                    }
                }
                i++
            }
        }
        bar.EwmaIncrement(time.Since(start))
    }
    for _, targetTableData := range targetTableDataList {
        start := time.Now()
        k := td.getPrimaryKeyData(sourcePrimaryKeyList, targetTableData)

        if _, ok := sourceTableDataMap[k]; !ok {
            for kk, column := range diffColumnList {
                colName, err2 := excelize.ColumnNumberToName(kk + 1)
                if err2 != nil {
                    errChan <- err2
                    return
                }
                cell := fmt.Sprintf("%s%d", colName, i+3)
                if diffColumnMap[column.ColumnName] != def.DiffColumnStateAdd {
                    value := targetTableData[column.ColumnName]
                    if bytes, ok1 := value.([]byte); ok1 {
                        value = string(bytes)
                    }
                    err = f.SetCellValue(sheetName, cell, value)
                    if err != nil {
                        errChan <- err
                        return
                    }
                    style, err3 := f.NewStyle(def.StyleDel)
                    if err3 != nil {
                        errChan <- err3
                        return
                    }
                    err = f.SetCellStyle(sheetName, cell, cell, style)
                    if err != nil {
                        errChan <- gerror.Wrap(err, "f.SetCellStyle Failed")
                        return
                    }
                } else {
                    err = f.SetCellValue(sheetName, cell, "")
                    if err != nil {
                        errChan <- gerror.Wrap(err, "f.SetCellValue Failed")
                        return
                    }
                    style, err3 := f.NewStyle(def.StyleAdd)
                    if err3 != nil {
                        errChan <- err3
                        return
                    }
                    err = f.SetCellStyle(sheetName, cell, cell, style)
                    if err != nil {
                        errChan <- gerror.Wrap(err, "f.SetCellStyle Failed")
                        return
                    }
                }
            }
            i++
        }
        bar.EwmaIncrement(time.Since(start))
    }

    start := time.Now()
    if i > 0 {
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
    }
    bar.EwmaIncrement(time.Since(start))
}

func (td *TableDiff) diffColumn(sourceColumnList, targetColumnList []model.Column) ([]model.Column, map[string]def.DiffColumnState) {
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

func (td *TableDiff) getSourcePrimaryKeyList(table model.Table) []string {
    var (
        sourceStatisticList []model.Statistic
        primaryKeyList      []string
        err                 error
    )
    err = td.sourceDb.Table("information_schema.STATISTICS").Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", td.sourceDbConfig.Database, table.TableName).Order("SEQ_IN_INDEX ASC").Find(&sourceStatisticList).Error
    if err != nil {
        glog.Fatal(gerror.Wrapf(err, "sourceDb Table %s STATISTICS Find Failed", table.TableName))
    }
    for _, statistic := range sourceStatisticList {
        primaryKeyList = append(primaryKeyList, statistic.ColumnName)
    }
    return primaryKeyList
}

func (td *TableDiff) getTargetPrimaryKeyList(table model.Table) []string {
    var (
        targetStatisticList []model.Statistic
        primaryKeyList      []string
        err                 error
    )
    err = td.targetDb.Table("information_schema.STATISTICS").Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", td.targetDbConfig.Database, table.TableName).Order("SEQ_IN_INDEX ASC").Find(&targetStatisticList).Error
    if err != nil {
        glog.Fatal(gerror.Wrapf(err, "targetDb Table %s STATISTICS Find Failed", table.TableName))
    }
    for _, statistic := range targetStatisticList {
        primaryKeyList = append(primaryKeyList, statistic.ColumnName)
    }
    return primaryKeyList
}

func (td *TableDiff) getPrimaryKeyData(primaryKeyList []string, record g.MapStrAny) string {
    var primaryData []string
    for _, columnName := range primaryKeyList {
        primaryData = append(primaryData, fmt.Sprintf("%v", record[columnName]))
    }
    return strings.Join(primaryData, "_")
}
