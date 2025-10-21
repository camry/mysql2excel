package cmd

import (
    "fmt"
    "os"
    "regexp"
    "strconv"
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
    "github.com/spf13/cobra"
    "github.com/vbauerster/mpb/v8"
    "github.com/vbauerster/mpb/v8/decor"
    "github.com/xuri/excelize/v2"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "gorm.io/gorm/logger"

    "mysql2excel/internal/def"
    "mysql2excel/internal/excel"
    "mysql2excel/internal/model"
)

var (
    dumpSource        string
    dumpDb            string
    dumpIncludeTables []string
    dumpExcludeTables []string

    dumpCmd = &cobra.Command{
        Use:   "dump",
        Short: "将数据从指定的 MySQL 数据库导出到 Excel 文件",
        Run: func(cmd *cobra.Command, args []string) {
            sourceMatched, err := regexp.MatchString(def.HostPattern, dumpSource)
            if err != nil {
                glog.Fatal(gerror.Wrap(err, "regexp.MatchString Failed"))
            }
            if !sourceMatched {
                glog.Fatal(gerror.Newf("源服务器 `%s` 格式错误。(正确格式: <user>:<password>@<host>:<port>)", dumpSource))
            }
            var (
                sourceUser = strings.Split(dumpSource[0:strings.LastIndex(dumpSource, "@")], ":")
                sourceHost = strings.Split(dumpSource[strings.LastIndex(dumpSource, "@")+1:], ":")

                tableList      []model.Table
                finalTableList []model.Table
            )
            sourceDbConfig := model.DbConfig{
                User:     sourceUser[0],
                Password: sourceUser[1],
                Host:     sourceHost[0],
                Charset:  "utf8mb4",
                Database: dumpDb,
            }
            sourceDbConfig.Port, err = strconv.Atoi(sourceHost[1])
            if err != nil {
                glog.Fatal(gerror.Wrap(err, "strconv.Atoi Failed"))
            }
            sourceDb, err := gorm.Open(mysql.New(mysql.Config{
                DSN: fmt.Sprintf(def.Dsn,
                    sourceDbConfig.User, sourceDbConfig.Password,
                    sourceDbConfig.Host, sourceDbConfig.Port,
                    sourceDbConfig.Database,
                    sourceDbConfig.Charset,
                ),
            }), &gorm.Config{
                SkipDefaultTransaction: true,
                DisableAutomaticPing:   true,
                Logger:                 logger.Default.LogMode(logger.Silent),
            })
            if err != nil {
                glog.Fatal(gerror.Wrap(err, "gorm.Open Failed"))
            }
            err = sourceDb.Table("information_schema.TABLES").Where("TABLE_SCHEMA = ?", dumpDb).Find(&tableList).Error
            if err != nil {
                glog.Fatal(gerror.Wrap(err, "dumpCmd sourceDb TABLES Find Failed"))
            }
            finalTableList = lo.Filter(tableList, func(table model.Table, index int) bool {
                c1 := len(dumpIncludeTables) > 0
                c2 := len(dumpExcludeTables) > 0
                if c1 && c2 {
                    return lo.Contains(dumpIncludeTables, table.TableName) && !lo.Contains(dumpExcludeTables, table.TableName)
                }
                if c1 {
                    return lo.Contains(dumpIncludeTables, table.TableName)
                }
                if c2 {
                    return !lo.Contains(dumpExcludeTables, table.TableName)
                }
                return true
            })
            tableChunkList := lo.Chunk(finalTableList, 10)
            for _, tableChunk := range tableChunkList {
                errChan := make(chan error, 10)
                var wg sync.WaitGroup
                p := mpb.New(mpb.WithWaitGroup(&wg))
                wg.Add(len(tableChunk))
                for _, table := range tableChunk {
                    tableName := fmt.Sprintf("%s.%s", table.TableSchema, table.TableName)
                    count := int64(0)
                    limit := int32(10000)
                    page := int32(1)

                    err = sourceDb.Table(tableName).Count(&count).Error
                    if err != nil {
                        glog.Fatal(gerror.Wrapf(err, "dumpCmd TableName %s Count Failed", tableName))
                    }
                    if count > 0 {
                        page = fp.F64FromInt64(count).DivPrecise(fp.F64FromInt32(limit)).CeilToInt()
                    }

                    bar := p.AddBar(count+1,
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

                    go func() {
                        defer wg.Done()

                        xlsxName := excel.FilterXlsxName(fmt.Sprintf("%s（%s）", table.TableName, table.TableComment))
                        sheetName := lo.Substring(table.TableName, 0, 31)

                        var columnList []model.Column
                        err := sourceDb.Table("information_schema.COLUMNS").Where("TABLE_SCHEMA = ? AND TABLE_NAME = ?", table.TableSchema, table.TableName).Order("ORDINAL_POSITION ASC").Find(&columnList).Error
                        if err != nil {
                            glog.Fatal(gerror.Wrap(err, "sourceDb Table COLUMNS Find Failed"))
                        }

                        xlsxFile, err := excel.NewFile(sheetName)
                        if err != nil {
                            glog.Fatal(gerror.Wrap(err, "excel.NewFile Failed"))
                        }
                        f := xlsxFile.File

                        // Sheet Title
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

                            err1 = f.SetCellValue(sheetName, fmt.Sprintf("%s1", colName), column.ColumnName)
                            if err1 != nil {
                                glog.Fatal(gerror.Wrap(err1, "f.SetCellValue Failed"))
                            }
                            err1 = f.SetCellValue(sheetName, fmt.Sprintf("%s2", colName), column.ColumnComment)
                            if err1 != nil {
                                glog.Fatal(gerror.Wrap(err1, "f.SetCellValue Failed"))
                            }
                        }

                        for curPage := int32(1); curPage <= page; curPage++ {
                            offset := limit * (curPage - 1)
                            var (
                                results []g.MapStrAny
                                err1    error
                            )
                            err1 = sourceDb.Table(tableName).Offset(int(offset)).Limit(int(limit)).Find(&results).Error
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
                                                colName, err1 := excelize.ColumnNumberToName(column.OrdinalPosition)
                                                if err1 != nil {
                                                    errChan <- gerror.Wrap(err1, "excelize.ColumnNumberToName Failed")
                                                    return
                                                }
                                                if v, ok1 := columnValue.(time.Time); ok1 {
                                                    columnValue = carbon.CreateFromStdTime(v).ToDateTimeString()
                                                }
                                                if bytes, ok1 := columnValue.([]byte); ok1 {
                                                    columnValue = string(bytes)
                                                }
                                                err1 = f.SetCellValue(sheetName, fmt.Sprintf("%s%d", colName, int(offset)+k+3), columnValue)
                                                if err1 != nil {
                                                    errChan <- gerror.Wrap(err1, "f.SetCellValue Failed")
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
                        path := fmt.Sprintf("dump/%s", table.TableSchema)
                        _, err = os.Stat(path)
                        if os.IsNotExist(err) {
                            err = os.MkdirAll(path, os.ModePerm)
                            if err != nil {
                                glog.Fatal(gerror.Wrap(err, "os.MkdirAll Failed"))
                            }
                        }
                        err = f.SaveAs(fmt.Sprintf("%s/%s.xlsx", path, xlsxName))
                        if err != nil {
                            glog.Fatal(gerror.Wrap(err, "f.SaveAs Failed"))
                        }
                        bar.EwmaIncrement(time.Since(start))
                    }()
                }
                p.Wait()
                close(errChan)
                for errC := range errChan {
                    if errC != nil {
                        glog.Fatal(errC)
                    }
                }
            }
        },
    }
)

func init() {
    dumpCmd.Flags().StringVarP(&dumpSource, "source", "s", "", "指定源服务器。(格式: <user>:<password>@<host>:<port>)")
    dumpCmd.Flags().StringVarP(&dumpDb, "db", "d", "", "指定数据库。(格式: <source_db>:<target_db>)")
    dumpCmd.Flags().StringSliceVarP(&dumpIncludeTables, "include-tables", "i", []string{}, "指定包含的表。(格式：base_activity,base_activity_limit)")
    dumpCmd.Flags().StringSliceVarP(&dumpExcludeTables, "exclude-tables", "e", []string{}, "指定排除的表。(格式：base_dist,base_sdk)")
    cobra.CheckErr(dumpCmd.MarkFlagRequired("source"))
    cobra.CheckErr(dumpCmd.MarkFlagRequired("db"))
}
