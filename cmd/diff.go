package cmd

import (
    "fmt"
    "regexp"
    "strconv"
    "strings"

    "github.com/camry/g/gerrors/gerror"
    "github.com/camry/g/glog"
    "github.com/spf13/cobra"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
    "gorm.io/gorm/logger"

    "mysql2excel/internal/diff"
    "mysql2excel/internal/model"
)

var (
    diffSource string
    diffTarget string
    diffDb     string

    diffCmd = &cobra.Command{
        Use:   "diff",
        Short: "比较两个 MySQL 数据库之间的差异数据导出到 Excel 文件。",
        Run: func(cmd *cobra.Command, args []string) {
            sourceMatched, err := regexp.MatchString(HostPattern, diffSource)
            if err != nil {
                glog.Fatal(gerror.Wrapf(err, "regexp.MatchString HostPattern diffSource %s Failed", diffSource))
            }
            dbMatched, err := regexp.MatchString(DbPattern, diffDb)
            if err != nil {
                glog.Fatal(gerror.Wrapf(err, "regexp.MatchString DbPattern diffDb %s Failed", diffDb))
            }
            if !sourceMatched {
                cobra.CheckErr(fmt.Errorf("源服务器 `%s` 格式错误。(正确格式: <user>:<password>@<host>:<port>)", diffSource))
            }
            if !dbMatched {
                cobra.CheckErr(fmt.Errorf("数据库 `%s` 格式错误。(正确格式: <source_db>:<target_db>)", diffDb))
            }

            sourceUser := strings.Split(diffSource[0:strings.LastIndex(diffSource, "@")], ":")
            sourceHost := strings.Split(diffSource[strings.LastIndex(diffSource, "@")+1:], ":")
            databases := strings.Split(diffDb, ":")

            sourceDbConfig := model.DbConfig{
                User:     sourceUser[0],
                Password: sourceUser[1],
                Host:     sourceHost[0],
                Charset:  "utf8mb4",
                Database: databases[0],
            }
            sourceDbConfig.Port, err = strconv.Atoi(sourceHost[1])
            if err != nil {
                glog.Fatal(gerror.Wrap(err, "strconv.Atoi Failed"))
            }
            sourceDb, err := gorm.Open(mysql.New(mysql.Config{
                DSN: fmt.Sprintf(Dsn,
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
                glog.Fatal(gerror.Wrap(err, "sourceDb gorm.Open Failed"))
            }

            targetDbConfig := model.DbConfig{
                Charset:  "utf8mb4",
                Database: databases[1],
            }
            var targetDb *gorm.DB
            if diffTarget != "" {
                targetMatched, err1 := regexp.MatchString(HostPattern, diffTarget)
                if err1 != nil {
                    glog.Fatal(gerror.Wrapf(err1, "regexp.MatchString HostPattern diffTarget %s Failed", diffTarget))
                }
                if !targetMatched {
                    cobra.CheckErr(fmt.Errorf("目标服务器 `%s` 格式错误。(正确格式: <user>:<password>@<host>:<port>)", diffTarget))
                }
                targetUser := strings.Split(diffTarget[0:strings.LastIndex(diffTarget, "@")], ":")
                targetHost := strings.Split(diffTarget[strings.LastIndex(diffTarget, "@")+1:], ":")
                targetDbConfig.User = targetUser[0]
                targetDbConfig.Password = targetUser[1]
                targetDbConfig.Host = targetHost[0]
                targetDbConfig.Port, err = strconv.Atoi(targetHost[1])
                if err != nil {
                    glog.Fatal(gerror.Wrap(err, "strconv.Atoi Failed"))
                }
                targetDb, err = gorm.Open(mysql.New(mysql.Config{
                    DSN: fmt.Sprintf(Dsn,
                        targetDbConfig.User, targetDbConfig.Password,
                        targetDbConfig.Host, targetDbConfig.Port,
                        targetDbConfig.Database,
                        targetDbConfig.Charset,
                    ),
                }), &gorm.Config{
                    SkipDefaultTransaction: true,
                    DisableAutomaticPing:   true,
                    Logger:                 logger.Default.LogMode(logger.Silent),
                })
                if err != nil {
                    glog.Fatal(gerror.Wrap(err, "targetDb gorm.Open Failed"))
                }
            } else {
                targetDb = sourceDb
            }

            tableDiff := diff.NewTableDiff(sourceDb, targetDb, sourceDbConfig, targetDbConfig)
            err = tableDiff.Run()
            if err != nil {
                glog.Fatal(gerror.Wrap(err, "tableDiff.Run Failed"))
            }
        },
    }
)

func init() {
    diffCmd.Flags().StringVarP(&diffSource, "source", "s", "", "指定源服务器。(格式: <user>:<password>@<host>:<port>)")
    diffCmd.Flags().StringVarP(&diffTarget, "target", "t", "", "指定目标服务器。(格式: <user>:<password>@<host>:<port>)")
    diffCmd.Flags().StringVarP(&diffDb, "db", "d", "", "指定数据库。(格式: <source_db>:<target_db>)")
    cobra.CheckErr(diffCmd.MarkFlagRequired("source"))
    cobra.CheckErr(diffCmd.MarkFlagRequired("db"))
}
