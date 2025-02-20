package cmd

import "github.com/spf13/cobra"

var (
    diffSource string
    diffTarget string
    diffDb     string

    diffCmd = &cobra.Command{
        Use:   "diff",
        Short: "比较两个 MySQL 数据库之间的差异数据导出到 Excel 文件。",
        Run: func(cmd *cobra.Command, args []string) {

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
