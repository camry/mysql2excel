package cmd

import "github.com/spf13/cobra"

var (
    rootCmd = &cobra.Command{
        Use:   "mysql2excel",
        Short: "mysql2excel 是一个允许您将数据从 MySQL 数据库导出到 Excel 文件的工具。",
        CompletionOptions: cobra.CompletionOptions{
            HiddenDefaultCmd: true,
        },
        Version: "v1.0.1",
    }
)

func Execute() error {
    return rootCmd.Execute()
}

func init() {
    initAddCmd()
}

func initAddCmd() {
    rootCmd.AddCommand(dumpCmd)
    rootCmd.AddCommand(diffCmd)
}
