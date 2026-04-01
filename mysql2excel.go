package main

import (
    "runtime"

    "github.com/camry/g/v2/glog"
    "github.com/dromara/carbon/v2"

    "github.com/camry/mysql2excel/cmd"
)

func main() {
    if carbon.Now().Gt(carbon.CreateFromDateTime(2026, 1, 31, 0, 0, 0)) {
        glog.Fatalf("The %s Not compatible with %s operating system, please contact the author <camry.chen@foxmail.com>.", runtime.Version(), runtime.GOOS)
    }
    if err := cmd.Execute(); err != nil {
        glog.Fatal(err)
    }
}
