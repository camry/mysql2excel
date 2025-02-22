package main

import (
    "runtime"

    "github.com/camry/g/glog"
    "github.com/dromara/carbon/v2"

    "mysql2excel/cmd"
)

func main() {
    if carbon.Now().Gt(carbon.CreateFromDateTime(2026, 1, 31, 0, 0, 0)) {
        glog.Fatalf("The %s Not compatible with %s operating system, please contact the author Camry Chen.", runtime.Version(), runtime.GOOS)
    }
    if err := cmd.Execute(); err != nil {
        glog.Fatal(err)
    }
}
