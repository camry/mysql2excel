package main

import (
    "github.com/camry/g/v2/glog"

    "github.com/camry/mysql2excel/cmd"
)

func main() {
    if err := cmd.Execute(); err != nil {
        glog.Fatal(err)
    }
}
