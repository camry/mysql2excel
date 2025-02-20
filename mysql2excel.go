package main

import (
    "github.com/camry/g/glog"

    "mysql2excel/cmd"
)

func main() {
    if err := cmd.Execute(); err != nil {
        glog.Fatal(err)
    }
}
