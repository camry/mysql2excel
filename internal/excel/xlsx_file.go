package excel

import (
    "strings"

    "github.com/dromara/carbon/v2"
    "github.com/xuri/excelize/v2"
)

func FilterXlsxName(xlsxName string) string {
    xlsxName = strings.ReplaceAll(xlsxName, "/", "")
    xlsxName = strings.ReplaceAll(xlsxName, "\\", "")
    xlsxName = strings.ReplaceAll(xlsxName, ":", "")
    xlsxName = strings.ReplaceAll(xlsxName, "*", "")
    xlsxName = strings.ReplaceAll(xlsxName, "?", "")
    xlsxName = strings.ReplaceAll(xlsxName, "\"", "")
    xlsxName = strings.ReplaceAll(xlsxName, "<", "")
    xlsxName = strings.ReplaceAll(xlsxName, ">", "")
    xlsxName = strings.ReplaceAll(xlsxName, "|", "")

    return xlsxName
}

type XlsxFile struct {
    File *excelize.File
}

func NewFile() (*XlsxFile, error) {
    file := excelize.NewFile()

    err := file.SetDocProps(&excelize.DocProperties{
        Created:        carbon.Now().ToIso8601String(),
        Creator:        "Camry.Chen (陈庚茂)",
        Identifier:     "xlsx",
        LastModifiedBy: "Camry.Chen (陈庚茂)",
        Language:       "zh-CN",
        Modified:       carbon.Now().ToIso8601String(),
    })
    if err != nil {
        return nil, err
    }

    index, err := file.NewSheet("Sheet1")
    if err != nil {
        return nil, err
    }
    file.SetActiveSheet(index)

    return &XlsxFile{File: file}, nil
}
