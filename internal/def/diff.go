package def

import "github.com/xuri/excelize/v2"

type DiffColumnState int8

const (
    DiffColumnStateEqual DiffColumnState = 0
    DiffColumnStateAdd   DiffColumnState = 1
    DiffColumnStateDel   DiffColumnState = 2
)

const SheetNameDefault = "Sheet1"

var StyleAdd = &excelize.Style{
    Border: []excelize.Border{
        {Type: "left", Color: "#adbac7", Style: 1},
        {Type: "top", Color: "#adbac7", Style: 1},
        {Type: "bottom", Color: "#adbac7", Style: 1},
        {Type: "right", Color: "#adbac7", Style: 1},
    },
    Fill: excelize.Fill{
        Type:    "pattern",
        Color:   []string{"#87edc3"},
        Pattern: 1,
    },
}

var StyleDel = &excelize.Style{
    Border: []excelize.Border{
        {Type: "left", Color: "#adbac7", Style: 1},
        {Type: "top", Color: "#adbac7", Style: 1},
        {Type: "bottom", Color: "#adbac7", Style: 1},
        {Type: "right", Color: "#adbac7", Style: 1},
    },
    Fill: excelize.Fill{
        Type:    "pattern",
        Color:   []string{"#ef8282"},
        Pattern: 1,
    },
}

var StyleDiff = &excelize.Style{
    Border: []excelize.Border{
        {Type: "left", Color: "#adbac7", Style: 1},
        {Type: "top", Color: "#adbac7", Style: 1},
        {Type: "bottom", Color: "#adbac7", Style: 1},
        {Type: "right", Color: "#adbac7", Style: 1},
    },
    Fill: excelize.Fill{
        Type:    "pattern",
        Color:   []string{"#e4e04b"},
        Pattern: 1,
    },
}
