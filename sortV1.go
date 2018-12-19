package main

import (
	"SortPro/lib"
	"fmt"
)

func main() {
	fileName := "data.in"
	n := 4
	total := 800000000
	ch := lib.DataSort(fileName,total, n)
	writeName := "data.out"
	err := lib.WriteData(writeName, ch)
	if err != nil {
		panic(fmt.Sprintf("write error : %s", err.Error()))
	}
}
