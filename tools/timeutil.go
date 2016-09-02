package mytools

import (
	"fmt"
	"time"
)

var Datestr string

func GetTimeString() string {
	t := time.Now()
	return fmt.Sprintf("[%02d:%02d:%02d]", t.Hour(), t.Minute(), t.Second())
}

func GetDateString() string {
	if Datestr != "" {
		return Datestr
	}

	t := time.Now()
	return fmt.Sprintf("%d/%d/%d", t.Year(), t.Month(), t.Day())
}
