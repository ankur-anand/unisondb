package cliapp

import (
	"fmt"
	"strings"

	"github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
)

// PrintBanner print the UnisonDB Banner.
// nolint
func PrintBanner() {
	defer func() {
		fmt.Println("")
	}()

	banner := figure.NewFigure("UnisonDB", "small", true)
	bannerStr := banner.String()
	lines := strings.Split(bannerStr, "\n")

	maxWidth := 0
	for _, line := range lines {
		if len(line) > maxWidth {
			maxWidth = len(line)
		}
	}

	color.New(color.FgCyan, color.Bold).Println(bannerStr)

	centerPrint("Distributed. Durable. Globally Synced.", maxWidth, color.FgHiBlack)
	centerPrint("https://unisondb.io", maxWidth, color.FgHiBlack)
}

func centerPrint(text string, width int, attr color.Attribute) {
	padding := (width - len(text)) / 2
	if padding < 0 {
		padding = 0
	}
	pad := strings.Repeat(" ", padding)
	color.New(attr).Println(pad + text)
}
