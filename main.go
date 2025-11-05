package main

import (
	"github.com/AustralianCyberSecurityCentre/azul-backup.git/cmd"
	_ "go.uber.org/automaxprocs"
)

func main() {
	cmd.Execute()
}
