package main

import (
	"fmt"
	"os"

	"github.com/lyft/flytepropeller/cmd/build-tool/cmd"
)

func main() {

	rootCmd := cmd.NewBuildToolCommand()
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
