package main

import (
	_ "github.com/lyft/flyteplugins/go/tasks/array/k8s"
	_ "github.com/lyft/flyteplugins/go/tasks/plugins/k8s/container"

	"github.com/lyft/flytepropeller/cmd/controller/cmd"
)

func main() {
	cmd.Execute()
}
