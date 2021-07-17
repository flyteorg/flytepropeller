package cmd

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/lyft/flytestdlib/logger"
	"github.com/spf13/cobra"

	extender2 "github.com/lyft/flytepropeller/k8s-scheduler/pkg/extender"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
)


// TODO ssingh: move it to a type
var extender extender2.FlyteSchedulerExtender

const (
	filterPath = "/filter"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "runs the scheduler extender server",
	RunE: func(cmd *cobra.Command, args []string) error {
		serve()
		return nil
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
}

func filterNodes(w http.ResponseWriter, req *http.Request) {
	decoder := json.NewDecoder(req.Body)
	var args schedulerapi.ExtenderArgs
	err := decoder.Decode(&args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		result, err := extender.Filter(&args)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		writeJsonResponse(w, result, http.StatusOK)
	}
}


func writeJsonResponse(w http.ResponseWriter, obj interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(obj); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func serve() {
	ctx := context.Background()
	http.HandleFunc(filterPath, filterNodes)

	logger.Info(ctx," extender server starting on the port :80")
	if err := http.ListenAndServe(":80", nil); err != nil {
		logger.Fatal(ctx, err)
	}
}