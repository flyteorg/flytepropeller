// Package plugins facilitates all the plugins that should be loaded by FlytePropeller
package plugins

import (
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/array/awsbatch"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/array/k8s"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/hive"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/k8s/kfoperators/mpi"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/k8s/kfoperators/pytorch"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/k8s/pod"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/k8s/sagemaker"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/k8s/spark"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/webapi/athena"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/webapi/bigquery"
	_ "github.com/flyteorg/flyteplugins/go/tasks/plugins/webapi/snowflake"
)
