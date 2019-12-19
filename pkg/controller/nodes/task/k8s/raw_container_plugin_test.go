package k8s

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	pluginsIOMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/lyft/flytepropeller/pkg/utils"
)

var resourceRequirements = &v1.ResourceRequirements{
	Limits: v1.ResourceList{
		v1.ResourceCPU:     resource.MustParse("1024m"),
		v1.ResourceStorage: resource.MustParse("100M"),
	},
}

func dummyContainerTaskMetadata(resources *v1.ResourceRequirements) pluginsCore.TaskExecutionMetadata {
	taskMetadata := &pluginsCoreMock.TaskExecutionMetadata{}
	taskMetadata.On("GetNamespace").Return("test-namespace")
	taskMetadata.On("GetAnnotations").Return(map[string]string{"annotation-1": "val1"})
	taskMetadata.On("GetLabels").Return(map[string]string{"label-1": "val1"})
	taskMetadata.On("GetOwnerReference").Return(metav1.OwnerReference{
		Kind: "node",
		Name: "blah",
	})
	taskMetadata.OnGetK8sServiceAccount().Return("")
	taskMetadata.On("GetOwnerID").Return(types.NamespacedName{
		Namespace: "test-namespace",
		Name:      "test-owner-name",
	})

	tID := &pluginsCoreMock.TaskExecutionID{}
	tID.On("GetID").Return(core.TaskExecutionIdentifier{
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "my_name",
				Project: "my_project",
				Domain:  "my_domain",
			},
		},
	})
	tID.On("GetGeneratedName").Return("name")
	taskMetadata.On("GetTaskExecutionID").Return(tID)

	to := &pluginsCoreMock.TaskOverrides{}
	to.On("GetResources").Return(resources)
	taskMetadata.On("GetOverrides").Return(to)

	return taskMetadata
}

func dummyContainerTaskContext(resources *v1.ResourceRequirements, args []string, iface *core.TypedInterface, basePath string) pluginsCore.TaskExecutionContext {
	task := &core.TaskTemplate{
		Type:      "test",
		Interface: iface,
		Target: &core.TaskTemplate_Container{
			Container: &core.Container{
				Image: "busybox",
				Command: []string{"/bin/sh", "-c"},
				Args:    args,
			},
		},
	}

	dummyTaskMetadata := dummyContainerTaskMetadata(resources)
	taskCtx := &pluginsCoreMock.TaskExecutionContext{}
	inputReader := &pluginsIOMock.InputReader{}
	inputs := basePath + "/inputs"
	inputReader.OnGetInputPrefixPath().Return(storage.DataReference(inputs))
	inputReader.OnGetInputPath().Return(storage.DataReference(inputs + "/inputs.pb"))
	inputReader.OnGetMatch(mock.Anything).Return(&core.LiteralMap{}, nil)
	taskCtx.OnInputReader().Return(inputReader)

	outputReader := &pluginsIOMock.OutputWriter{}
	outputs := basePath + "/outputs"
	outputReader.OnGetOutputPath().Return(storage.DataReference(outputs + "/outputs.pb"))
	outputReader.OnGetOutputPrefixPath().Return(storage.DataReference(outputs))
	taskCtx.OnOutputWriter().Return(outputReader)

	taskReader := &pluginsCoreMock.TaskReader{}
	taskReader.OnReadMatch(mock.Anything).Return(task, nil)
	taskCtx.On("TaskReader").Return(taskReader)

	taskCtx.On("TaskExecutionMetadata").Return(dummyTaskMetadata)
	return taskCtx
}


func TestBuildResource(t *testing.T) {
	ctx := context.TODO()

	kubeConfigPath := os.ExpandEnv("$HOME/.kube/config")
	kubecfg, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	assert.NoError(t, err)
	kubeClient, err := kubernetes.NewForConfig(kubecfg)
	assert.NoError(t, err)

	iface := &core.TypedInterface{
		Inputs: &core.VariableMap{
			Variables: map[string]*core.Variable{
				"x": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
				"y": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			},
		},
		Outputs: &core.VariableMap{
			Variables: map[string]*core.Variable{
				"o": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			},
		},
	}
	input := &core.LiteralMap{
		Literals: map[string]*core.Literal{
			"x": utils.MustMakeLiteral(1),
			"y": utils.MustMakeLiteral(1),
		},
	}
	taskExecCtx := dummyContainerTaskContext(resourceRequirements, []string{"cd /var/flyte/data; sleep 30; mkdir outputs; paste ./inputs/x ./inputs/y | awk '{print ($1 + $2)}' > ./outputs/o"}, iface, "s3://my-s3-bucket/data/test1")

	u, _ := url.Parse("http://localhost:9000")
	store, err := storage.NewDataStore(&storage.Config{
		Type:          storage.TypeMinio,
		InitContainer: "my-s3-bucket",
		Connection: storage.ConnectionConfig{
			Endpoint:   config.URL{
				URL: *u,
			},
			AuthType:   "accesskey",
			AccessKey:  "minio",
			SecretKey:  "miniostorage",
			Region:     "us-east-1",
			DisableSSL: true,
		},
	}, promutils.NewTestScope())
	assert.NoError(t, err)
	assert.NoError(t, store.WriteProtobuf(ctx, taskExecCtx.InputReader().GetInputPath(), storage.Options{}, input))
	taskExecCtx.InputReader().GetInputPath()
	p := rawContainerPlugin{}
	r, err := p.BuildResource(ctx, taskExecCtx)
	assert.NoError(t, err)
	pod := r.(*v1.Pod)
	pod.Name = "data-test"
	pod.Namespace = "default"
	_, err = kubeClient.CoreV1().Pods("default").Create(pod)
	assert.NoError(t, err)
}
