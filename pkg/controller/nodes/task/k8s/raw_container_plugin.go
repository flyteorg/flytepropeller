package k8s

import (
	"context"
	"encoding/base64"

	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/logs"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
)

const (
	containerTaskType = "raw_container"
	flyteDataVolume   = "flyte-data-vol"
	flyteDataPath     = "/var/flyte/data"
)

func FlyteDataContainer() v1.Container {

	return v1.Container{
		Name:       "flyteData",
		Image:      "test-data",
		Command:    []string{"/bin/flytedata"},
		Args:       nil,
		WorkingDir: "/",
		Resources:  v1.ResourceRequirements{},
		VolumeMounts: []v1.VolumeMount{
			{
				Name:      flyteDataVolume,
				MountPath: flyteDataPath, // TODO maybe we can restrict this to uploader and download only
			},
		},
		TerminationMessagePolicy: v1.TerminationMessageFallbackToLogsOnError,
	}
}

func UploadCommandArgs(fromLocalPath string, outputPrefix, outputSandbox storage.DataReference, outputInterface *core.VariableMap) ([]string, error) {
	args := []string{
		"upload",
		"--to-sandbox",
		outputSandbox.String(),
		"--to-output-prefix",
		outputPrefix.String(),
		"--from-local-dir",
		fromLocalPath,
	}
	if outputInterface != nil {
		b, err := proto.Marshal(outputInterface)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal given output interface")
		}
		args = append(args, "--output-interface", base64.StdEncoding.EncodeToString(b))
	}
	return args, nil
}

func DownloadCommandArgs(fromInputsPath, outputPrefix storage.DataReference, toLocalPath string) []string {
	return []string{
		"download",
		"--from-remote",
		fromInputsPath.String(),
		"--to-output-prefix",
		outputPrefix.String(),
		"--to-local-dir",
		toLocalPath,
	}
}

func ToK8sPodSpec(ctx context.Context, taskExecutionMetadata pluginsCore.TaskExecutionMetadata, taskReader pluginsCore.TaskReader,
	inputs io.InputReader, outputPaths io.OutputFilePaths) (*v1.PodSpec, error) {
	task, err := taskReader.Read(ctx)
	if err != nil {
		logger.Warnf(ctx, "failed to read task information when trying to construct Pod, err: %s", err.Error())
		return nil, err
	}
	c, err := flytek8s.ToK8sContainer(ctx, taskExecutionMetadata, task.GetContainer(), inputs, outputPaths)
	if err != nil {
		return nil, err
	}

	remoteInputPath := inputs.GetInputPath()
	remoteOutputPrefix := outputPaths.GetOutputPrefixPath()
	flyteOutputPath := flyteDataPath + "/outputs/"
	flyteInputPath := flyteDataPath + "/inputs/"
	var outputInterface *core.VariableMap
	if task.Interface != nil {
		outputInterface = task.Interface.Outputs
	}

	flyteData := FlyteDataContainer()

	uploader := flyteData.DeepCopy()
	uploaderArgs, err := UploadCommandArgs(flyteOutputPath, remoteOutputPrefix, remoteOutputPrefix, outputInterface)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create uploader arguments")
	}
	uploader.Args = uploaderArgs
	containers := []v1.Container{
		*c,
		*uploader,
	}

	downloader := flyteData.DeepCopy()
	downloader.Args = DownloadCommandArgs(remoteInputPath, remoteOutputPrefix, flyteInputPath)
	initContainers := []v1.Container{
		*downloader,
	}

	shareProcessNamespaceEnabled := true

	return &v1.PodSpec{
		// We could specify Scheduler, Affinity, nodename etc
		RestartPolicy:         v1.RestartPolicyNever,
		Containers:            containers,
		InitContainers:        initContainers,
		Tolerations:           flytek8s.GetTolerationsForResources(c.Resources),
		ServiceAccountName:    taskExecutionMetadata.GetK8sServiceAccount(),
		ShareProcessNamespace: &shareProcessNamespaceEnabled,
		Volumes: []v1.Volume{
			{
				Name: flyteDataVolume,
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{},
				},
			},
		},
	}, nil
}

type rawContainerPlugin struct {
}

func (rawContainerPlugin) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, r k8s.Resource) (pluginsCore.PhaseInfo, error) {

	pod := r.(*v1.Pod)

	t := flytek8s.GetLastTransitionOccurredAt(pod).Time
	info := pluginsCore.TaskInfo{
		OccurredAt: &t,
	}
	if pod.Status.Phase != v1.PodPending && pod.Status.Phase != v1.PodUnknown {
		taskLogs, err := logs.GetLogsForContainerInPod(ctx, pod, 0, " (User)")
		if err != nil {
			return pluginsCore.PhaseInfoUndefined, err
		}
		info.Logs = taskLogs
	}
	switch pod.Status.Phase {
	case v1.PodSucceeded:
		return pluginsCore.PhaseInfoSuccess(&info), nil
	case v1.PodFailed:
		code, message := flytek8s.ConvertPodFailureToError(pod.Status)
		return pluginsCore.PhaseInfoRetryableFailure(code, message, &info), nil
	case v1.PodPending:
		return flytek8s.DemystifyPending(pod.Status)
	case v1.PodUnknown:
		return pluginsCore.PhaseInfoUndefined, nil
	}
	if len(info.Logs) > 0 {
		return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion+1, &info), nil
	}
	return pluginsCore.PhaseInfoRunning(pluginsCore.DefaultPhaseVersion, &info), nil
}

// Creates a new Pod that will Exit on completion. The pods have no retries by design
func (rawContainerPlugin) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {

	podSpec, err := ToK8sPodSpec(ctx, taskCtx.TaskExecutionMetadata(), taskCtx.TaskReader(), taskCtx.InputReader(), taskCtx.OutputWriter())
	if err != nil {
		return nil, err
	}

	pod := flytek8s.BuildPodWithSpec(podSpec)

	// We want to Also update the serviceAccount to the serviceaccount of the workflow
	pod.Spec.ServiceAccountName = taskCtx.TaskExecutionMetadata().GetK8sServiceAccount()

	return pod, nil
}

func (rawContainerPlugin) BuildIdentityResource(_ context.Context, _ pluginsCore.TaskExecutionMetadata) (k8s.Resource, error) {
	return flytek8s.BuildIdentityPod(), nil
}

func init() {
	pluginmachinery.PluginRegistry().RegisterK8sPlugin(
		k8s.PluginEntry{
			ID:                  containerTaskType,
			RegisteredTaskTypes: []pluginsCore.TaskType{containerTaskType},
			ResourceToWatch:     &v1.Pod{},
			Plugin:              rawContainerPlugin{},
			IsDefault:           false,
		})
}
