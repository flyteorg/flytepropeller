package containercompletion

import (
	"context"
	"strings"
	"time"

	"github.com/lyft/flytestdlib/logger"
	"github.com/mitchellh/go-ps"
	"github.com/pkg/errors"
	"k8s.io/utils/clock"
)

func FilterProcessListByNamespace(procs []ps.Process, ns string) ([]ps.Process, error) {
	var filteredProcs []ps.Process
	for _, p := range procs {
		proc := p
		procExec := proc.Executable()
		splits := strings.SplitN(procExec, ":", 1)
		if len(splits) == 2 {
			splitNs := splits[0]
			if splitNs == ns {
				filteredProcs = append(filteredProcs, proc)
			}
		}
	}
	return filteredProcs, nil
}

// The best option for this is to use https://kubernetes.io/docs/tasks/configure-pod-container/share-process-namespace/
// This is only available as Beta as of 1.16, so we will launch with this feature only as beta
// But this is the most efficient way to monitor the pod
type sharedProcessNSWatcher struct {
	// Fake clock to aid in testing
	c clock.Clock
	// Duration to wait for the container to start
	containerStartTimeout time.Duration
	// Rate at which to poll the process list
	pollInterval time.Duration
	// Number of cycles to wait before finalizing exit of container
	cyclesToWait int
}

func (k sharedProcessNSWatcher) WaitForContainerToStart(ctx context.Context, information ContainerInformation) error {
	t := k.c.NewTimer(k.pollInterval)
	defer t.Stop()
	name := information.Name
	for ; ; {
		select {
		case <-ctx.Done():
			return errors.Errorf("Context canceled while waiting for Container [%s] to start.", name)
		case <-t.C():
			logger.Debugf(ctx, "Checking processes to see if any process was started in namespace [%s]", name)
			procs, err := ps.Processes()
			if err != nil {
				return errors.Wrap(err, "Failed to list processes")
			}
			filteredProcs, err := FilterProcessListByNamespace(procs, information.Name)
			if err != nil {
				return errors.Wrapf(err, "failed to filter processes for namespace %s", information.Name)
			}
			if len(filteredProcs) > 0 {
				logger.Infof(ctx, "Detected processes in namespace [%s]", information.Name)
				return nil
			}
		}
	}
}

func (k sharedProcessNSWatcher) WaitForContainerToExit(ctx context.Context, information ContainerInformation) error {
	t := k.c.NewTimer(k.pollInterval)
	defer t.Stop()
	name := information.Name
	cyclesOfMissingProcesses := 0
	for ; ; {
		select {
		case <-ctx.Done():
			return errors.Errorf("Context canceled while waiting for Container [%s] to exit.", name)
		case <-t.C():
			logger.Debugf(ctx, "Checking processes to see if all processes in namespace [%s] have exited.", name)
			procs, err := ps.Processes()
			if err != nil {
				return errors.Wrap(err, "Failed to list processes")
			}
			filteredProcs, err := FilterProcessListByNamespace(procs, information.Name)
			if err != nil {
				return errors.Wrapf(err, "failed to filter processes for namespace %s", information.Name)
			}
			if len(filteredProcs) == 0 {
				cyclesOfMissingProcesses += 1
				if cyclesOfMissingProcesses >= k.cyclesToWait {
					logger.Infof(ctx, "All Processes in namespace [%s] exited.", information.Name)
					return nil
				}
			}
		}
	}
}

func (k sharedProcessNSWatcher) WaitForContainerToComplete(ctx context.Context, information ContainerInformation) error {
	timeoutCtx := ctx
	if k.containerStartTimeout > 0 {
		timeoutCtx, _ = context.WithTimeout(ctx, k.containerStartTimeout)
	}
	if err := k.WaitForContainerToStart(timeoutCtx, information); err != nil {
		logger.Errorf(ctx, "Failed while waiting for Container to start. Err: %s", err)
		return err
	}
	if err := k.WaitForContainerToExit(ctx, information); err != nil {
		logger.Errorf(ctx, "Failed while waiting for Container to exit. Err: %s", err)
		return err
	}
	return nil
}

// c -> clock.Clock allows for injecting a fake clock. The watcher uses a timer
// pollInterval -> time.Duration, wait for this amount of time between successive process checks
// waitNumIntervalsBeforeFinalize -> Number of successive poll intervals of missing processes for the container, before assuming process is complete. 0/1 indicate the first time a process is detected to be missing, the wait if finalized.
// containerStartupTimeout -> Duration for which to wait for the container to start up. If the container has not started up in this time, exit with error.
func NewSharedProcessNSWatcher(_ context.Context, c clock.Clock, pollInterval, containerStartupTimeout time.Duration, waitNumIntervalsBeforeFinalize int) (Watcher, error) {
	return sharedProcessNSWatcher{
		c:                     c,
		pollInterval:          pollInterval,
		cyclesToWait:          waitNumIntervalsBeforeFinalize,
		containerStartTimeout: containerStartupTimeout,
	}, nil
}
