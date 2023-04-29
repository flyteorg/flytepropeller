package resourceRequest

import (
	"context"
	v1alpha12 "github.com/lyft/flytepropeller/k8s-scheduler/pkg/apis/resourceRequest/v1alpha1"
	config2 "github.com/lyft/flytepropeller/k8s-scheduler/pkg/controller/config"
	"sort"
	"time"

	kcorev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/controller-runtime/pkg/cache"

	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
)

type Handler interface {
	Handle(ctx context.Context, resourceRequest *v1alpha12.ResourceRequest) (bool, error)
	IsRequestApproved(ctx context.Context, resourceRequestName string) (bool, error)
}

type requestInfo struct {
	submittedAt     time.Time
	startByTime     time.Time
	totalCpu        resource.Quantity
	totalMemory     resource.Quantity
	resourceRequest *v1alpha12.ResourceRequest
	name            string
	phase           v1alpha12.ResourceRequestPhase
}

type resourceRequestHandler struct {
	requestMap          map[string]*requestInfo
	requestList         []*requestInfo
	metrics             *resourceRequestHandlerMetrics
	eventRecorder       record.EventRecorder
	podCache            cache.Cache
	totalCpuCapacity    resource.Quantity
	totalMemoryCapacity resource.Quantity
}

func NewResourceRequestHandler(scope promutils.Scope, eventRecorder record.EventRecorder, config config2.RuntimeConfig) (*resourceRequestHandler, error) {
	cfg := &rest.Config{}
	podCache, err := cache.New(cfg, cache.Options{})
	if err != nil {
		return nil, err
	}

	return &resourceRequestHandler{
		metrics:       newResourceRequestHandlerMetrics(scope),
		eventRecorder: eventRecorder,
		podCache:      podCache,
	}, nil
}

type resourceRequestHandlerMetrics struct {
	scope promutils.Scope
}

func newResourceRequestHandlerMetrics(scope promutils.Scope) *resourceRequestHandlerMetrics {
	return &resourceRequestHandlerMetrics{
		scope: scope,
	}
}

func (s *resourceRequestHandler) Handle(ctx context.Context, application *v1alpha12.ResourceRequest) (bool, error) {

	// TODO ssingh: we shouldn't need do this.
	if !application.ObjectMeta.DeletionTimestamp.IsZero() {
		return false, nil
	}

	if application.Status.Phase == v1alpha12.RequestUnknown {
		application.Status.Phase = v1alpha12.RequestKnown
		s.addNew(application)
	}

	// TODO ssingh: complete the rest of logic
	return true, nil
}

func (s *resourceRequestHandler) IsRequestApproved(ctx context.Context, resourceRequestName string) (bool, error) {
	// TODO ssingh lock
	if i, ok := s.requestMap[resourceRequestName]; ok {
		return i.phase == v1alpha12.RequestAllocated, nil
	}
	return false, nil
}


func (s *resourceRequestHandler) addNew(application *v1alpha12.ResourceRequest) {
	s.requestList = append(s.requestList, &requestInfo{resourceRequest:application})
}


func (s *resourceRequestHandler) processAllocationInBackground(ctx context.Context) {

	allocate := func() {
		// TODO ssingh get the current usage from s.requestMap
		cpu, memory, err := s.getCurrentUsage()
		if err != nil {
			logger.Errorf(ctx, "failed to get current resource usage: %v", err)
			return
		}

		remainingCpuCapacity := s.totalCpuCapacity
		remainingMemoryCapacity := s.totalMemoryCapacity
		remainingCpuCapacity.Sub(cpu)
		remainingMemoryCapacity.Sub(memory)

		// TODO ssingh Lock
		// making a copy complicated way:) => https://github.com/go101/go101/wiki/How-to-perfectly-clone-a-slice%3F
		rl := append(s.requestList[:0:0], s.requestList...)
		sort.Sort(RequestList(rl))
		for _, request := range rl {

			if request.totalCpu.Cmp(remainingCpuCapacity) > 0 ||  request.totalMemory.Cmp(remainingMemoryCapacity) > 0 {
				// no more allocations possible in this round.
				break
			}

			request.phase = v1alpha12.RequestAllocated
			s.requestMap[request.name] = request
			remainingCpuCapacity.Sub(request.totalCpu)
			remainingMemoryCapacity.Sub(request.totalMemory)
		}

		s.removeProcessed()
	}

	stop := make(chan struct{})
	go wait.Until(allocate, 30 * time.Second, stop)
}

// TODO ssingh Lock
func (s *resourceRequestHandler) removeProcessed() {
	tmp := s.requestList[:0]
	for _, p := range s.requestList {
		if p.phase != v1alpha12.RequestAllocated {
			tmp = append(tmp, p)
		}
	}
	s.requestList = tmp
}

func (s *resourceRequestHandler) getCurrentUsage() (resource.Quantity, resource.Quantity, error) {

	var memory resource.Quantity
	var cpu resource.Quantity

	out := &kcorev1.PodList{}
	err := s.podCache.List(context.Background(), out)
	if err != nil {
		return memory, cpu, err
	}

	for _, pod := range out.Items {
		if pod.Status.Phase == kcorev1.PodRunning {
			for _, container := range pod.Spec.Containers {
				memory.Add(*container.Resources.Requests.Memory())
				cpu.Add(*container.Resources.Requests.Cpu())
			}
		}
	}
	return memory, cpu, nil
}


type RequestList []*requestInfo

func (a RequestList) Len() int           { return len(a) }
func (a RequestList) Less(i, j int) bool {

	currentTime := time.Now()

	// requests that do not any queuing budget
	if a[i].startByTime.Before(currentTime) {
		return a[j].startByTime.After(currentTime) || a[i].submittedAt.Before(a[j].submittedAt)
	}

	return a[j].startByTime.After(currentTime) && a[i].submittedAt.Before(a[j].submittedAt)
}

func (a RequestList) Swap(i, j int)  { a[i], a[j] = a[j], a[i] }