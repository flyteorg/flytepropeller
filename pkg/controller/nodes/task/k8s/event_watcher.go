package k8s

import (
	"context"
	"sort"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	informerEventsv1 "k8s.io/client-go/informers/events/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/flyteorg/flytestdlib/logger"
)

type EventWatcher interface {
	List(objectNsName types.NamespacedName, createdAfter time.Time) []*eventsv1.Event
}

type eventWatcher struct {
	ctx          context.Context
	informer     informerEventsv1.EventInformer
	objectEvents map[types.NamespacedName]eventSet
}

type eventSet map[types.NamespacedName]*eventsv1.Event

func (e *eventWatcher) OnAdd(obj interface{}) {
	event := obj.(*eventsv1.Event)
	objectNsName := types.NamespacedName{Namespace: event.Regarding.Namespace, Name: event.Regarding.Name}
	eventNsName := types.NamespacedName{Namespace: event.Namespace, Name: event.Name}
	events, ok := e.objectEvents[objectNsName]
	if !ok {
		events = make(eventSet)
		e.objectEvents[objectNsName] = events
	}
	if _, ok := events[eventNsName]; ok {
		logger.Warnf(e.ctx, "Event add [%s/%s] received for object [%s/%s] that already exists in the cache",
			event.Namespace, event.Name, event.Regarding.Namespace, event.Regarding.Name)
	}
	events[eventNsName] = event
}

func (e *eventWatcher) OnUpdate(_, newObj interface{}) {
	event := newObj.(*eventsv1.Event)
	objectNsName := types.NamespacedName{Namespace: event.Regarding.Namespace, Name: event.Regarding.Name}
	eventNsName := types.NamespacedName{Namespace: event.Namespace, Name: event.Name}
	events, ok := e.objectEvents[objectNsName]
	if !ok {
		logger.Warn(e.ctx, "Event update [%s/%s] received for object [%s/%s] that does not exist in the cache",
			event.Namespace, event.Name, event.Regarding.Namespace, event.Regarding.Name)
		events = make(eventSet)
		e.objectEvents[objectNsName] = events
	}
	events[eventNsName] = event
}

func (e *eventWatcher) OnDelete(obj interface{}) {
	event := obj.(*eventsv1.Event)
	objectNsName := types.NamespacedName{Namespace: event.Regarding.Namespace, Name: event.Regarding.Name}
	eventNsName := types.NamespacedName{Namespace: event.Namespace, Name: event.Name}
	events, ok := e.objectEvents[objectNsName]
	if !ok {
		logger.Warn(e.ctx, "Event delete [%s/%s] received for object [%s/%s] that does not exist in the cache",
			event.Namespace, event.Name, event.Regarding.Namespace, event.Regarding.Name)
		return
	}
	delete(events, eventNsName)
	if len(events) == 0 {
		delete(e.objectEvents, objectNsName)
	}
}

// List returns all events for the given object that were created after the given time, sorted by creation time.
func (e *eventWatcher) List(objectNsName types.NamespacedName, createdAfter time.Time) []*eventsv1.Event {
	events, ok := e.objectEvents[objectNsName]
	if !ok {
		return []*eventsv1.Event{}
	}
	result := make([]*eventsv1.Event, 0, len(events))
	for _, event := range events {
		if event.CreationTimestamp.Time.After(createdAfter) {
			result = append(result, event)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].CreationTimestamp.Time.Before(result[j].CreationTimestamp.Time)
	})
	return result
}

func NewEventWatcher(ctx context.Context, gvk schema.GroupVersionKind, kubeClientset kubernetes.Interface) (EventWatcher, error) {
	objectSelector := func(opts *metav1.ListOptions) {
		opts.FieldSelector = fields.OneTermEqualSelector("regarding.kind", gvk.Kind).String()
	}
	eventInformer := informers.NewSharedInformerFactoryWithOptions(
		kubeClientset, 0, informers.WithTweakListOptions(objectSelector)).Events().V1().Events()
	watcher := &eventWatcher{
		informer:     eventInformer,
		objectEvents: make(map[types.NamespacedName]eventSet),
	}
	eventInformer.Informer().AddEventHandler(watcher)

	go eventInformer.Informer().Run(ctx.Done())
	logger.Debugf(ctx, "Started informer for [%s] events", gvk.Kind)

	return watcher, nil
}
