package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestEventWatcher_OnAdd(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	ew := eventWatcher{ctx: ctx, objectEvents: map[types.NamespacedName]eventSet{
		{Namespace: "ns1", Name: "name1"}: {
			{Namespace: "eventns1", Name: "eventname1"}: &eventsv1.Event{EventTime: metav1.NewMicroTime(now.Add(-time.Minute))},
		},
		{Namespace: "ns2", Name: "name2"}: {},
	}}

	t.Run("existing event", func(t *testing.T) {
		ew.OnAdd(&eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns1",
				Name:      "eventname1",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns1",
				Name:      "name1",
			},
			EventTime: metav1.NewMicroTime(now),
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns1", Name: "name1"}], 1)
		assert.Equal(t, now, ew.objectEvents[types.NamespacedName{Namespace: "ns1", Name: "name1"}][types.NamespacedName{Namespace: "eventns1", Name: "eventname1"}].EventTime.Time)
	})

	t.Run("new event", func(t *testing.T) {
		ew.OnAdd(&eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns2",
				Name:      "eventname2",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns2",
				Name:      "name2",
			},
			EventTime: metav1.NewMicroTime(now),
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns2", Name: "name2"}], 1)
		assert.Equal(t, now, ew.objectEvents[types.NamespacedName{Namespace: "ns2", Name: "name2"}][types.NamespacedName{Namespace: "eventns2", Name: "eventname2"}].EventTime.Time)
	})

	t.Run("new object", func(t *testing.T) {
		ew.OnAdd(&eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns3",
				Name:      "eventname3",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns3",
				Name:      "name3",
			},
			EventTime: metav1.NewMicroTime(now),
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns3", Name: "name3"}], 1)
		assert.Equal(t, now, ew.objectEvents[types.NamespacedName{Namespace: "ns3", Name: "name3"}][types.NamespacedName{Namespace: "eventns3", Name: "eventname3"}].EventTime.Time)
	})
}

func TestEventWatcher_OnUpdate(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	ew := eventWatcher{ctx: ctx, objectEvents: map[types.NamespacedName]eventSet{
		{Namespace: "ns1", Name: "name1"}: {
			{Namespace: "eventns1", Name: "eventname1"}: &eventsv1.Event{EventTime: metav1.NewMicroTime(now.Add(-time.Minute))},
		},
		{Namespace: "ns2", Name: "name2"}: {},
	}}

	t.Run("existing event", func(t *testing.T) {
		ew.OnUpdate(&eventsv1.Event{}, &eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns1",
				Name:      "eventname1",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns1",
				Name:      "name1",
			},
			EventTime: metav1.NewMicroTime(now),
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns1", Name: "name1"}], 1)
		assert.Equal(t, now, ew.objectEvents[types.NamespacedName{Namespace: "ns1", Name: "name1"}][types.NamespacedName{Namespace: "eventns1", Name: "eventname1"}].EventTime.Time)
	})

	t.Run("new event", func(t *testing.T) {
		ew.OnUpdate(&eventsv1.Event{}, &eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns2",
				Name:      "eventname2",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns2",
				Name:      "name2",
			},
			EventTime: metav1.NewMicroTime(now),
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns2", Name: "name2"}], 1)
		assert.Equal(t, now, ew.objectEvents[types.NamespacedName{Namespace: "ns2", Name: "name2"}][types.NamespacedName{Namespace: "eventns2", Name: "eventname2"}].EventTime.Time)
	})

	t.Run("new object", func(t *testing.T) {
		ew.OnUpdate(&eventsv1.Event{}, &eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns3",
				Name:      "eventname3",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns3",
				Name:      "name3",
			},
			EventTime: metav1.NewMicroTime(now),
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns3", Name: "name3"}], 1)
		assert.Equal(t, now, ew.objectEvents[types.NamespacedName{Namespace: "ns3", Name: "name3"}][types.NamespacedName{Namespace: "eventns3", Name: "eventname3"}].EventTime.Time)
	})
}

func TestEventWatcher_OnDelete(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	ew := eventWatcher{ctx: ctx, objectEvents: map[types.NamespacedName]eventSet{
		{Namespace: "ns1", Name: "name1"}: {
			{Namespace: "eventns1", Name: "eventname1"}: &eventsv1.Event{},
		},
		{Namespace: "ns2", Name: "name2"}: {},
	}}

	t.Run("existing event", func(t *testing.T) {
		ew.OnDelete(&eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns1",
				Name:      "eventname1",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns1",
				Name:      "name1",
			},
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns1", Name: "name1"}], 0)
	})

	t.Run("missing event", func(t *testing.T) {
		ew.OnDelete(&eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns2",
				Name:      "eventname2",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns2",
				Name:      "name2",
			},
			EventTime: metav1.NewMicroTime(now),
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns2", Name: "name2"}], 0)
	})

	t.Run("missing object", func(t *testing.T) {
		ew.OnDelete(&eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "eventns3",
				Name:      "eventname3",
			},
			Regarding: corev1.ObjectReference{
				Namespace: "ns3",
				Name:      "name3",
			},
			EventTime: metav1.NewMicroTime(now),
		})

		assert.Len(t, ew.objectEvents[types.NamespacedName{Namespace: "ns3", Name: "name3"}], 0)
	})
}

func TestEventWatcher_List(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	ew := eventWatcher{ctx: ctx, objectEvents: map[types.NamespacedName]eventSet{
		{Namespace: "ns1", Name: "name1"}: {
			{Namespace: "eventns1", Name: "eventname1"}: &eventsv1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now)}},
			{Namespace: "eventns2", Name: "eventname2"}: &eventsv1.Event{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.Add(-time.Hour))}},
		},
		{Namespace: "ns2", Name: "name2"}: {},
	}}

	t.Run("all events", func(t *testing.T) {
		result := ew.List(types.NamespacedName{Namespace: "ns1", Name: "name1"}, time.Time{})

		assert.Len(t, result, 2)
	})

	t.Run("recent events", func(t *testing.T) {
		result := ew.List(types.NamespacedName{Namespace: "ns1", Name: "name1"}, now.Add(-time.Minute))

		assert.Len(t, result, 1)
	})

	t.Run("no events", func(t *testing.T) {
		result := ew.List(types.NamespacedName{Namespace: "ns2", Name: "name2"}, time.Time{})

		assert.Len(t, result, 0)
	})

	t.Run("no object", func(t *testing.T) {
		result := ew.List(types.NamespacedName{Namespace: "ns3", Name: "name3"}, time.Time{})

		assert.Len(t, result, 0)
	})

	t.Run("sorted", func(t *testing.T) {
		result := ew.List(types.NamespacedName{Namespace: "ns1", Name: "name1"}, time.Time{})

		assert.Equal(t, metav1.NewTime(now.Add(-time.Hour)), result[0].CreationTimestamp)
		assert.Equal(t, metav1.NewTime(now), result[1].CreationTimestamp)
	})
}
