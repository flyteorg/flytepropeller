package backoff

import (
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// SyncResourceList is a thread-safe Map. It's meant to replace v1.ResourceList for concurrency-sensitive
// code.
type SyncResourceList struct {
	sync.Map
}

// Store stores the value in the map overriding existing value or adding a new one of one doesn't exist.
func (s *SyncResourceList) Store(resourceName v1.ResourceName, quantity resource.Quantity) {
	s.Map.Store(resourceName, quantity)
}

// Load loads a resource quantity if one exists.
func (s *SyncResourceList) Load(resourceName v1.ResourceName) (quantity resource.Quantity, found bool) {
	val, found := s.Map.Load(resourceName)
	if !found {
		return
	}

	return val.(resource.Quantity), true
}

// Range iterates over all the entries of the list in a non-sorted non-deterministic order.
func (s *SyncResourceList) Range(visitor func(key v1.ResourceName, value resource.Quantity) bool) {
	s.Map.Range(func(key, value interface{}) bool {
		return visitor(key.(v1.ResourceName), value.(resource.Quantity))
	})
}

// Delete deletes items from the map
func (s *SyncResourceList) Delete(key v1.ResourceName) {
	s.Map.Delete(key)
}

// String returns a formatted string of some snapshot of the map.
func (s *SyncResourceList) String() string {
	sb := strings.Builder{}
	s.Range(func(key v1.ResourceName, value resource.Quantity) bool {
		sb.WriteString(key.String())
		sb.WriteString(":")
		sb.WriteString(value.String())
		sb.WriteString(", ")
		return true
	})

	return sb.String()
}

// NewSyncResourceList creates a thread-safe map to store resource names and resource
// quantities. Equivalent to v1.ResourceList but offering concurrent-safe operations.
func NewSyncResourceList() SyncResourceList {
	return SyncResourceList{
		Map: sync.Map{},
	}
}
