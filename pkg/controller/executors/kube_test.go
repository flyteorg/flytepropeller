package executors

import (
	"context"
	"github.com/stretchr/testify/assert"
	cache2 "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"testing"

	pluginMachinery "github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	"k8s.io/apimachinery/pkg/runtime"
)

func Test_IterateObjects(t *testing.T) {
	plugins := pluginMachinery.PluginRegistry().GetK8sPlugins()

	// Get all objects to monitor
	objectsToMonitor := make([]runtime.Object, 0, len(plugins))
	var c cache.Cache
	c = cache.New(kube.NewCon)
	for _, p := range plugins {
		objectsToMonitor = append(objectsToMonitor, p.ResourceToWatch)
		//objectsToMonitor = append(objectsToMonitor,
		//	struct {
		//		metav1.TypeMeta `json:",inline"`
		//		metav1.ListMeta `json:"metadata"`
		//		Items           []runtime.Object `json:"items"`
		//	}{
		//		Items: []runtime.Object{p.ResourceToWatch},
		//	},
		//)
		i, err := c.GetInformer(context.TODO(), p.ResourceToWatch)
		assert.NoError(t, err)

		si := i.(cache2.SharedIndexInformer)
		assert.True(t, len(si.GetStore().List()) > 0)
	}
}
