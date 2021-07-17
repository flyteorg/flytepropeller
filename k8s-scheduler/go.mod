module github.com/lyft/flytepropeller/k8s-scheduler

go 1.13

require (
	github.com/lyft/flytestdlib v0.3.3
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v0.0.6
	github.com/spf13/pflag v1.0.5
	k8s.io/api v0.18.3
	k8s.io/apimachinery v0.18.3
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/kube-scheduler v0.18.3
	sigs.k8s.io/controller-runtime v0.6.0

)

// Pin the version of client-go to something that's compatible with lyft's fork of api and apimachinery
// Type the following
//   replace k8s.io/client-go => k8s.io/client-go kubernetes-1.16.2
// and it will be replaced with the 'sha' variant of the version

replace (
	k8s.io/api => github.com/lyft/api v0.0.0-20200605230047-a49d6ebc29da
	k8s.io/apimachinery => github.com/lyft/apimachinery v0.0.0-20200605231607-2d7f408c0fc2
	k8s.io/client-go => k8s.io/client-go v0.18.0
)
