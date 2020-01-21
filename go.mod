module github.com/lyft/flytepropeller

go 1.13

require (
	cloud.google.com/go v0.51.0 // indirect
	github.com/DiSiqueira/GoTree v1.0.1-0.20180907134536-53a8e837f295
	github.com/aws/aws-sdk-go v1.28.5 // indirect
	github.com/benlaurie/objecthash v0.0.0-20180202135721-d1e3d6079fc1
	github.com/fatih/color v1.9.0
	github.com/ghodss/yaml v1.0.0
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/go-test/deep v1.0.5 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/google/go-cmp v0.4.0 // indirect
	github.com/google/uuid v1.1.1
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/grpc-gateway v1.12.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/influxdata/influxdb v1.7.9 // indirect
	github.com/json-iterator/go v1.1.9 // indirect
	github.com/lyft/datacatalog v0.1.1
	github.com/lyft/flyteidl v0.16.6
	github.com/lyft/flyteplugins v0.2.8
	github.com/lyft/flytestdlib v0.2.31
	github.com/magiconair/properties v1.8.1
	github.com/mitchellh/mapstructure v1.1.2
	github.com/ncw/swift v1.0.49-0.20191117165619-017f012e58fa // indirect
	github.com/pkg/errors v0.8.1
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.1.0 // indirect
	github.com/prometheus/common v0.8.0 // indirect
	github.com/prometheus/procfs v0.0.8 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.6.2 // indirect
	github.com/stretchr/testify v1.4.0
	golang.org/x/crypto v0.0.0-20200117160349-530e935923ad // indirect
	golang.org/x/net v0.0.0-20200114155413-6afb5195e5aa // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sys v0.0.0-20200117145432-59e60aa80a0c // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/api v0.15.1-0.20200117000758-b4cd77d6a56c // indirect
	google.golang.org/genproto v0.0.0-20200117163144-32f20d992d24 // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/ini.v1 v1.51.1 // indirect
	gopkg.in/square/go-jose.v2 v2.4.1 // indirect
	gopkg.in/yaml.v2 v2.2.7 // indirect
	k8s.io/api v0.0.0-20191031200350-b49a72c274e0
	k8s.io/apimachinery v0.0.0-20191031200210-047e3ea32d7f
	k8s.io/client-go v0.0.0-20191016111102-bec269661e48
	k8s.io/klog v1.0.0
	k8s.io/kube-openapi v0.0.0-20191107075043-30be4d16710a // indirect
	k8s.io/utils v0.0.0-20200109141947-94aeca20bf09 // indirect
	sigs.k8s.io/controller-runtime v0.3.1-0.20191029211253-40070e2a1958
)

replace (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator => github.com/lyft/spark-on-k8s-operator v0.1.3
	// FIXME: remove replace after flyteplugins migrated to go module
	github.com/lyft/flyteplugins => github.com/honnix/flyteplugins v0.2.3-0.20200121090137-d83cb70f168e
	gopkg.in/fsnotify.v1 => github.com/fsnotify/fsnotify v1.4.7
	k8s.io/api => github.com/lyft/api v0.0.0-20191031200350-b49a72c274e0
	k8s.io/apimachinery => github.com/lyft/apimachinery v0.0.0-20191031200210-047e3ea32d7f
)
