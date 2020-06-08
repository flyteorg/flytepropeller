module github.com/lyft/flytepropeller

go 1.13

require (
	cloud.google.com/go v0.58.0 // indirect
	github.com/Azure/azure-sdk-for-go v43.0.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.10.2 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.8.3 // indirect
	github.com/DiSiqueira/GoTree v1.0.1-0.20180907134536-53a8e837f295
	github.com/aws/aws-sdk-go v1.31.13 // indirect
	github.com/benlaurie/objecthash v0.0.0-20180202135721-d1e3d6079fc1
	github.com/coreos/go-oidc v2.2.1+incompatible // indirect
	github.com/fatih/color v1.9.0
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/go-redis/redis v6.15.8+incompatible
	github.com/go-test/deep v1.0.6 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.2
	github.com/google/uuid v1.1.1
	github.com/googleapis/gnostic v0.4.2 // indirect
	github.com/graymeta/stow v0.2.5 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.14.6 // indirect
	github.com/imdario/mergo v0.3.9 // indirect
	github.com/lyft/datacatalog v0.2.1
	github.com/lyft/flyteidl v0.17.33
	github.com/lyft/flyteplugins v0.3.30
	github.com/lyft/flytestdlib v0.3.7
	github.com/magiconair/properties v1.8.1
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mitchellh/mapstructure v1.3.2
	github.com/ncw/swift v1.0.52 // indirect
	github.com/pelletier/go-toml v1.8.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.6.0
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.0 // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.0 // indirect
	github.com/stretchr/testify v1.6.1
	golang.org/x/crypto v0.0.0-20200604202706-70a84ac30bf9 // indirect
	golang.org/x/net v0.0.0-20200602114024-627f9648deb9 // indirect
	golang.org/x/sys v0.0.0-20200602225109-6fdc65e7d980 // indirect
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1
	gomodules.xyz/jsonpatch/v2 v2.1.0 // indirect
	google.golang.org/grpc v1.29.1
	gopkg.in/ini.v1 v1.57.0 // indirect
	gopkg.in/square/go-jose.v2 v2.5.1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200605160147-a5ece683394c // indirect
	k8s.io/api v0.18.3
	k8s.io/apimachinery v0.18.3
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/klog v1.0.0
	k8s.io/klog/v2 v2.1.0 // indirect
	k8s.io/kube-openapi v0.0.0-20200427153329-656914f816f9 // indirect
	k8s.io/utils v0.0.0-20200603063816-c1c6865ac451 // indirect
	sigs.k8s.io/controller-runtime v0.6.0
	sigs.k8s.io/structured-merge-diff v1.0.1-0.20191108220359-b1b620dd3f06 // indirect
)

// Pin the version of client-go to something that's compatible with katrogan's fork of api and apimachinery
// Type the following
//   replace k8s.io/client-go => k8s.io/client-go kubernetes-1.16.2
// and it will be replaced with the 'sha' variant of the version

replace (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator => github.com/lyft/spark-on-k8s-operator v0.1.3
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.3.1
	gopkg.in/fsnotify.v1 => github.com/fsnotify/fsnotify v1.4.7
	k8s.io/api => github.com/lyft/api v0.0.0-20191031200350-b49a72c274e0
	k8s.io/apimachinery => github.com/lyft/apimachinery v0.0.0-20191031200210-047e3ea32d7f
	k8s.io/client-go => k8s.io/client-go v0.0.0-20191016111102-bec269661e48
)
