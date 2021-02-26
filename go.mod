module github.com/lyft/flytepropeller

go 1.13

require (
	github.com/Azure/go-ansiterm v0.0.0-20170929234023-d6e3b3328b78 // indirect
	github.com/DiSiqueira/GoTree v1.0.1-0.20180907134536-53a8e837f295
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-20200210170106-c8ae9e352035 // indirect
	github.com/Jeffail/gabs/v2 v2.0.0 // indirect
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Selvatico/go-mocket v1.0.7 // indirect
	github.com/adammck/venv v0.0.0-20160819025605-8a9c907a37d3 // indirect
	github.com/antihax/optional v1.0.0 // indirect
	github.com/appscode/jsonpatch v1.0.1 // indirect
	github.com/aws/aws-sdk-go-v2 v0.20.0 // indirect
	github.com/benlaurie/objecthash v0.0.0-20180202135721-d1e3d6079fc1
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/coreos/go-oidc v2.2.1+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/docker/docker v0.7.3-0.20190327010347-be7ac8be2ae0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/fatih/color v1.10.0
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/go-logr/zapr v0.2.0 // indirect
	github.com/go-openapi/validate v0.19.5 // indirect
	github.com/go-redis/redis v6.15.7+incompatible
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.1.2
	github.com/googleapis/gnostic v0.5.4 // indirect
	github.com/gophercloud/gophercloud v0.1.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.14.3 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/imdario/mergo v0.3.10 // indirect
	github.com/influxdata/influxdb v1.7.9 // indirect
	github.com/jinzhu/gorm v1.9.11 // indirect
	github.com/kubeflow/pytorch-operator v0.6.0 // indirect
	github.com/kubeflow/tf-operator v0.5.3 // indirect
	github.com/lib/pq v1.2.0 // indirect
	github.com/lyft/datacatalog v0.1.2
	github.com/lyft/flyteidl v0.18.12
	github.com/lyft/flyteplugins v0.0.0-00010101000000-000000000000
	github.com/lyft/flytestdlib v0.3.9
	github.com/magiconair/properties v1.8.4
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/mitchellh/mapstructure v1.4.1
	github.com/moby/spdystream v0.2.0 // indirect
	github.com/moby/term v0.0.0-20200312100748-672ec06f55cd // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/onsi/ginkgo v1.14.1 // indirect
	github.com/onsi/gomega v1.10.2 // indirect
	github.com/pkg/errors v0.9.1
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/prometheus/client_golang v1.9.0
	github.com/satori/uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	go.etcd.io/bbolt v1.3.5 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200910180754-dd1b699fc489 // indirect
	go.uber.org/goleak v1.1.10 // indirect
	go.uber.org/zap v1.15.0 // indirect
	golang.org/x/time v0.0.0-20201208040808-7e3f01d25324
	gomodules.xyz/jsonpatch/v2 v2.1.0 // indirect
	google.golang.org/grpc v1.35.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/square/go-jose.v2 v2.4.1 // indirect
	gotest.tools v2.2.0+incompatible // indirect
	k8s.io/api v0.20.2
	k8s.io/apimachinery v0.20.2
	k8s.io/client-go v11.0.1-0.20190918222721-c0e3722d5cf0+incompatible
	k8s.io/klog v1.0.0
	k8s.io/klog/v2 v2.5.0 // indirect
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009 // indirect
	sigs.k8s.io/apiserver-network-proxy/konnectivity-client v0.0.14 // indirect
	sigs.k8s.io/controller-runtime v0.8.2
	sigs.k8s.io/testing_frameworks v0.1.1 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

replace (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator => github.com/lyft/spark-on-k8s-operator v0.1.4-0.20201027003055-c76b67e3b6d0
	github.com/lyft/flyteidl => ../flyteidl
	github.com/lyft/flyteplugins => ../flyteplugins
	github.com/lyft/flytestdlib => ../flytestdlib
	gopkg.in/fsnotify.v1 => github.com/fsnotify/fsnotify v1.4.7
	k8s.io/client-go => k8s.io/client-go v0.0.0-20210217172142-7279fc64d847
)
