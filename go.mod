module github.com/lyft/flytepropeller

go 1.13

require (
	cloud.google.com/go v0.48.0 // indirect
	github.com/Azure/azure-sdk-for-go v10.2.1-beta+incompatible // indirect
	github.com/Azure/go-autorest v13.3.0+incompatible // indirect
	github.com/DiSiqueira/GoTree v1.0.1-0.20180907134536-53a8e837f295
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-00010101000000-000000000000 // indirect
	github.com/Masterminds/semver v1.5.0
	github.com/aws/aws-sdk-go v1.25.33 // indirect
	github.com/benlaurie/objecthash v0.0.0-20180202135721-d1e3d6079fc1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cheekybits/is v0.0.0-20150225183255-68e9c0620927 // indirect
	github.com/coocood/freecache v1.1.0 // indirect
	github.com/dnaeon/go-vcr v1.0.1 // indirect
	github.com/fatih/color v1.7.0
	github.com/ghodss/yaml v1.0.0
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/go-test/deep v1.0.4 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20191027212112-611e8accdfc9 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/uuid v1.1.1
	github.com/graymeta/stow v0.0.0-20190522170649-903027f87de7 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/grpc-gateway v1.12.1 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/influxdata/influxdb v1.7.9 // indirect
	github.com/json-iterator/go v1.1.8 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/lyft/datacatalog v0.1.1
	github.com/lyft/flyteidl v0.14.1
	github.com/lyft/flyteplugins v0.2.2
	github.com/lyft/flytestdlib v0.2.28
	github.com/magiconair/properties v1.8.1
	github.com/mattn/go-colorable v0.1.4 // indirect
	github.com/mattn/go-isatty v0.0.10 // indirect
	github.com/mitchellh/mapstructure v1.1.2
	github.com/ncw/swift v1.0.49-0.20191112130638-27a552ee74bc // indirect
	github.com/pelletier/go-toml v1.6.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/common v0.7.0 // indirect
	github.com/prometheus/procfs v0.0.6 // indirect
	github.com/satori/uuid v1.2.0 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.4.0
	go.opencensus.io v0.22.2 // indirect
	golang.org/x/crypto v0.0.0-20191112222119-e1110fd1c708 // indirect
	golang.org/x/net v0.0.0-20191112182307-2180aed22343 // indirect
	golang.org/x/sys v0.0.0-20191112214154-59a1497f0cea // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools v0.0.0-20191112232237-76a3b8da50ef // indirect
	golang.org/x/xerrors v0.0.0-20191011141410-1b5146add898 // indirect
	gomodules.xyz/jsonpatch v2.0.1+incompatible // indirect
	google.golang.org/api v0.13.1-0.20191113000739-8a53bfa9f89b // indirect
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/grpc v1.25.1
	gopkg.in/yaml.v2 v2.2.5 // indirect
	k8s.io/api v0.0.0-20191016110408-35e52d86657a
	k8s.io/apimachinery v0.0.0-20191004115801-a2eda9f80ab8
	k8s.io/client-go v0.0.0-20191016111102-bec269661e48
	k8s.io/code-generator v0.0.0-20191004115455-8e001e5d1894 // indirect
	k8s.io/gengo v0.0.0-20191108084044-e500ee069b5c // indirect
	k8s.io/klog v1.0.0
	k8s.io/kube-openapi v0.0.0-20191107075043-30be4d16710a // indirect
	k8s.io/utils v0.0.0-20191030222137-2b95a09bc58d // indirect
	sigs.k8s.io/controller-runtime v0.3.1-0.20191029211253-40070e2a1958
)

replace (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator => github.com/lyft/spark-on-k8s-operator v0.1.3
	k8s.io/api => github.com/lyft/api v0.0.0-20191031200350-b49a72c274e0
	k8s.io/apimachinery => github.com/lyft/apimachinery v0.0.0-20191031200210-047e3ea32d7f
)
