module github.com/flyteorg/flytepropeller

go 1.18

require (
	github.com/DiSiqueira/GoTree v1.0.1-0.20180907134536-53a8e837f295
	github.com/benlaurie/objecthash v0.0.0-20180202135721-d1e3d6079fc1
	github.com/fatih/color v1.13.0
	github.com/flyteorg/flyteidl v1.1.10
	github.com/flyteorg/flyteplugins v1.0.10
	github.com/flyteorg/flytestdlib v1.0.5
	github.com/ghodss/yaml v1.0.0
	github.com/go-redis/redis v6.15.7+incompatible
	github.com/go-test/deep v1.0.7
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/imdario/mergo v0.3.13
	github.com/magiconair/properties v1.8.6
	github.com/mitchellh/mapstructure v1.4.3
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.4.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.2
	golang.org/x/exp v0.0.0-20220722155223-a9213eeb770e
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20220210224613-90d013bbcef8
	google.golang.org/grpc v1.46.0
	google.golang.org/protobuf v1.28.0
	k8s.io/api v0.24.1
	k8s.io/apiextensions-apiserver v0.24.1
	k8s.io/apimachinery v0.24.1
	k8s.io/client-go v0.24.1
	k8s.io/klog v1.0.0
	sigs.k8s.io/controller-runtime v0.12.1
)

require (
	cloud.google.com/go v0.101.0 // indirect
	cloud.google.com/go/compute v1.6.1 // indirect
	github.com/aws/aws-sdk-go v1.44.2 // indirect
	google.golang.org/api v0.76.0 // indirect
	google.golang.org/genproto v0.0.0-20220426171045-31bebdecfb46 // indirect
)

replace github.com/aws/amazon-sagemaker-operator-for-k8s => github.com/aws/amazon-sagemaker-operator-for-k8s v1.0.1-0.20210303003444-0fb33b1fd49d
