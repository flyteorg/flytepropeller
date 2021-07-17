export REPOSITORY=flytepropeller
include ../boilerplate/lyft/docker_build/Makefile
include ../boilerplate/lyft/golang_test_targets/Makefile
include ../boilerplate/lyft/end2end/Makefile

.PHONY: update_boilerplate
update_boilerplate:
	@boilerplate/update.sh

.PHONY: linux_compile
linux_compile:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /artifacts/k8s-scheduler ./cmd/k8sschedulerextender/main.go

.PHONY: compile
compile:
	mkdir -p ./bin
	go build -o bin/k8s-scheduler ./cmd/k8sschedulerextender/main.go

cross_compile:
	@glide install
	@mkdir -p ./bin/cross
	GOOS=linux GOARCH=amd64 go build -o bin/cross/k8s-scheduler ./cmd/controller/main.go

op_code_generate:
	@RESOURCE_NAME=resourceRequest OPERATOR_PKG=github.com/lyft/flytepropeller/k8s-scheduler/pkg GENERATORS=deepcopy,client ./hack/update-codegen.sh

# server starts the service in development mode
.PHONY: server
server:
	@go run ./cmd/controller/main.go --alsologtostderr --propeller.kube-config=$(HOME)/.kube/config

.PHONY: generate
generate: download_tooling
	@go generate ./...
