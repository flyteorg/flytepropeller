export REPOSITORY=flytepropeller
include boilerplate/lyft/docker_build/Makefile
include boilerplate/lyft/golang_test_targets/Makefile
include boilerplate/lyft/end2end/Makefile

PWD := $(shell pwd)
GIT_HASH := $(shell git log -1 --pretty=format:"%H")

.PHONY: update_boilerplate
update_boilerplate:
	@boilerplate/update.sh

.PHONY: linux_compile
linux_compile:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /artifacts/flytepropeller ./cmd/controller/main.go
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /artifacts/kubectl-flyte ./cmd/kubectl-flyte/main.go

.PHONY: compile
compile:
	mkdir -p ./bin
	go build -o bin/flytepropeller ./cmd/controller/main.go
	go build -o bin/kubectl-flyte ./cmd/kubectl-flyte/main.go && cp bin/kubectl-flyte ${GOPATH}/bin

cross_compile:
	@glide install
	@mkdir -p ./bin/cross
	GOOS=linux GOARCH=amd64 go build -o bin/cross/flytepropeller ./cmd/controller/main.go
	GOOS=linux GOARCH=amd64 go build -o bin/cross/kubectl-flyte ./cmd/kubectl-flyte/main.go

op_code_generate:
	@RESOURCE_NAME=flyteworkflow OPERATOR_PKG=github.com/lyft/flytepropeller ./hack/update-codegen.sh

benchmark:
	mkdir -p ./bin/benchmark
	@go test -run=^$ -bench=. -cpuprofile=cpu.out -memprofile=mem.out ./pkg/controller/nodes/. && mv *.out ./bin/benchmark/ && mv *.test ./bin/benchmark/

# server starts the service in development mode
.PHONY: server
server:
	@go run ./cmd/controller/main.go --alsologtostderr --propeller.kube-config=$(HOME)/.kube/config

clean:
	rm -rf bin

# Generate golden files. Add test packages that generate golden files here.
golden:
	go test ./cmd/kubectl-flyte/cmd -update
	go test ./pkg/compiler/test -update

.PHONY: generate
generate: download_tooling
	@go generate ./...

.PHONY: e2e-setup
e2e-setup:
	curl -Lo $(PWD)/kind https://kind.sigs.k8s.io/dl/v0.8.1/kind-linux-amd64
	chmod a+x $(PWD)/kind
	$(PWD)/kind create cluster
	@docker build -t lyft/flytepropeller/flytepropeller:$(GIT_HASH) .
	$(PWD)/kind load docker-image lyft/flytepropeller/flytepropeller:$(GIT_HASH)



