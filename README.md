Flyte Propeller
===============
[![Current Release](https://img.shields.io/github/release/lyft/flytepropeller.svg)](https://github.com/lyft/flytepropeller/releases/latest)
[![Build Status](https://travis-ci.org/lyft/flytepropeller.svg?branch=master)](https://travis-ci.org/lyft/flytepropeller)
[![GoDoc](https://godoc.org/github.com/lyft/flytepropeller?status.svg)](https://pkg.go.dev/mod/github.com/lyft/flytepropeller)
[![License](https://img.shields.io/badge/LICENSE-Apache2.0-ff69b4.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![CodeCoverage](https://img.shields.io/codecov/c/github/lyft/flytepropeller.svg)](https://codecov.io/gh/lyft/flytepropeller)
[![Go Report Card](https://goreportcard.com/badge/github.com/lyft/flytepropeller)](https://goreportcard.com/report/github.com/lyft/flytepropeller)
![Commit activity](https://img.shields.io/github/commit-activity/w/lyft/flytepropeller.svg?style=plastic)
![Commit since last release](https://img.shields.io/github/commits-since/lyft/flytepropeller/latest.svg?style=plastic)

Kubernetes operator to executes Flyte graphs natively on kubernetes

Getting Started
===============
kubectl-flyte tool
------------------
kubectl-flyte is an command line tool that can be used as an extension to kubectl. It is a separate binary that is built from the propeller repo.

Install
-------
This command will install kubectl-flyte and flytepropeller to `~/go/bin`
```
   $ make compile
```

Use
---
Two ways to execute the command, either standalone *kubectl-flyte* or as a subcommand of *kubectl*

```
    $ kubectl-flyte --help
    OR
    $ kubectl flyte --help
    Flyte is a serverless workflow processing platform built for native execution on K8s.
          It is extensible and flexible to allow adding new operators and comes with many operators built in

    Usage:
      kubectl-flyte [flags]
      kubectl-flyte [command]

    Available Commands:
      compile     Compile a workflow from core proto-buffer files and output a closure.
      config      Runs various config commands, look at the help of this command to get a list of available commands..
      create      Creates a new workflow from proto-buffer files.
      delete      delete a workflow
      get         Gets a single workflow or lists all workflows currently in execution
      help        Help about any command
      visualize   Get GraphViz dot-formatted output.
```


Observing running workflows
---------------------------

To retrieve all workflows in a namespace use the --namespace option, --namespace = "" implies all namespaces.

```
   $ kubectl-flyte get --namespace flytekit-development
    workflows
    ├── flytekit-development/flytekit-development-f01c74085110840b8827 [ExecId: ... ] (2m34s Succeeded) - Time SinceCreation(30h1m39.683602s)
    ...
    Found 19 workflows
    Success: 19, Failed: 0, Running: 0, Waiting: 0
```

To retrieve a specific workflow, namespace can either be provided in the format namespace/name or using the --namespace argument

```
   $ kubectl-flyte get flytekit-development/flytekit-development-ff806e973581f4508bf1
    Workflow
    └── flytekit-development/flytekit-development-ff806e973581f4508bf1 [ExecId: project:"flytekit" domain:"development" name:"ff806e973581f4508bf1" ] (2m32s Succeeded )
        ├── start-node start 0s Succeeded
        ├── c task 0s Succeeded
        ├── b task 0s Succeeded
        ├── a task 0s Succeeded
        └── end-node end 0s Succeeded
```

Deleting workflows
------------------
To delete a specific workflow

```
   $ kubectl-flyte delete --namespace flytekit-development flytekit-development-ff806e973581f4508bf1
```

To delete all completed workflows - they have to be either success/failed with a special isCompleted label set on them. The Label is set `here <https://github.com/lyft/flytepropeller/blob/master/pkg/controller/controller.go#L247>`

```
   $ kubectl-flyte delete --namespace flytekit-development --all-completed
```

build-tool 
-------------
build-tool is another command line tool binary that is built from the propeller repo. Currently it only supports 
one command `crd-validation`, which is used to generate the validation spec for Flyteworkflow CRD.

Install
-------
 build-tool can also be installed to `~/go/bin` via the following command
 ```
    $ make compile
 ```

Use
___
```
    $ ./bin/build-tool --help
    Flyte is a serverless workflow processing platform built for native execution on K8s.
          It is extensible and flexible to allow adding new operators and comes with many operators built in
    
    Usage:
      build-tool [flags]
      build-tool [command]
    
    Available Commands:
      crd-validation Augment a CRD YAML file with validation section based on a base CRD file
      help           Help about any command    
```

Augment a Base CRD with Validation Spec
--------------------------------------- 
We can augment a base CRD yaml with the validation spec by using the `crd-validation` command in build-tool.

```
    $ ./bin/build-tool crd-validation --help
    Augment a CRD YAML file with validation section based on a base CRD file
    
    Usage:
      build-tool crd-validation [flags]
    
    Aliases:
      crd-validation, validate
    
    Flags:
      -b, --base-crd string      Path to base CRD file.
      -c, --config-file string   Path of the config file for the execution of CRD validation
      -d, --dry-run              Compiles and transforms, but does not create a workflow. OutputsRef ts to STDOUT.
      -h, --help                 help for crd-validation
```

The `--config-file` specifies the configuration file that contains the setting like the output location of the augmented CRD 
relative to pwd. The `--base-crd` specifies the YAML file containing the base CRD definition to be augmented. 

Example:
```
    $ ./bin/build-tool crd-validation -c crd_validation_config.yaml -b base_wf_crd.yaml
    2020/06/11 15:48:00 Using config file: crd_validation_config.yaml
    2020/06/11 15:48:00 Using config file: crd_validation_config.yaml
    2020/06/11 15:48:00 Reading base CRD from base_wf_crd.yaml
    2020/06/11 15:48:00 Generating validation
```



Running propeller locally
-------------------------
use the config.yaml in root found `here <https://github.com/lyft/flytepropeller/blob/master/config.yaml>`. Cd into this folder and then run

```
   $ flytepropeller --logtostderr
```

Following dependencies need to be met
1. Blob store (you can forward minio port to localhost)
2. Admin Service endpoint (can be forwarded) OR *Disable* events to admin and launchplans
3. access to kubeconfig and kubeapi


Making changes to CRD
=====================
*Remember* changes to CRD should be carefully done, they should be backwards compatible or else you should use proper
operator versioning system. Once you do the changes, remember to execute

```
    $make op_code_generate
```
You also have to update the OpenAPI definition by running the following make target
```
    $make openapi_generate
```

Once that is done, run the `build-tool crd-validation` command to generate the new augmented YAML file again. **Double check the YAML file to see if your changes are reflected**
