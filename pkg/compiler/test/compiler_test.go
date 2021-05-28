package test

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ghodss/yaml"

	"github.com/flyteorg/flyteidl/clients/go/coreutils"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytepropeller/pkg/compiler"
	"github.com/flyteorg/flytepropeller/pkg/compiler/common"
	"github.com/flyteorg/flytepropeller/pkg/compiler/errors"
	"github.com/flyteorg/flytepropeller/pkg/compiler/transformers/k8s"
	"github.com/flyteorg/flytepropeller/pkg/visualize"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

var update = flag.Bool("update", false, "Update .golden files")
var reverse = flag.Bool("reverse", false, "Reverse .golden files")

func makeDefaultInputs(iface *core.TypedInterface) *core.LiteralMap {
	if iface == nil || iface.GetInputs() == nil {
		return nil
	}

	res := make(map[string]*core.Literal, len(iface.GetInputs().Variables))
	for inputName, inputVar := range iface.GetInputs().Variables {
		val := coreutils.MustMakeDefaultLiteralForType(inputVar.Type)
		res[inputName] = val
	}

	return &core.LiteralMap{
		Literals: res,
	}
}

func setDefaultFields(task *core.TaskTemplate) {
	if container := task.GetContainer(); container != nil {
		if container.Config == nil {
			container.Config = []*core.KeyValuePair{}
		}

		container.Config = append(container.Config, &core.KeyValuePair{
			Key:   "testKey1",
			Value: "testValue1",
		})
		container.Config = append(container.Config, &core.KeyValuePair{
			Key:   "testKey2",
			Value: "testValue2",
		})
		container.Config = append(container.Config, &core.KeyValuePair{
			Key:   "testKey3",
			Value: "testValue3",
		})
	}
}

func mustCompileTasks(t *testing.T, tasks []*core.TaskTemplate) []*core.CompiledTask {
	compiledTasks := make([]*core.CompiledTask, 0, len(tasks))
	for _, inputTask := range tasks {
		setDefaultFields(inputTask)
		task, err := compiler.CompileTask(inputTask)
		compiledTasks = append(compiledTasks, task)
		assert.NoError(t, err)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
	}

	return compiledTasks
}

func marshalProto(t *testing.T, filename string, p proto.Message) {
	marshaller := &jsonpb.Marshaler{}
	s, err := marshaller.MarshalToString(p)
	assert.NoError(t, err)

	if err != nil {
		return
	}

	originalRaw, err := proto.Marshal(p)
	assert.NoError(t, err)
	assert.NoError(t, ioutil.WriteFile(strings.Replace(filename, filepath.Ext(filename), ".pb", 1), originalRaw, os.ModePerm))

	m := map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &m)
	assert.NoError(t, err)

	b, err := yaml.Marshal(m)
	assert.NoError(t, err)
	assert.NoError(t, ioutil.WriteFile(strings.Replace(filename, filepath.Ext(filename), ".yaml", 1), b, os.ModePerm))
}

func TestDynamic(t *testing.T) {
	errors.SetConfig(errors.Config{IncludeSource: true})
	assert.NoError(t, filepath.Walk("testdata/dynamic", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		t.Run(path, func(t *testing.T) {
			// If you want to debug a single use-case. Uncomment this line.
			//if !strings.HasSuffix(path, "success_1.json") {
			//	t.SkipNow()
			//}

			raw, err := ioutil.ReadFile(path)
			assert.NoError(t, err)
			wf := &core.DynamicJobSpec{}
			err = jsonpb.UnmarshalString(string(raw), wf)
			if !assert.NoError(t, err) {
				t.FailNow()
			}

			t.Log("Compiling Workflow")
			compiledTasks := mustCompileTasks(t, wf.Tasks)
			wfTemplate := &core.WorkflowTemplate{
				Id: &core.Identifier{
					Domain:  "domain",
					Name:    "name",
					Version: "version",
				},
				Interface: &core.TypedInterface{
					Inputs: &core.VariableMap{Variables: map[string]*core.Variable{}},
					Outputs: &core.VariableMap{Variables: map[string]*core.Variable{
						"o0": {
							Type: &core.LiteralType{
								Type: &core.LiteralType_CollectionType{
									CollectionType: &core.LiteralType{
										Type: &core.LiteralType_Simple{
											Simple: core.SimpleType_INTEGER,
										},
									},
								},
							},
						},
					}},
				},
				Nodes:   wf.Nodes,
				Outputs: wf.Outputs,
			}
			compiledWfc, err := compiler.CompileWorkflow(wfTemplate, wf.Subworkflows, compiledTasks,
				[]common.InterfaceProvider{})
			if !assert.NoError(t, err) {
				t.FailNow()
			}

			inputs := map[string]interface{}{}
			for varName, v := range compiledWfc.Primary.Template.Interface.Inputs.Variables {
				inputs[varName] = coreutils.MustMakeDefaultLiteralForType(v.Type)
			}

			flyteWf, err := k8s.BuildFlyteWorkflow(compiledWfc,
				coreutils.MustMakeLiteral(inputs).GetMap(),
				&core.WorkflowExecutionIdentifier{
					Project: "hello",
					Domain:  "domain",
					Name:    "name",
				},
				"namespace")
			if assert.NoError(t, err) {
				raw, err := json.Marshal(flyteWf)
				if assert.NoError(t, err) {
					assert.NotEmpty(t, raw)
				}
			}
		})

		return nil
	}))
}

func TestBranches(t *testing.T) {
	errors.SetConfig(errors.Config{IncludeSource: true})
	assert.NoError(t, filepath.Walk("testdata/branch", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		t.Run(path, func(t *testing.T) {
			// If you want to debug a single use-case. Uncomment this line.
			if !strings.HasSuffix(path, "success_7.json") {
				t.SkipNow()
			}

			raw, err := ioutil.ReadFile(path)
			assert.NoError(t, err)
			wf := &core.WorkflowClosure{}
			err = jsonpb.UnmarshalString(string(raw), wf)
			if !assert.NoError(t, err) {
				t.FailNow()
			}

			t.Log("Compiling Workflow")
			compiledTasks := mustCompileTasks(t, wf.Tasks)
			compiledWfc, err := compiler.CompileWorkflow(wf.Workflow, []*core.WorkflowTemplate{}, compiledTasks,
				[]common.InterfaceProvider{})
			if !assert.NoError(t, err) {
				t.FailNow()
			}

			inputs := map[string]interface{}{}
			for varName, v := range compiledWfc.Primary.Template.Interface.Inputs.Variables {
				inputs[varName] = coreutils.MustMakeDefaultLiteralForType(v.Type)
			}

			flyteWf, err := k8s.BuildFlyteWorkflow(compiledWfc,
				coreutils.MustMakeLiteral(inputs).GetMap(),
				&core.WorkflowExecutionIdentifier{
					Project: "hello",
					Domain:  "domain",
					Name:    "name",
				},
				"namespace")
			if assert.NoError(t, err) {
				raw, err := json.Marshal(flyteWf)
				if assert.NoError(t, err) {
					assert.NotEmpty(t, raw)
				}
			}
		})

		return nil
	}))
}

func TestReverseEngineerFromYaml(t *testing.T) {
	root := "testdata"
	errors.SetConfig(errors.Config{IncludeSource: true})
	assert.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if !strings.HasSuffix(path, ".yaml") {
			return nil
		}

		if strings.HasSuffix(path, "-inputs.yaml") {
			return nil
		}

		ext := ".yaml"

		testName := strings.TrimLeft(path, root)
		testName = strings.Trim(testName, string(os.PathSeparator))
		testName = strings.TrimSuffix(testName, ext)
		testName = strings.Replace(testName, string(os.PathSeparator), "_", -1)

		t.Run(testName, func(t *testing.T) {
			t.Log("Reading from file")
			raw, err := ioutil.ReadFile(path)
			assert.NoError(t, err)

			raw, err = yaml.YAMLToJSON(raw)
			assert.NoError(t, err)

			t.Log("Unmarshalling Workflow Closure")
			wf := &core.WorkflowClosure{}
			err = jsonpb.UnmarshalString(string(raw), wf)
			assert.NoError(t, err)
			assert.NotNil(t, wf)
			if err != nil {
				return
			}

			t.Log("Compiling Workflow")
			compiledWf, err := compiler.CompileWorkflow(wf.Workflow, []*core.WorkflowTemplate{}, mustCompileTasks(t, wf.Tasks), []common.InterfaceProvider{})
			assert.NoError(t, err)
			if err != nil {
				return
			}

			inputs := makeDefaultInputs(compiledWf.Primary.Template.GetInterface())
			if *reverse {
				marshalProto(t, strings.Replace(path, ext, fmt.Sprintf("-inputs%v", ext), -1), inputs)
			}

			t.Log("Building k8s resource")
			_, err = k8s.BuildFlyteWorkflow(compiledWf, inputs, nil, "")
			assert.NoError(t, err)
			if err != nil {
				return
			}

			dotFormat := visualize.ToGraphViz(compiledWf.Primary)
			t.Logf("GraphViz Dot: %v\n", dotFormat)

			if *reverse {
				marshalProto(t, path, wf)
			}
		})

		return nil
	}))
}

func TestCompileAndBuild(t *testing.T) {
	root := "testdata"
	errors.SetConfig(errors.Config{IncludeSource: true})
	assert.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if ext := filepath.Ext(path); ext != ".pb" {
			return nil
		}

		if strings.HasSuffix(path, "-inputs.pb") {
			return nil
		}

		testName := strings.TrimLeft(path, root)
		testName = strings.Trim(testName, string(os.PathSeparator))
		testName = strings.Trim(testName, filepath.Ext(testName))
		testName = strings.Replace(testName, string(os.PathSeparator), "_", -1)

		t.Run(testName, func(t *testing.T) {
			t.Log("Reading from file")
			raw, err := ioutil.ReadFile(path)
			assert.NoError(t, err)

			t.Log("Unmarshalling Workflow Closure")
			wf := &core.WorkflowClosure{}
			err = proto.Unmarshal(raw, wf)
			assert.NoError(t, err)
			assert.NotNil(t, wf)
			if err != nil {
				return
			}

			t.Log("Compiling Workflow")
			compiledWf, err := compiler.CompileWorkflow(wf.Workflow, []*core.WorkflowTemplate{}, mustCompileTasks(t, wf.Tasks), []common.InterfaceProvider{})
			assert.NoError(t, err)
			if err != nil {
				return
			}

			inputs := makeDefaultInputs(compiledWf.Primary.Template.GetInterface())
			if *update {
				marshalProto(t, strings.Replace(path, filepath.Ext(path), fmt.Sprintf("-inputs%v", filepath.Ext(path)), -1), inputs)
			}

			t.Log("Building k8s resource")
			_, err = k8s.BuildFlyteWorkflow(compiledWf, inputs, nil, "")
			assert.NoError(t, err)
			if err != nil {
				return
			}

			dotFormat := visualize.ToGraphViz(compiledWf.Primary)
			t.Logf("GraphViz Dot: %v\n", dotFormat)

			if *update {
				marshalProto(t, path, wf)
			}
		})

		return nil
	}))
}
