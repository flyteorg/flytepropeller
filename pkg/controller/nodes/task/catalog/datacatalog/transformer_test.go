package datacatalog

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/gogo/protobuf/proto"
	datacatalog "github.com/lyft/datacatalog/protos/gen"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/catalog"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flytepropeller/pkg/utils"
)

// add test for raarranged Literal maps for input values

func TestNilParamTask(t *testing.T) {
	key := catalog.Key{
		Identifier: core.Identifier{
			Project: "project",
			Domain:  "domain",
			Name:    "name",
			Version: "1.0.0",
		},
		CacheVersion: "1.0.0",
		TypedInterface: core.TypedInterface{
			Inputs:  nil,
			Outputs: nil,
		},
	}
	datasetID, err := GenerateDatasetIDForTask(context.TODO(), key)
	assert.NoError(t, err)
	assert.NotEmpty(t, datasetID.Version)
	assert.Equal(t, "1.0.0-V-K42BDF-V-K42BDF", datasetID.Version)
}

// Ensure that empty parameters generate the same dataset as nil parameters
func TestEmptyParamTask(t *testing.T) {
	key := catalog.Key{
		Identifier: core.Identifier{
			Project: "project",
			Domain:  "domain",
			Name:    "name",
			Version: "1.0.0",
		},
		CacheVersion: "1.0.0",
		TypedInterface: core.TypedInterface{
			Inputs:  &core.VariableMap{},
			Outputs: &core.VariableMap{},
		},
	}
	datasetID, err := GenerateDatasetIDForTask(context.TODO(), key)
	assert.NoError(t, err)
	assert.NotEmpty(t, datasetID.Version)
	assert.Equal(t, "1.0.0-V-K42BDF-V-K42BDF", datasetID.Version)

	key.TypedInterface.Inputs = nil
	key.TypedInterface.Outputs = nil
	datasetIDDupe, err := GenerateDatasetIDForTask(context.TODO(), key)
	assert.NoError(t, err)
	assert.True(t, proto.Equal(datasetIDDupe, datasetID))
}

// Ensure the key order on the map generates the same dataset
func TestVariableMapOrder(t *testing.T) {
	key := catalog.Key{
		Identifier: core.Identifier{
			Project: "project",
			Domain:  "domain",
			Name:    "name",
			Version: "1.0.0",
		},
		CacheVersion: "1.0.0",
		TypedInterface: core.TypedInterface{
			Inputs: &core.VariableMap{
				Variables: map[string]*core.Variable{
					"1": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
					"2": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
				},
			},
		},
	}
	datasetID, err := GenerateDatasetIDForTask(context.TODO(), key)
	assert.NoError(t, err)
	assert.NotEmpty(t, datasetID.Version)
	assert.Equal(t, "1.0.0-UxVtPm0k-V-K42BDF", datasetID.Version)

	key.TypedInterface.Inputs = &core.VariableMap{
		Variables: map[string]*core.Variable{
			"2": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
			"1": {Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_INTEGER}}},
		},
	}
	datasetIDDupe, err := GenerateDatasetIDForTask(context.TODO(), key)
	assert.NoError(t, err)

	assert.Equal(t, "1.0.0-UxVtPm0k-V-K42BDF", datasetIDDupe.Version)
	assert.True(t, proto.Equal(datasetID, datasetIDDupe))
}

// Ensure the key order on the inputs generates the same tag
func TestInputValueSorted(t *testing.T) {
	literalMap, err := utils.MakeLiteralMap(map[string]interface{}{"1": 1, "2": 2})
	assert.NoError(t, err)

	tag, err := GenerateArtifactTagName(context.TODO(), literalMap)
	assert.NoError(t, err)
	assert.Equal(t, "flyte_cached-GQid5LjHbakcW68DS3P2jp80QLbiF0olFHF2hTh5bg8", tag)

	literalMap, err = utils.MakeLiteralMap(map[string]interface{}{"2": 2, "1": 1})
	assert.NoError(t, err)

	tagDupe, err := GenerateArtifactTagName(context.TODO(), literalMap)
	assert.NoError(t, err)
	assert.Equal(t, tagDupe, tag)
}

// Ensure that empty inputs are hashed the same way
func TestNoInputValues(t *testing.T) {
	tag, err := GenerateArtifactTagName(context.TODO(), nil)
	assert.NoError(t, err)
	assert.Equal(t, "flyte_cached-m4vFNUOHOFEFIiZSyOyid92TkWFFBDha4UOkkBb47XU", tag)

	tagDupe, err := GenerateArtifactTagName(context.TODO(), &core.LiteralMap{Literals: nil})
	assert.NoError(t, err)
	assert.Equal(t, "flyte_cached-m4vFNUOHOFEFIiZSyOyid92TkWFFBDha4UOkkBb47XU", tagDupe)
	assert.Equal(t, tagDupe, tag)
}

func TestGetOrDefault(t *testing.T) {
	type args struct {
		m            map[string]string
		key          string
		defaultValue string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"default", args{m: map[string]string{"x": "val"}, key: "y", defaultValue: "def"}, "def"},
		{"original", args{m: map[string]string{"y": "val"}, key: "y", defaultValue: "def"}, "val"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetOrDefault(tt.args.m, tt.args.key, tt.args.defaultValue); got != tt.want {
				t.Errorf("GetOrDefault() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetArtifactMetadataForSource(t *testing.T) {
	type args struct {
		taskExecutionID *core.TaskExecutionIdentifier
	}

	tID := &core.TaskExecutionIdentifier{
		TaskId: &core.Identifier{
			ResourceType: core.ResourceType_TASK,
			Name:         "x",
			Project:      "project",
			Domain:       "development",
			Version:      "ver",
		},
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "wf",
				Project: "p1",
				Domain:  "d1",
			},
			NodeId: "n",
		},
		RetryAttempt: 1,
	}

	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{"nil TaskExec", args{}, nil},
		{"TaskExec", args{tID}, map[string]string{
			execTaskAttemptKey: strconv.Itoa(int(tID.RetryAttempt)),
			execProjectKey:     tID.NodeExecutionId.ExecutionId.Project,
			execDomainKey:      tID.NodeExecutionId.ExecutionId.Domain,
			execNodeIDKey:      tID.NodeExecutionId.NodeId,
			execNameKey:        tID.NodeExecutionId.ExecutionId.Name,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetArtifactMetadataForSource(tt.args.taskExecutionID); !reflect.DeepEqual(got.KeyMap, tt.want) {
				t.Errorf("GetMetadataForSource() = %v, want %v", got.KeyMap, tt.want)
			}
		})
	}
}

func TestGetSourceFromMetadata(t *testing.T) {
	tID := core.TaskExecutionIdentifier{
		TaskId: &core.Identifier{
			ResourceType: core.ResourceType_TASK,
			Name:         "x",
			Project:      "project",
			Domain:       "development",
			Version:      "ver",
		},
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "wf",
				Project: "p1",
				Domain:  "d1",
			},
			NodeId: "n",
		},
		RetryAttempt: 1,
	}

	currentTaskID := core.Identifier{
		ResourceType: core.ResourceType_TASK,
		Name:         "x",
		Project:      "project",
		Domain:       "development",
		Version:      "ver2",
	}

	type args struct {
		datasetMd  map[string]string
		artifactMd map[string]string
		currentID  core.Identifier
	}
	tests := []struct {
		name string
		args args
		want *core.TaskExecutionIdentifier
	}{
		// EVerything is missing
		{"missing", args{currentID: currentTaskID}, &core.TaskExecutionIdentifier{
			TaskId: &core.Identifier{
				ResourceType: core.ResourceType_TASK,
				Name:         "x",
				Project:      "project",
				Domain:       "development",
				Version:      "unknown",
			},
			NodeExecutionId: &core.NodeExecutionIdentifier{
				ExecutionId: &core.WorkflowExecutionIdentifier{
					Name:    "unknown",
					Project: "project",
					Domain:  "development",
				},
				NodeId: "unknown",
			},
			RetryAttempt: 0,
		}},
		// In legacy only taskVersionKey is available
		{"legacy", args{datasetMd: GetDatasetMetadataForSource(&tID).KeyMap, currentID: currentTaskID}, &core.TaskExecutionIdentifier{
			TaskId: &core.Identifier{
				ResourceType: core.ResourceType_TASK,
				Name:         "x",
				Project:      "project",
				Domain:       "development",
				Version:      tID.TaskId.Version,
			},
			NodeExecutionId: &core.NodeExecutionIdentifier{
				ExecutionId: &core.WorkflowExecutionIdentifier{
					Name:    "unknown",
					Project: "project",
					Domain:  "development",
				},
				NodeId: "unknown",
			},
			RetryAttempt: 0,
		}},
		// Completely available
		{"latest", args{datasetMd: GetDatasetMetadataForSource(&tID).KeyMap, artifactMd: GetArtifactMetadataForSource(&tID).KeyMap, currentID: currentTaskID}, &tID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSourceFromMetadata(&datacatalog.Metadata{KeyMap: tt.args.datasetMd}, &datacatalog.Metadata{KeyMap: tt.args.artifactMd}, tt.args.currentID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSourceFromMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEventCatalogMetadata(t *testing.T) {
	tID := core.TaskExecutionIdentifier{
		TaskId: &core.Identifier{
			ResourceType: core.ResourceType_TASK,
			Name:         "x",
			Project:      "project",
			Domain:       "development",
			Version:      "ver",
		},
		NodeExecutionId: &core.NodeExecutionIdentifier{
			ExecutionId: &core.WorkflowExecutionIdentifier{
				Name:    "wf",
				Project: "p1",
				Domain:  "d1",
			},
			NodeId: "n",
		},
		RetryAttempt: 1,
	}
	datasetID := &datacatalog.DatasetID{Project: "p", Domain: "d", Name: "n", Version: "v"}
	type args struct {
		datasetID *datacatalog.DatasetID
		tag       *datacatalog.Tag
		sourceID  *core.TaskExecutionIdentifier
	}
	tests := []struct {
		name string
		args args
		want *core.CatalogMetadata
	}{
		{"only datasetID", args{datasetID: datasetID}, &core.CatalogMetadata{DatasetId: DatasetIDToIdentifier(datasetID)}},
		{"tag", args{datasetID: datasetID, tag: &datacatalog.Tag{Name: "n", ArtifactId: "a"}}, &core.CatalogMetadata{DatasetId: DatasetIDToIdentifier(datasetID), ArtifactTag: &core.CatalogArtifactTag{Name: "n", ArtifactId: "a"}}},
		{"source", args{datasetID: datasetID, tag: &datacatalog.Tag{Name: "n", ArtifactId: "a"}, sourceID: &tID}, &core.CatalogMetadata{DatasetId: DatasetIDToIdentifier(datasetID), ArtifactTag: &core.CatalogArtifactTag{Name: "n", ArtifactId: "a"}, SourceExecution: &core.CatalogMetadata_SourceTaskExecution{
			SourceTaskExecution: &tID,
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EventCatalogMetadata(tt.args.datasetID, tt.args.tag, tt.args.sourceID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EventCatalogMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDatasetIDToIdentifier(t *testing.T) {
	id := DatasetIDToIdentifier(&datacatalog.DatasetID{Project: "p", Domain: "d", Name: "n", Version: "v"})
	assert.Equal(t, core.ResourceType_DATASET, id.ResourceType)
	assert.Equal(t, "n", id.Name)
	assert.Equal(t, "p", id.Project)
	assert.Equal(t, "d", id.Domain)
	assert.Equal(t, "v", id.Version)
}
