package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flytepropeller/cmd/data/cmd/containerwatcher"
	"github.com/lyft/flytepropeller/data"
)

func TestUploadOptions_Upload_SuccessFile(t *testing.T) {
	tmpFolderLocation := ""
	tmpPrefix := "upload_test"
	outputPath := "output"

	ctx := context.TODO()
	uopts := UploadOptions{
		remoteOutputsPrefix: outputPath,
		outputFormat:        data.FormatJSON,
		watcherType:         containerwatcher.WatcherTypeSuccessFile,
	}

	t.Run("uploadNoOutputs", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir(tmpFolderLocation, tmpPrefix)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, os.RemoveAll(tmpDir))
		}()
		uopts.localDirectoryPath = tmpDir

		uopts.outputInterface = nil
		s := promutils.NewTestScope()
		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, s.NewSubScope("storage"))
		assert.NoError(t, err)
		uopts.RootOptions = &RootOptions{
			Scope: s,
			Store: store,
		}

		assert.NoError(t, uopts.Upload(ctx))
	})

	uopts.outputInterface = nil
	vmap := &core.VariableMap{
		Variables: map[string]*core.Variable{
			"x_test": {
				Type:        &core.LiteralType{Type: &core.LiteralType_Blob{Blob: &core.BlobType{Dimensionality: core.BlobType_SINGLE}}},
				Description: "example",
			},
		},
	}
	d, err := proto.Marshal(vmap)
	assert.NoError(t, err)
	fmt.Println("========")
	fmt.Println(base64.StdEncoding.EncodeToString(d))
}
