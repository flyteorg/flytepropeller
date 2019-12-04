package cmd

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flytepropeller/cmd/data/cmd/containercompletion"
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
		watcherType:         containercompletion.WatcherTypeSuccessFile,
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
}
