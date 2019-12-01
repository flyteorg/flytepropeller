package cmd

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flytepropeller/data"
	"github.com/lyft/flytepropeller/pkg/utils"
)

func TestDownloadOptions_Download(t *testing.T) {

	tmpFolderLocation := ""
	tmpPrefix := "download_test"
	inputPath := "input/inputs.pb"
	outputPath := "output"

	ctx := context.TODO()
	dopts := DownloadOptions{
		remoteInputsPath:    inputPath,
		remoteOutputsPrefix: outputPath,
		outputFormat:        data.FormatJSON,
	}

	collectFile := func(d string) []string {
		var files []string
		assert.NoError(t, filepath.Walk(d, func(path string, info os.FileInfo, err error) error {
			if !strings.Contains(info.Name(), tmpPrefix) {
				files = append(files, info.Name())
			} // Skip tmp folder
			return nil
		}))
		sort.Strings(files)
		return files
	}

	t.Run("emptyInputs", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir(tmpFolderLocation, tmpPrefix)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, os.RemoveAll(tmpDir))
		}()
		dopts.localDirectoryPath = tmpDir

		s := promutils.NewTestScope()
		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, s.NewSubScope("storage"))
		assert.NoError(t, err)
		dopts.RootOptions = &RootOptions{
			Scope: s,
			Store: store,
		}

		assert.NoError(t, store.WriteProtobuf(ctx, storage.DataReference(inputPath), storage.Options{}, &core.LiteralMap{}))
		assert.NoError(t, dopts.Download(ctx))

		assert.Equal(t, []string{"inputs"}, collectFile(tmpDir))
	})

	t.Run("primitiveInputs", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir(tmpFolderLocation, tmpPrefix)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, os.RemoveAll(tmpDir))
		}()
		dopts.localDirectoryPath = tmpDir

		s := promutils.NewTestScope()
		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, s.NewSubScope("storage"))
		assert.NoError(t, err)
		dopts.RootOptions = &RootOptions{
			Scope: s,
			Store: store,
		}

		assert.NoError(t, store.WriteProtobuf(ctx, storage.DataReference(inputPath), storage.Options{}, &core.LiteralMap{
			Literals: map[string]*core.Literal{
				"x": utils.MustMakePrimitiveLiteral(1),
				"y": utils.MustMakePrimitiveLiteral("hello"),
			},
		}))
		assert.NoError(t, dopts.Download(ctx), "Download Operation failed")
		assert.Equal(t, []string{"inputs", "x", "y"}, collectFile(tmpDir))
	})

	t.Run("primitiveAndBlobInputs", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir(tmpFolderLocation, tmpPrefix)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, os.RemoveAll(tmpDir))
		}()
		dopts.localDirectoryPath = tmpDir

		s := promutils.NewTestScope()
		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, s.NewSubScope("storage"))
		assert.NoError(t, err)
		dopts.RootOptions = &RootOptions{
			Scope: s,
			Store: store,
		}

		blobLoc := storage.DataReference("blob-loc")
		br := bytes.NewBuffer([]byte("Hello World!"))
		assert.NoError(t, store.WriteRaw(ctx, blobLoc, int64(br.Len()), storage.Options{}, br))
		assert.NoError(t, store.WriteProtobuf(ctx, storage.DataReference(inputPath), storage.Options{}, &core.LiteralMap{
			Literals: map[string]*core.Literal{
				"x": utils.MustMakePrimitiveLiteral(1),
				"y": utils.MustMakePrimitiveLiteral("hello"),
				"blob": {Value: &core.Literal_Scalar{
					Scalar: &core.Scalar{
						Value: &core.Scalar_Blob{
							Blob: &core.Blob{
								Uri: blobLoc.String(),
								Metadata: &core.BlobMetadata{
									Type: &core.BlobType{
										Dimensionality: core.BlobType_SINGLE,
										Format:         ".xyz",
									},
								},
							},
						},
					},
				}},
			},
		}))
		assert.NoError(t, dopts.Download(ctx), "Download Operation failed")
		assert.ElementsMatch(t, []string{"inputs", "x", "y", "blob"}, collectFile(tmpDir))
	})

	t.Run("primitiveAndBlobInputs", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir(tmpFolderLocation, tmpPrefix)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, os.RemoveAll(tmpDir))
		}()
		dopts.localDirectoryPath = tmpDir

		s := promutils.NewTestScope()
		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, s.NewSubScope("storage"))
		assert.NoError(t, err)
		dopts.RootOptions = &RootOptions{
			Scope: s,
			Store: store,
		}

		blobLoc := storage.DataReference("blob-loc")
		br := bytes.NewBuffer([]byte("Hello World!"))
		assert.NoError(t, store.WriteRaw(ctx, blobLoc, int64(br.Len()), storage.Options{}, br))
		assert.NoError(t, store.WriteProtobuf(ctx, storage.DataReference(inputPath), storage.Options{}, &core.LiteralMap{
			Literals: map[string]*core.Literal{
				"x": utils.MustMakePrimitiveLiteral(1),
				"y": utils.MustMakePrimitiveLiteral("hello"),
				"blob": {Value: &core.Literal_Scalar{
					Scalar: &core.Scalar{
						Value: &core.Scalar_Blob{
							Blob: &core.Blob{
								Uri: blobLoc.String(),
								Metadata: &core.BlobMetadata{
									Type: &core.BlobType{
										Dimensionality: core.BlobType_SINGLE,
										Format:         ".xyz",
									},
								},
							},
						},
					},
				}},
			},
		}))
		assert.NoError(t, dopts.Download(ctx), "Download Operation failed")
		assert.ElementsMatch(t, []string{"blob", "inputs", "x", "y"}, collectFile(tmpDir))
	})

	t.Run("primitiveAndMissingBlobInputs", func(t *testing.T) {
		tmpDir, err := ioutil.TempDir(tmpFolderLocation, tmpPrefix)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, os.RemoveAll(tmpDir))
		}()
		dopts.localDirectoryPath = tmpDir

		s := promutils.NewTestScope()
		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, s.NewSubScope("storage"))
		assert.NoError(t, err)
		dopts.RootOptions = &RootOptions{
			Scope: s,
			Store: store,
		}

		assert.NoError(t, store.WriteProtobuf(ctx, storage.DataReference(inputPath), storage.Options{}, &core.LiteralMap{
			Literals: map[string]*core.Literal{
				"x": utils.MustMakePrimitiveLiteral(1),
				"y": utils.MustMakePrimitiveLiteral("hello"),
				"blob": {Value: &core.Literal_Scalar{
					Scalar: &core.Scalar{
						Value: &core.Scalar_Blob{
							Blob: &core.Blob{
								Uri: "blob",
								Metadata: &core.BlobMetadata{
									Type: &core.BlobType{
										Dimensionality: core.BlobType_SINGLE,
										Format:         ".xyz",
									},
								},
							},
						},
					},
				}},
			},
		}))
		assert.NoError(t, dopts.Download(ctx), "Download Operation failed")
		errFile, err := store.ConstructReference(ctx, storage.DataReference(outputPath), "errors.pb")
		assert.NoError(t, err)
		errProto := &core.ErrorDocument{}
		err = store.ReadProtobuf(ctx, errFile, errProto)
		assert.NoError(t, err)
		assert.NotNil(t, errProto.Error)
		assert.Equal(t, core.ContainerError_RECOVERABLE, errProto.Error.Kind)
	})

}
