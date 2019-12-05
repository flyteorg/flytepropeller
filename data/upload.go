package data

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
)

const maxPrimitiveSize = 1024

type Unmarshal func(r io.Reader, msg proto.Message) error
type Uploader struct {
	format    Format
	unmarshal Unmarshal
	// TODO support multiple buckets
	store                   *storage.DataStore
	aggregateOutputFileName string
}

type dirFile struct {
	path string
	info os.FileInfo
	ref  storage.DataReference
}

func MakeLiteralForSimpleType(_ context.Context, t core.SimpleType, s string) (*core.Literal, error) {
	scalar := &core.Scalar{}
	switch t {
	case core.SimpleType_STRUCT:
		st := &structpb.Struct{}
		err := jsonpb.UnmarshalString(s, st)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load generic type as json.")
		}
		scalar.Value = &core.Scalar_Generic{
			Generic: st,
		}
	case core.SimpleType_INTEGER, core.SimpleType_BOOLEAN, core.SimpleType_FLOAT, core.SimpleType_STRING, core.SimpleType_DATETIME, core.SimpleType_DURATION:
		p := &core.Primitive{}
		switch t {
		case core.SimpleType_INTEGER:
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse integer value")
			}
			p.Value = &core.Primitive_Integer{Integer: v}
		case core.SimpleType_FLOAT:
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse Float value")
			}
			p.Value = &core.Primitive_FloatValue{FloatValue: v}
		case core.SimpleType_BOOLEAN:
			v, err := strconv.ParseBool(s)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse Bool value")
			}
			p.Value = &core.Primitive_Boolean{Boolean: v}
		case core.SimpleType_STRING:
			p.Value = &core.Primitive_StringValue{StringValue: s}
		case core.SimpleType_DURATION:
			v, err := time.ParseDuration(s)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse Duration, valid formats: e.g. 300ms, -1.5h, 2h45m")
			}
			p.Value = &core.Primitive_Duration{Duration: ptypes.DurationProto(v)}
		case core.SimpleType_DATETIME:
			v, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse Datetime in RFC3339 format")
			}
			ts, err := ptypes.TimestampProto(v)
			if err != nil {
				return nil, errors.Wrap(err, "failed to convert datetime to proto")
			}
			p.Value = &core.Primitive_Datetime{Datetime: ts}
		}
		scalar.Value = &core.Scalar_Primitive{Primitive: p}
	}
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: scalar,
		},
	}, nil
}

func MakeLiteralForBlob(_ context.Context, path storage.DataReference, isDir bool, format string) *core.Literal {
	dim := core.BlobType_SINGLE
	if isDir {
		dim = core.BlobType_MULTIPART
	}
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: &core.Scalar{
				Value: &core.Scalar_Blob{
					Blob: &core.Blob{
						Uri: path.String(),
						Metadata: &core.BlobMetadata{
							Type: &core.BlobType{
								Dimensionality: dim,
								Format:         format,
							},
						},
					},
				},
			},
		},
	}
}

func IsFileReadable(filePath string) (os.FileInfo, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Wrapf(err, "file not found at path [%s]", filePath)
		}
		if os.IsPermission(err) {
			return nil, errors.Wrapf(err, "unable to read file [%s], Flyte does not have permissions", filePath)
		}
		return nil, errors.Wrapf(err, "failed to read file")
	}
	return info, nil
}

func (u Uploader) handleSimpleType(ctx context.Context, t core.SimpleType, filePath string) (*core.Literal, error) {
	if info, err := IsFileReadable(filePath); err != nil {
		return nil, err
	} else {
		if info.IsDir() {
			return nil, fmt.Errorf("expected file for type [%s], found dir at path [%s]", t.String(), filePath)
		}
		if info.Size() > maxPrimitiveSize {
			return nil, fmt.Errorf("maximum allowed filesize is [%d], but found [%d]", maxPrimitiveSize, info.Size())
		}
	}
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return MakeLiteralForSimpleType(ctx, t, string(b))
}

func UploadFile(ctx context.Context, filePath string, toPath storage.DataReference, size int64, store *storage.DataStore) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logger.Errorf(ctx, "failed to close blob file at path [%s]", filePath)
		}
	}()
	return store.WriteRaw(ctx, toPath, size, storage.Options{}, f)
}

func (u Uploader) handleBlobType(ctx context.Context, localPath string, toPath storage.DataReference) (*core.Literal, error) {
	if info, err := IsFileReadable(localPath); err != nil {
		return nil, err
	} else {
		if info.IsDir() {
			var files []dirFile
			err := filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					logger.Errorf(ctx, "encountered error when uploading multipart blob, %s", err)
					return err
				}
				if info.IsDir() {
					logger.Warnf(ctx, "Currently nested directories are not supported in multipart blob uploads, for directory @ %s", path)
				} else {
					ref, err := u.store.ConstructReference(ctx, toPath, info.Name())
					if err != nil {
						return err
					}
					files = append(files, dirFile{
						path: path,
						info: info,
						ref:  ref,
					})
				}
				return nil
			})
			if err != nil {
				return nil, err
			}

			childCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			fileUploader := make([]Future, 0, len(files))
			for _, f := range files {
				pth := f.path
				ref := f.ref
				size := f.info.Size()
				fileUploader = append(fileUploader, NewAsyncFuture(childCtx, func(i2 context.Context) (i interface{}, e error) {
					return nil, UploadFile(i2, pth, ref, size, u.store)
				}))
			}

			for _, f := range fileUploader {
				// TODO maybe we should have timeouts, or we can have a global timeout at the top level
				_, err := f.Get(ctx)
				if err != nil {
					return nil, err
				}
			}

			return MakeLiteralForBlob(ctx, toPath, false, ""), nil
		} else {
			size := info.Size()
			// Should we make this a go routine as well, so that we can introduce timeouts
			return MakeLiteralForBlob(ctx, toPath, false, ""), UploadFile(ctx, localPath, toPath, size, u.store)
		}
	}
}

func (u Uploader) RecursiveUpload(ctx context.Context, vars *core.VariableMap, fromPath string, toPathPrefix storage.DataReference) error {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	varFutures := make(map[string]Future, len(vars.Variables))
	for varName, variable := range vars.Variables {
		varPath := path.Join(fromPath, varName)
		varType := variable.GetType()
		switch varType.GetType().(type) {
		case *core.LiteralType_Blob:
			var varOutputPath storage.DataReference
			var err error
			if varName == u.aggregateOutputFileName {
				varOutputPath, err = u.store.ConstructReference(ctx, toPathPrefix, "_"+varName)
			} else {
				varOutputPath, err = u.store.ConstructReference(ctx, toPathPrefix, varName)
			}
			if err != nil {
				return err
			}
			varFutures[varName] = NewAsyncFuture(childCtx, func(ctx2 context.Context) (interface{}, error) {
				return u.handleBlobType(ctx2, varPath, varOutputPath)
			})
		case *core.LiteralType_Simple:
			varFutures[varName] = NewAsyncFuture(childCtx, func(ctx2 context.Context) (interface{}, error) {
				return u.handleSimpleType(ctx2, varType.GetSimple(), varPath)
			})
		}
	}

	outputs := &core.LiteralMap{
		Literals: make(map[string]*core.Literal, len(varFutures)),
	}
	for k, f := range varFutures {
		logger.Infof(ctx, "Waiting for [%s] to complete (it may have a background upload too)", k)
		v, err := f.Get(ctx)
		if err != nil {
			logger.Errorf(ctx, "Failed to upload [%s], reason [%s]", k, err)
			return err
		}
		l, ok := v.(*core.Literal)
		if !ok {
			return fmt.Errorf("IllegalState, expected core.Literal, received [%s]", reflect.TypeOf(v))
		}
		outputs.Literals[k] = l
		logger.Infof(ctx, "Var [%s] completed", k)
	}

	toOutputPath, err := u.store.ConstructReference(ctx, toPathPrefix, u.aggregateOutputFileName)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "Uploading final outputs to [%s]", toOutputPath)
	if err := u.store.WriteProtobuf(ctx, toOutputPath, storage.Options{}, outputs); err != nil {
		logger.Errorf(ctx, "Failed to upload final outputs file to [%s], err [%s]", toOutputPath, err)
		return err
	}
	logger.Infof(ctx, "Uploaded final outputs to [%s]", toOutputPath)
	return nil
}

func NewUploader(_ context.Context, store *storage.DataStore, format Format) Uploader {
	return Uploader{
		format:                  format,
		store:                   store,
		aggregateOutputFileName: "outputs.pb",
	}
}
