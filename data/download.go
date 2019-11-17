package data

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"reflect"
	"strconv"

	"github.com/golang/protobuf/ptypes"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type Format = string

const (
	FormatJSON Format = "json"
	FormatYAML Format = "yaml"
)

var AllOutputFormats = []Format{
	FormatJSON,
	FormatYAML,
}

type VarMap map[string]interface{}
type FutureMap map[string]Future

type Downloader struct {
	format  Format
	marshal func(v interface{}) ([]byte, error)
	// TODO support multiple buckets
	store *storage.DataStore
}

func (d Downloader) downloadFromStorage(ctx context.Context, ref storage.DataReference) (io.ReadCloser, error) {
	// We should probably directly use stow!??
	m, err := d.store.Head(ctx, ref)
	if err != nil {
		return nil, errors.Wrapf(err, "failed when looking up Blob")
	}
	if m.Exists() {
		r, err := d.store.ReadRaw(ctx, ref)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read Blob from storage")
		}
		return r, err

	}
	return nil, fmt.Errorf("incorrect blob reference, does not exist")
}

func (d Downloader) downloadFromHttp(ctx context.Context, ref storage.DataReference) (io.ReadCloser, error) {
	resp, err := http.Get(ref.String())
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (d Downloader) handleBlob(ctx context.Context, blob *core.Blob, toFilePath string) (interface{}, error) {
	ref := storage.DataReference(blob.Uri)
	scheme, _, _, err := ref.Split()
	if err != nil {
		return nil, errors.Wrapf(err, "Blob uri incorrectly formatted")
	}
	var reader io.ReadCloser
	if scheme == "http" || scheme == "https" {
		reader, err = d.downloadFromHttp(ctx, ref)
	} else {
		reader, err = d.downloadFromStorage(ctx, ref)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to download from given url [%s]", ref)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			logger.Errorf(ctx, "failed to close Blob read stream. Error: %s", err)
		}
	}()

	writer, err := os.Create(toFilePath)
	// handle err
	defer func() {
		err := writer.Close()
		if err != nil {
			logger.Errorf(ctx, "failed to close File write stream. Error: %s", err)
		}
	}()
	v, err := io.Copy(writer, reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to write remote data to local filesystem")
	}
	logger.Infof(ctx, "Successfully copied [%d] bytes remote data from [%s] to local [%s]", v, ref, toFilePath)
	return ref.String(), nil
}

func (d Downloader) handlePrimitive(primitive *core.Primitive, toFilePath string) (interface{}, error) {

	switch primitive.Value.(type) {
	case *core.Primitive_StringValue:
		return primitive.GetStringValue(), ioutil.WriteFile(toFilePath, []byte(primitive.GetStringValue()), os.ModePerm)
	case *core.Primitive_Boolean:
		return primitive.GetBoolean(), ioutil.WriteFile(toFilePath, []byte(strconv.FormatBool(primitive.GetBoolean())), os.ModePerm)
	case *core.Primitive_Integer:
		return primitive.GetInteger(), ioutil.WriteFile(toFilePath, []byte(strconv.FormatInt(primitive.GetInteger(), 10)), os.ModePerm)
	case *core.Primitive_FloatValue:
		return primitive.GetFloatValue(), ioutil.WriteFile(toFilePath, []byte(strconv.FormatFloat(primitive.GetFloatValue(), 'f', -1, 64)), os.ModePerm)
	case *core.Primitive_Datetime:
		return primitive.GetDatetime(), ioutil.WriteFile(toFilePath, []byte(ptypes.TimestampString(primitive.GetDatetime())), os.ModePerm)
	case *core.Primitive_Duration:
		d, err := ptypes.Duration(primitive.GetDuration())
		if err != nil {
			return nil, err
		}
		return primitive.GetDuration(), ioutil.WriteFile(toFilePath, []byte(d.String()), os.ModePerm)
	}
	return nil, ioutil.WriteFile(toFilePath, []byte("null"), os.ModePerm)
}

func (d Downloader) handleScalar(ctx context.Context, scalar *core.Scalar, toFilePath string) Future {
	switch scalar.GetValue().(type) {
	case *core.Scalar_Primitive:
		p := scalar.GetPrimitive()
		pth := toFilePath
		return NewAsyncFuture(ctx, func(ctx2 context.Context) (interface{}, error) {
			return d.handlePrimitive(p, pth)
		})
	case *core.Scalar_Blob:
		b := scalar.GetBlob()
		p := toFilePath
		return NewAsyncFuture(ctx, func(ctx2 context.Context) (interface{}, error) {
			return d.handleBlob(ctx2, b, p)
		})
	}
	return NewSyncFuture(nil, fmt.Errorf("unsupported scalar type [%v]", reflect.TypeOf(scalar.GetValue())))
}

func (d Downloader) RecursiveDownload(ctx context.Context, inputs *core.LiteralMap, dir string) (VarMap, error) {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	f := make(FutureMap, len(inputs.Literals))
	for variable, literal := range inputs.Literals {
		switch literal.GetValue().(type) {
		case *core.Literal_Scalar:
			f[variable] = d.handleScalar(childCtx, literal.GetScalar(), path.Join(dir, variable))
		default:
			return nil, fmt.Errorf("received unsupported literal type [%s]", reflect.TypeOf(literal.GetValue()))
		}
	}

	vmap := make(VarMap, len(f))
	for variable, future := range f {
		v, err := future.Get(childCtx)
		if err != nil && err != AsyncFutureCanceledErr {
			return nil, errors.Wrapf(err, "failed to download and store data for variable [%s]", variable)
		}
		vmap[variable] = v
	}

	return vmap, nil
}

func (d Downloader) DownloadInputs(ctx context.Context, inputRef storage.DataReference, outputDir string) error {
	inputs := &core.LiteralMap{}
	err := d.store.ReadProtobuf(ctx, inputRef, inputs)
	if err != nil {
		return errors.Wrapf(err, "failed to download input metadata message from remote store")
	}
	varMap, err := d.RecursiveDownload(ctx, inputs, outputDir)
	if err != nil {
		return errors.Wrapf(err, "failed to download input variable from remote store")
	}

	m, err := d.marshal(varMap)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal out inputs")
	}

	aggregatePath := path.Join(outputDir, "inputs")
	return ioutil.WriteFile(aggregatePath, m, os.ModePerm)
}

func NewDownloader(ctx context.Context, store *storage.DataStore, format Format) Downloader {
	m := json.Marshal
	if format == FormatYAML {
		m = yaml.Marshal
	}
	return Downloader{
		format:  format,
		marshal: m,
		store:   store,
	}
}
