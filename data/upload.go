package data

import (
	"context"
	"io"
	"path"
	"strconv"
	"time"

	"github.com/golang/protobuf/ptypes"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/envoy/bazel-envoy/external/com_github_gogo_protobuf/jsonpb"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
)

const maxPrimitiveSize = 1024

type Uploader struct {
	format  Format
	marshal func(v interface{}) ([]byte, error)
	// TODO support multiple buckets
	store *storage.DataStore
}

func MakeLiteralForSimpleType(t core.SimpleType, r io.Reader) (*core.Literal, error) {
	scalar := &core.Scalar{}
	switch t {
	case core.SimpleType_STRUCT:
		s := &structpb.Struct{}
		err := jsonpb.Unmarshal(r, s)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load generic type as json.")
		}
		scalar.Value = &core.Scalar_Generic{
			Generic: s,
		}
	case core.SimpleType_INTEGER, core.SimpleType_BOOLEAN, core.SimpleType_FLOAT, core.SimpleType_STRING, core.SimpleType_DATETIME, core.SimpleType_DURATION:
		b := make([]byte, 0, 1024)
		_, err := r.Read(b)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read primitive type")
		}
		s := string(b)
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
			p.Value = &core.Primitive_Datetime{Datetime: v}
		}
		scalar.Value = &core.Scalar_Primitive{Primitive: p}
	}
	return &core.Literal{
		Value: &core.Literal_Scalar{
			Scalar: scalar,
		},
	}, nil
}

func (u Uploader) handleSimpleType(ctx context.Context, t core.SimpleType, filePath string) (*core.Literal, error) {

}

func (u Uploader) handleBlobType(ctx context.Context, filePath string) (*core.Literal, error) {

}

func (u Uploader) RecursiveUpload(ctx context.Context, vars *core.VariableMap, fromPath string, toPath storage.DataReference) error {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	varFutures := make(map[string]Future, len(vars.Variables))
	for varName, variable := range vars.Variables {
		varPath := path.Join(fromPath, varName)
		varType := variable.GetType()
		switch varType.GetType().(type) {
		case *core.LiteralType_Blob:
			varFutures[varName] = NewAsyncFuture(childCtx, func(ctx2 context.Context) (interface{}, error) {
				return u.handleBlobType(ctx2, varPath)
			})
		case *core.LiteralType_Simple:
			varFutures[varName] = NewAsyncFuture(childCtx, func(ctx2 context.Context) (interface{}, error) {
				return u.handleSimpleType(ctx2, varType.GetSimple(), varPath)
			})
		}
	}
}
