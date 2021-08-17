package nodes

import (
	"testing"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func TestToNodeExecOutputData(t *testing.T) {
	outputData := &core.LiteralMap{
		Literals: map[string]*core.Literal{
			"foo": {
				Value: &core.Literal_Scalar{
					Scalar: &core.Scalar{
						Value: &core.Scalar_Primitive{
							Primitive: &core.Primitive{
								Value: &core.Primitive_Integer{
									Integer: 4,
								},
							},
						},
					},
				},
			},
		},
	}

	info := &handler.OutputInfo{
		OutputData: outputData,
		OutputURI:  "s3://foo/bar/outputs.pb",
	}
	output := ToNodeExecOutputData(info)
	assert.True(t, proto.Equal(outputData, output.OutputData))
}
