package validators

import (
	"testing"

	"github.com/flyteorg/flyteidl/clients/go/coreutils"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
)

func TestJoinVariableMapsUniqueKeys(t *testing.T) {
	intType := &core.LiteralType{
		Type: &core.LiteralType_Simple{
			Simple: core.SimpleType_INTEGER,
		},
	}

	strType := &core.LiteralType{
		Type: &core.LiteralType_Simple{
			Simple: core.SimpleType_STRING,
		},
	}

	t.Run("Simple", func(t *testing.T) {
		m1 := map[string]*core.Variable{
			"x": {
				Type: intType,
			},
		}

		m2 := map[string]*core.Variable{
			"y": {
				Type: intType,
			},
		}

		res, err := UnionDistinctVariableMaps(m1, m2)
		assert.NoError(t, err)
		assert.Len(t, res, 2)
	})

	t.Run("No type collision", func(t *testing.T) {
		m1 := map[string]*core.Variable{
			"x": {
				Type: intType,
			},
		}

		m2 := map[string]*core.Variable{
			"x": {
				Type: intType,
			},
		}

		res, err := UnionDistinctVariableMaps(m1, m2)
		assert.NoError(t, err)
		assert.Len(t, res, 1)
	})

	t.Run("Type collision", func(t *testing.T) {
		m1 := map[string]*core.Variable{
			"x": {
				Type: intType,
			},
		}

		m2 := map[string]*core.Variable{
			"x": {
				Type: strType,
			},
		}

		_, err := UnionDistinctVariableMaps(m1, m2)
		assert.Error(t, err)
	})
}
