package handler

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"testing"

	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/k8s"

	"github.com/stretchr/testify/assert"
)

func TestDecodeTaskState(t *testing.T) {
	str := `I/+DAwEBC1BsdWdpblN0YXRlAf+EAAEBAQVQaGFzZQEGAAAABf+EAQIA`
	reader := base64.NewDecoder(base64.RawStdEncoding, bytes.NewReader([]byte(str)))
	dec := gob.NewDecoder(reader)
	st := &k8s.PluginState{}
	err := dec.Decode(st)
	if assert.NoError(t, err) {
		t.Logf("Deserialized State: [%+v]", st)
	}
}
