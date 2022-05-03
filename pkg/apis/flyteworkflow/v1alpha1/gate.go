package v1alpha1

import (
	"bytes"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/golang/protobuf/jsonpb"
)

// ConditionalKind refers to the type of Node.
type ConditionalKind string

func (n ConditionalKind) String() string {
	return string(n)
}

const (
	ConditionalKindSignal ConditionalKind = "signal"
	ConditionalKindSleep  ConditionalKind = "sleep"
)

type SignalConditional struct {
	*core.SignalConditional
}

func (in SignalConditional) MarshalJSON() ([]byte, error) {
	if in.SignalConditional == nil {
		return nilJSON, nil
	}

	var buf bytes.Buffer
	if err := marshaler.Marshal(&buf, in.SignalConditional); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (in *SignalConditional) UnmarshalJSON(b []byte) error {
	in.SignalConditional = &core.SignalConditional{}
	return jsonpb.Unmarshal(bytes.NewReader(b), in.SignalConditional)
}

type SleepConditional struct {
	*core.SleepConditional
}

func (in SleepConditional) MarshalJSON() ([]byte, error) {
	if in.SleepConditional == nil {
		return nilJSON, nil
	}

	var buf bytes.Buffer
	if err := marshaler.Marshal(&buf, in.SleepConditional); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (in *SleepConditional) UnmarshalJSON(b []byte) error {
	in.SleepConditional = &core.SleepConditional{}
	return jsonpb.Unmarshal(bytes.NewReader(b), in.SleepConditional)
}

type GateNodeSpec struct {
	Kind   ConditionalKind    `json:"kind"`
	Signal *SignalConditional `json:"signal,omitempty"`
	Sleep  *SleepConditional  `json:"sleep,omitempty"`
}

func (g *GateNodeSpec) GetKind() ConditionalKind {
	return g.Kind
}

func (g *GateNodeSpec) GetSignal() *SignalConditional {
	return g.Signal
}

func (g *GateNodeSpec) GetSleep() *SleepConditional {
	return g.Sleep
}
