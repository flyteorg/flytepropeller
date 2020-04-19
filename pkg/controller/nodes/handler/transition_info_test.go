package handler

import (
	"reflect"
	"testing"
	"time"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
)

func TestPhaseInfoQueued(t *testing.T) {
	p := PhaseInfoQueued("queued")
	assert.Equal(t, EPhaseQueued, p.p)
}

func TestEPhase_String(t *testing.T) {
	tests := []struct {
		name string
		p    EPhase
	}{
		{"queued", EPhaseQueued},
		{"undefined", EPhaseUndefined},
		{"success", EPhaseSuccess},
		{"skip", EPhaseSkip},
		{"failed", EPhaseFailed},
		{"retryable-fail", EPhaseRetryableFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.String(); got != tt.name {
				t.Errorf("String() = %v, want %v", got, tt.name)
			}
		})
	}
}

func TestEPhase_IsTerminal(t *testing.T) {
	tests := []struct {
		name string
		p    EPhase
		want bool
	}{
		{"success", EPhaseSuccess, true},
		{"failure", EPhaseFailed, true},
		{"timeout", EPhaseTimedout, true},
		{"skip", EPhaseSkip, true},
		{"any", EPhaseQueued, false},
		{"retryable", EPhaseRetryableFailure, false},
		{"run", EPhaseRunning, false},
		{"nr", EPhaseNotReady, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.IsTerminal(); got != tt.want {
				t.Errorf("IsTerminal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoFailure(t *testing.T) {
	type args struct {
		code   string
		reason string
		info   *ExecutionInfo
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoFailure(tt.args.code, tt.args.reason, tt.args.info); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoFailure() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoFailureErr(t *testing.T) {
	type args struct {
		err  *core.ExecutionError
		info *ExecutionInfo
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoFailureErr(tt.args.err, tt.args.info); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoFailureErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoNotReady(t *testing.T) {
	type args struct {
		reason string
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoNotReady(tt.args.reason); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoNotReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoQueued1(t *testing.T) {
	type args struct {
		reason string
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoQueued(tt.args.reason); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoQueued() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoRetryableFailure(t *testing.T) {
	type args struct {
		code   string
		reason string
		info   *ExecutionInfo
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoRetryableFailure(tt.args.code, tt.args.reason, tt.args.info); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoRetryableFailure() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoRetryableFailureErr(t *testing.T) {
	type args struct {
		err  *core.ExecutionError
		info *ExecutionInfo
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoRetryableFailureErr(tt.args.err, tt.args.info); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoRetryableFailureErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoRunning(t *testing.T) {
	type args struct {
		info *ExecutionInfo
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoRunning(tt.args.info); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoRunning() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoSkip(t *testing.T) {
	type args struct {
		info   *ExecutionInfo
		reason string
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoSkip(tt.args.info, tt.args.reason); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoSkip() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoSuccess(t *testing.T) {
	type args struct {
		info *ExecutionInfo
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoSuccess(tt.args.info); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoSuccess() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfoTimedOut(t *testing.T) {
	type args struct {
		info   *ExecutionInfo
		reason string
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PhaseInfoTimedOut(tt.args.info, tt.args.reason); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PhaseInfoTimedOut() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfo_GetErr(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	tests := []struct {
		name   string
		fields fields
		want   *core.ExecutionError
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
			if got := p.GetErr(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetErr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfo_GetInfo(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	tests := []struct {
		name   string
		fields fields
		want   *ExecutionInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
			if got := p.GetInfo(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfo_GetOccurredAt(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
			if got := p.GetOccurredAt(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetOccurredAt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfo_GetPhase(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	tests := []struct {
		name   string
		fields fields
		want   EPhase
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
			if got := p.GetPhase(); got != tt.want {
				t.Errorf("GetPhase() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfo_GetReason(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
			if got := p.GetReason(); got != tt.want {
				t.Errorf("GetReason() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPhaseInfo_SetErr(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	type args struct {
		err *core.ExecutionError
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
		})
	}
}

func TestPhaseInfo_SetInfo(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	type args struct {
		info *ExecutionInfo
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
		})
	}
}

func TestPhaseInfo_SetOcurredAt(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	type args struct {
		t time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
		})
	}
}

func TestPhaseInfo_SetReason(t *testing.T) {
	type fields struct {
		p          EPhase
		occurredAt time.Time
		err        *core.ExecutionError
		info       *ExecutionInfo
		reason     string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PhaseInfo{
				p:          tt.fields.p,
				occurredAt: tt.fields.occurredAt,
				err:        tt.fields.err,
				info:       tt.fields.info,
				reason:     tt.fields.reason,
			}
			if got := p.SetReason(); got != tt.want {
				t.Errorf("SetReason() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_phaseInfo(t *testing.T) {
	type args struct {
		p      EPhase
		err    *core.ExecutionError
		info   *ExecutionInfo
		reason string
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := phaseInfo(tt.args.p, tt.args.err, tt.args.info, tt.args.reason); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("phaseInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_phaseInfoFailed(t *testing.T) {
	type args struct {
		p    EPhase
		err  *core.ExecutionError
		info *ExecutionInfo
	}
	tests := []struct {
		name string
		args args
		want PhaseInfo
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := phaseInfoFailed(tt.args.p, tt.args.err, tt.args.info); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("phaseInfoFailed() = %v, want %v", got, tt.want)
			}
		})
	}
}