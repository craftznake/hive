package pipeline

import (
	"context"
)

// StageResult tells the pipeline what todo after executing
type StageResult int

const (
	StageContinue StageResult = iota // Continue the current stage iteration, (in iteration context, this mean, continue the iteration)
	StageNext                        // go to next stage
	StageAbort                       // Stop entire run
)

// Stage is a step in our pipeline.
// Stage are stateless, mutable state should lives in the TaskContext.
type Stage interface {
	// Name returns a human-readable identifier.
	Name() string
	// Execute performs the logics, error will abort the pipeline.
	Execute(ctx context.Context, state *State) (StageResult, error)
}
