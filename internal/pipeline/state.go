package pipeline

import (
	"context"
	"time"

	"github.com/hnimtadd/hive/pkg/types"
)

// State is the shared mutabled state for the whole pipeline execution.
type State struct {
	Conversation *types.Conversation

	// Ctx holds enriched context after ContextStage. This context already have
	// task identity information injected, so inner agent could read from this.
	Ctx context.Context

	// Global state
	Iteration int
	ExitCode  StageResult
	RunID     string
}

// NewPipelineState creates a PipelineState with identity fields available.
func NewPipelineState(ctx context.Context, session *types.Conversation) *State {
	return &State{
		Conversation: session,
		Ctx:          ctx,
		RunID:        session.ID,
	}
}

type Result struct {
	RunID    string
	Output   string
	Duration time.Duration
}

// Result return the pipeline result that the pipeline state executed.
// TODO: fullfil this
func (p State) Result() *Result {
	// Result output should be the last machine replied in the pipeline conversation.
	output := ""
	for i := len(p.Conversation.Messages) - 1; i >= 0; i-- {
		if p.Conversation.Messages[i].Role == types.RoleAssistant {
			output = p.Conversation.Messages[i].Content
		}
	}
	return &Result{
		RunID:  p.RunID,
		Output: output,
	}
}
