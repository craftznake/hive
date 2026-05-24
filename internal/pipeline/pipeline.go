package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Pipeline struct {
	pre       []Stage
	iteration []Stage

	deps Dependencies

	// inline registry for pending feedback
	pendingFeedback sync.Map
}

func NewPipeline(deps Dependencies) *Pipeline {
	p := &Pipeline{
		deps: deps,
	}
	p.deps.parent = p

	stageDeps := &p.deps
	p.pre = []Stage{NewContextStage(stageDeps)}
	p.iteration = []Stage{NewExecuteStage(stageDeps)}
	return p
}

// Execute executes the full pipeline for a single agent run.
func (p *Pipeline) Execute(ctx context.Context, state *State) (*Result, error) {
	start := time.Now()
	for _, stage := range p.pre {
		_, err := stage.Execute(ctx, state)
		if err != nil {
			return nil, fmt.Errorf("pre: %w", err)
		}
	}

	// Propagate enriched context from pre stages
	if state.Ctx != nil {
		ctx = state.Ctx
	}

	// 2. Iteration loop.
	// StageContinue: Continue the current stage iteration, (in iteration context, this mean, continue the iteration)
	// StageNext: go to next stage
	// StageAbort: Stop entire run
	for state.Iteration = 0; state.Iteration < p.deps.Config.AI.MaxStep; state.Iteration++ {
	loop:
		for _, stage := range p.iteration {
			result, err := stage.Execute(ctx, state)
			if err != nil {
				return nil, fmt.Errorf("iter %d %s: %w", state.Iteration, stage.Name(), err)
			}

			switch result {
			case StageAbort:
				state.ExitCode = StageAbort
				break loop
			case StageNext:
				state.ExitCode = StageNext
				break loop
			case StageContinue:
				continue
			}
		}

		// Check if we need to exit the execution stages.
		if state.ExitCode == StageAbort || state.ExitCode == StageNext {
			break
		}

		// If the context is cancelled, then we cancel the pipeline also
		if ctx.Err() != nil {
			state.ExitCode = StageAbort
			break
		}
	}

	result := state.Result()
	result.Duration = time.Since(start)
	return result, nil
}

func (p *Pipeline) Handle(ctx context.Context, cmd PipelineCommand) error {
	switch cmd.Key {
	case PipelineSubmitInputKey:
		payload, ok := cmd.Payload.(PipelineSubmitInputPayload)
		if !ok {
			return errors.New("pipeline: payload must be PipelineSubmitInputPayload for PipelineSubmitInputCommand")
		}
		return p.handleSubmitInput(ctx, payload)
	default:
		return fmt.Errorf("pipeline: unknown command: %s", cmd)
	}
}

// handleSubmitInput handle the input submit from the user, send it to the
// waiting channel.
// The waiting channel is created with correlation ID as the key before, by
// the excute stage, after the task is paused by the queen.
func (p *Pipeline) handleSubmitInput(ctx context.Context, payload PipelineSubmitInputPayload) error {
	chAny, ok := p.pendingFeedback.Load(payload.CorrelationID)
	if !ok {
		return fmt.Errorf("pipeline: no waiting channel found for correlation ID: %s", payload.CorrelationID)
	}

	ch := chAny.(chan<- PipelineSubmitInputPayload) //nolint: errcheck// this is always true
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- payload:
		return nil
	default:
		return fmt.Errorf("pipeline: waiting channel is full for correlation ID: %s", payload.CorrelationID)
	}
}

// waitForFeedback waits for the feedback response for a given correlation ID.
// It returns the feedback response or an error if the context is done or the waiting channel is full.
func (p *Pipeline) waitForFeedback(ctx context.Context, correlationID string) (PipelineSubmitInputPayload, error) {
	ch := make(chan PipelineSubmitInputPayload, 1)
	p.pendingFeedback.Store(correlationID, ch)
	defer p.pendingFeedback.Delete(correlationID)

	select {
	case <-ctx.Done():
		return PipelineSubmitInputPayload{}, ctx.Err()
	case payload := <-ch:
		return payload, nil
	}
}
