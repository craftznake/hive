package pipeline

import (
	"context"

	"github.com/hnimtadd/hive/internal/budget"
	"github.com/hnimtadd/hive/internal/middleware"
	"github.com/hnimtadd/hive/internal/middleware/system"
	"github.com/hnimtadd/hive/internal/observability"
)

type ContextStage struct {
	deps *Dependencies
}

var _ Stage = &ContextStage{}

func NewContextStage(deps *Dependencies) *ContextStage {
	return &ContextStage{deps: deps}
}

// Execute implements [Stage].
// ContextStage populates context into the pipeline state.
func (c *ContextStage) Execute(ctx context.Context, state *State) (StageResult, error) {
	ctx = observability.ContextWithTraceContext(ctx, observability.NewRootTraceContext())

	eventBusTopic := c.deps.EventBus.Publish(state.Conversation.ID)
	ctx = middleware.ContextWithMiddleware(ctx, middleware.JointMiddleware(
		system.EventStreamMiddleware(eventBusTopic),
		observability.NewTraceMiddleware(c.deps.SessionLogger),
	))
	ctx = budget.ContextWithBudget(ctx, budget.NewContextBudget(c.deps.Config.AI.Context))

	state.Ctx = ctx
	return StageNext, nil
}

// Name implements [Stage].
func (c *ContextStage) Name() string { return "context" }
