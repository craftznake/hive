package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hnimtadd/hive/internal/bee/queen"
	"github.com/hnimtadd/hive/internal/observability"
	"github.com/hnimtadd/hive/pkg/types"
	agentv1 "github.com/hnimtadd/hive/proto/agent/v1"
)

type ExecuteStage struct {
	deps *Dependencies
}

var _ Stage = &ExecuteStage{}

func NewExecuteStage(deps *Dependencies) *ExecuteStage {
	return &ExecuteStage{deps: deps}
}

// Execute implements [Stage].
func (e *ExecuteStage) Execute(ctx context.Context, state *State) (StageResult, error) {
	logger := observability.Logger(state.Ctx)
	supervisor, err := queen.NewQueenBee(state.Conversation.ID, 10, e.deps.Registry, e.deps.Config.Server.MaxTimeout, e.deps.Provider)
	if err != nil {
		return StageAbort, fmt.Errorf("failed to init queen bee: %w", err)
	}

	publisher := e.deps.EventBus.Publish(state.Conversation.ID)
	// Execute supervisor loop
	for {
		select {
		case <-state.Ctx.Done():
			return StageAbort, state.Ctx.Err()

		case <-ctx.Done():
			logger.InfoContext(ctx, "context cancelled during execution")
			return StageAbort, fmt.Errorf("context cancelled during execution: %w", ctx.Err())

		default:
			// Execute supervisor iteration
			var output *queen.QueenOutput
			output, err = supervisor.Execute(ctx, state.Conversation)
			if err != nil {
				logger.ErrorContext(ctx, "supervisor execution failed", slog.Any("error", err))
				return StageAbort, fmt.Errorf("failed to execute supervisor: %w", err)
			}
			state.Conversation.Messages = append(state.Conversation.Messages, types.NewMessage(types.RoleAssistant, output.Content))
			if err = e.deps.Storage.Save(state.Conversation); err != nil {
				logger.ErrorContext(ctx, "failed to save conversation to storage, keeps continuing", slog.Any("error", err))
			}

			// Send update to client
			switch output.Status {
			case types.SessionStatusCompleted:
				logger.InfoContext(ctx, "task completed")
				publisher <- agentv1.NewSessionEventTurnResponse(
					state.RunID,
					agentv1.NewTurnResponseSuccess(state.Conversation.ID, state.RunID, state.RunID, output.Content),
				)
				return StageNext, nil

			case types.SessionStatusFailed:
				logger.InfoContext(ctx, "task failed")
				publisher <- agentv1.NewSessionEventTurnResponse(
					state.RunID,
					agentv1.NewTurnResponseFailed(state.Conversation.ID, state.RunID, state.RunID, output.Content),
				)
				return StageNext, nil

			case types.SessionStatusPaused:
				// feedback submit will be Submit(shared.TypeFeedbackInput, Feedback{correlation_id, answer})
				publisher <- agentv1.NewSessionEventInputRequired(
					state.RunID,
					agentv1.NewInputRequired(state.Conversation.ID, state.RunID, output.Content),
				)

				var feedback PipelineSubmitInputPayload
				feedback, err = e.deps.parent.waitForFeedback(ctx, state.RunID)
				if err != nil {
					logger.ErrorContext(ctx, "failed to wait for feedback", slog.Any("error", err))
					return StageAbort, fmt.Errorf("failed to wait for feedback: %w", err)
				}
				state.Conversation.Messages = append(state.Conversation.Messages, types.NewMessage(types.RoleUser, feedback.Input))
				// wait for the feedback response
				// update state message with feedback response
				continue

			case types.SessionStatusInProgress:
				publisher <- agentv1.NewSessionEventTurnResponse(
					state.RunID,
					agentv1.NewTurnResponseUpdate(state.Conversation.ID, state.RunID, state.RunID, output.Content),
				)
				continue
			}
		}
	}
}

// Name implements [Stage].
func (e *ExecuteStage) Name() string { return "execute" }
