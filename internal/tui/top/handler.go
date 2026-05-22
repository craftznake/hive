package top

import (
	"errors"

	tea "charm.land/bubbletea/v2"
	"github.com/hnimtadd/hive/internal/tui"
	"github.com/hnimtadd/hive/internal/tui/chat"
	agentv1 "github.com/hnimtadd/hive/proto/agent/v1"
)

func (m *model) handleOpenConversationMsg(msg tui.OpenConversationMsg) tea.Cmd {
	convID, respCh, err := m.client.StartConversation(m.ctx, msg.ConversationID)
	if err != nil {
		return tui.MsgCmd(tui.ErrorMsg(err))
	}
	// if current conversationID is not empty, which mean, we need to emit the
	// signal that we need to clear the old conversation.
	m.resetConversation()
	m.registerConversation(convID, respCh)
	m.content.Update(tui.ClearChatMsg{})
	// at this point, the activeconversation is already available inside model
	// we could subsitue and emit the message to the content model with the
	// new convID.
	msg.New = false
	msg.ConversationID = convID
	return m.content.Update(msg)
}

func (m *model) handleResizeMsg(msg tea.WindowSizeMsg) tea.Cmd { //nolint: unparam// This is conventional
	m.height, m.width = msg.Height, msg.Width
	mainHeight := max(0, msg.Height-tui.FooterHeight-tui.HeaderHeight)
	m.header.Update(tea.WindowSizeMsg{
		Height: tui.HeaderHeight,
		Width:  m.width,
	})
	m.footer.Update(tea.WindowSizeMsg{
		Height: tui.FooterHeight,
		Width:  m.width,
	})

	m.content.Update(tea.WindowSizeMsg{
		Height: mainHeight,
		Width:  m.width,
	})

	m.help.Update(tea.WindowSizeMsg{
		Height: mainHeight,
		Width:  m.width,
	})
	return nil
}

func (m *model) handleChangeModeMsg(msg tui.ChangeModeMsg) tea.Cmd {
	if m.mode != tui.Mode(msg) {
		m.mode = tui.Mode(msg)
		switch m.mode {
		case tui.ModeNormal:
			m.content.Update(tea.BlurMsg{})
		case tui.ModeInsert:
			m.content.Update(tea.FocusMsg{})
		}
		m.footer.Update(msg)
		// Forward the mode change to chat so inputbar can update
		return m.content.Update(msg)
	}
	return nil
}

// handleSendMessageMsg sends a message to the gRPC server and handles the response stream.
func (m *model) handleSendMessageMsg(content string) tea.Cmd {
	turnID, requestID, err := m.client.SendTurn(m.ctx, m.conversationID, content)
	if err != nil {
		return tui.MsgCmd(tui.ErrorMsg(err))
	}
	m.requestToTask[requestID] = turnID
	m.startedTasks[turnID] = struct{}{}

	select {
	case m.msgCh <- chat.StreamStartMsg{TaskID: turnID}:
	case <-m.ctx.Done():
		return tui.MsgCmd(tui.ErrorMsg(m.ctx.Err()))
	}

	return tui.NoopCmd
}

func (m *model) handleSessionUpdate(update *agentv1.HiveSessionResponse) []tea.Cmd {
	cmds := []tea.Cmd{}
	if update == nil {
		return cmds
	}

	if createConv := update.GetCreateConversation(); createConv != nil {
		m.conversationID = createConv.GetConversationId()
		_ = m.content.RegisterConversation(createConv.GetConversationId())
		return cmds
	}

	if notification := update.GetNotification(); notification != nil {
		taskID := m.requestToTask[update.GetInReplyTo()]
		if errMsg := notification.GetError(); errMsg != "" {
			if taskID != "" {
				cmds = append(cmds, tui.MsgCmd(chat.StreamCompleteMsg{
					Success: false,
					Content: "",
					Error:   errors.New(errMsg),
					TaskID:  taskID,
				}))
				m.cleanupTaskTracking(taskID)
			} else {
				cmds = append(cmds, tui.MsgCmd(tui.ErrorMsg(errors.New(errMsg))))
			}
			return cmds
		}

		if info := notification.GetInfo(); info != "" {
			if taskID != "" {
				cmds = append(cmds, tui.MsgCmd(chat.StreamChunkMsg{
					Content: info,
					Status:  "info",
					TaskID:  taskID,
				}))
			} else {
				cmds = append(cmds, tui.MsgCmd(tui.InfoMsg(info)))
			}
			return cmds
		}
	}

	if turn := update.GetTurnResponse(); turn != nil {
		taskID := turn.GetTurnId()
		if taskID == "" {
			taskID = m.requestToTask[turn.GetRequestId()]
		}
		if taskID != "" {
			if _, started := m.startedTasks[taskID]; !started {
				m.startedTasks[taskID] = struct{}{}
				cmds = append(cmds, tui.MsgCmd(chat.StreamStartMsg{TaskID: taskID}))
			}
		}

		if progress := turn.GetUpdate(); progress != nil && taskID != "" {
			cmds = append(cmds, tui.MsgCmd(chat.StreamChunkMsg{
				Content: progress.GetContent(),
				Status:  "in_progress",
				TaskID:  taskID,
			}))
		}

		if completed := turn.GetCompleted(); completed != nil && taskID != "" {
			if success := completed.GetSuccess(); success != nil {
				cmds = append(cmds, tui.MsgCmd(chat.StreamCompleteMsg{
					Success: true,
					Content: success.GetContent(),
					Error:   nil,
					TaskID:  taskID,
				}))
				m.cleanupTaskTracking(taskID)
				return cmds
			}
			if failed := completed.GetFailed(); failed != nil {
				cmds = append(cmds, tui.MsgCmd(chat.StreamCompleteMsg{
					Success: false,
					Content: "",
					Error:   errors.New(failed.GetMessage()),
					TaskID:  taskID,
				}))
				m.cleanupTaskTracking(taskID)
				return cmds
			}
		}
	}

	if inputRequired := update.GetInputRequired(); inputRequired != nil {
		taskID := inputRequired.GetTurnId()
		if taskID != "" {
			if _, started := m.startedTasks[taskID]; !started {
				m.startedTasks[taskID] = struct{}{}
				cmds = append(cmds, tui.MsgCmd(chat.StreamStartMsg{TaskID: taskID}))
			}
		}
		cmds = append(cmds, tui.MsgCmd(chat.FeedbackRequestMsg{
			ConversationID: inputRequired.GetConversationId(),
			TurnID:         inputRequired.GetTurnId(),
			Question:       inputRequired.GetQuestion(),
		}))
	}

	return cmds
}

func (m *model) cleanupTaskTracking(taskID string) {
	delete(m.startedTasks, taskID)
	for reqID, currentTaskID := range m.requestToTask {
		if currentTaskID == taskID {
			delete(m.requestToTask, reqID)
		}
	}
}

func (m *model) startStreamListener() {
	go func() {
		for {
			select {
			case <-m.ctx.Done():
				return

			case update, ok := <-m.responseCh:
				if !ok {
					return
				}
				select {
				case m.msgCh <- sessionUpdateMsg{update: update}:
				case <-m.ctx.Done():
					return
				}
			}
		}
	}()
}
