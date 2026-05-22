package top

import (
	"context"
	"fmt"

	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/hnimtadd/hive/internal/transport/client"
	"github.com/hnimtadd/hive/internal/tui"
	"github.com/hnimtadd/hive/internal/tui/chat"
	"github.com/hnimtadd/hive/internal/tui/content"
	"github.com/hnimtadd/hive/internal/tui/footer"
	"github.com/hnimtadd/hive/internal/tui/header"
	"github.com/hnimtadd/hive/internal/tui/help"
	"github.com/hnimtadd/hive/internal/tui/keys"
	"github.com/hnimtadd/hive/pkg/config"
	agentv1 "github.com/hnimtadd/hive/proto/agent/v1"
)

type model struct {
	cfg *config.Config

	mode   tui.Mode
	status tui.Status

	header        *header.Model
	content       *content.Model
	footer        *footer.Model
	help          *help.Model
	height, width int

	client *client.Client
	msgCh  chan tea.Msg // Channel for streaming messages
	ctx    context.Context
	cancel context.CancelFunc

	conversationID        string
	responseCh            <-chan *agentv1.HiveSessionResponse
	streamListenerStarted bool
	requestToTask         map[string]string
	startedTasks          map[string]struct{}
}

type sessionUpdateMsg struct {
	update *agentv1.HiveSessionResponse
}

func newModel(cfg *config.Config) (*model, error) {
	header, err := header.NewModel(header.ModelOptions{
		Status: tui.StatusConnecting,
	})
	if err != nil {
		return nil, err
	}
	footer, err := footer.NewModel(footer.ModelOptions{
		Mode: tui.DefaultMode,
	})
	if err != nil {
		return nil, err
	}
	client, err := client.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create grpc client: %w", err)
	}

	chat, err := chat.NewModel(chat.ModelOptions{
		Client: client,
	})
	if err != nil {
		return nil, err
	}
	mainContent, err := content.NewModel(&content.ModelOptions{
		Chat:   chat,
		Client: client,
	})
	if err != nil {
		return nil, err
	}
	helpModel := help.NewModel()

	ctx, cancel := context.WithCancel(context.Background())
	return &model{
		cfg:           cfg,
		mode:          tui.DefaultMode,
		header:        header,
		footer:        footer,
		content:       mainContent,
		help:          helpModel,
		client:        client,
		msgCh:         make(chan tea.Msg, 100),
		ctx:           ctx,
		cancel:        cancel,
		requestToTask: map[string]string{},
		startedTasks:  map[string]struct{}{},
	}, nil
}

// cleanup closes gRPC connections and cancels context.
func (m *model) cleanup() {
	if m.cancel != nil {
		m.cancel()
	}
}

// Init implements [tea.Model].
func (m *model) Init() tea.Cmd {
	return tea.Batch(
		m.waitForChannelMessage(),
		m.footer.Init(),
		m.content.Init(),
		m.header.Init(),
	)
}

// waitForChannelMessage creates a command that waits for messages from msgCh.
func (m *model) waitForChannelMessage() tea.Cmd {
	return func() tea.Msg {
		select {
		case msg, ok := <-m.msgCh:
			if !ok {
				return nil
			}
			return msg
		case <-m.ctx.Done():
			return nil
		}
	}
}

// Update implements [tea.Model].
func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	cmd := []tea.Cmd{}
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		cmd = append(cmd, m.handleResizeMsg(msg))

	case tui.ErrorMsg, tui.InfoMsg:
		cmd = append(cmd, m.footer.Update(msg))

	case tui.ChangeModeMsg:
		cmd = append(cmd, m.handleChangeModeMsg(msg))

	case tui.ChangeStatusMsg:
		if m.status != tui.Status(msg) {
			m.status = tui.Status(msg)
			m.header.Update(msg)
		}

	case tui.ToggleHelpMsg:
		m.help.Toggle()

	case chat.SendMessageMsg:
		cmd = append(cmd, m.handleSendMessageMsg(msg.Content))

	case chat.StreamStartMsg, chat.StreamChunkMsg, chat.StreamCompleteMsg, chat.FeedbackRequestMsg:
		// Forward streaming messages to chat model
		cmd = append(cmd, m.content.Update(msg))
		// Chain the next channel read
		cmd = append(cmd, m.waitForChannelMessage())

	case chat.FeedbackResponseMsg:
		// Handle feedback response (send to server)
		if err := m.client.SendFeedback(m.ctx, msg.ConversationID, msg.TurnID, msg.Response); err != nil {
			cmd = append(cmd, tui.MsgCmd(tui.ErrorMsg(err)))
		}

	case sessionUpdateMsg:
		cmd = append(cmd, m.handleSessionUpdate(msg.update)...)
		cmd = append(cmd, m.waitForChannelMessage())

	case tui.OpenConversationMsg:
		cmd = append(cmd, m.handleOpenConversationMsg(msg))

	case tea.KeyMsg:
		// Pressing any key makes any info/error message in the footer disappear.
		m.footer.ResetStatus()
		switch m.mode {
		case tui.ModeInsert:
			switch {
			case key.Matches(msg, keys.HiveKeys.Insert.Leave):
				return m, tui.MsgCmd(tui.ChangeModeMsg(tui.DefaultMode))
			default:
				cmd = append(cmd, m.content.Update(msg))
			}
		case tui.ModeNormal:
			switch {
			case key.Matches(msg, keys.HiveKeys.Normal.Insert) && m.content.IsChatView():
				return m, tui.MsgCmd(tui.ChangeModeMsg(tui.ModeInsert))
			case key.Matches(msg, keys.HiveKeys.Normal.Quit):
				m.cleanup()
				return m, tea.Quit
			case key.Matches(msg, keys.HiveKeys.Normal.Clear) && m.content.IsChatView():
				return m, tui.MsgCmd(tui.ClearChatMsg{})
			case key.Matches(msg, keys.HiveKeys.Normal.Help):
				return m, tui.MsgCmd(tui.ToggleHelpMsg{})
			case key.Matches(msg, keys.HiveKeys.Normal.Sessions):
				m.content.ToggleView()
			}
			cmd = append(cmd, m.content.Update(msg))
		}
	default:
		cmd = append(cmd, m.content.Update(msg))
	}
	return m, tea.Batch(cmd...)
}

// View implements [tea.Model].
func (m *model) View() tea.View {
	var main string
	mainHeight := max(0, m.height-tui.FooterHeight-tui.HeaderHeight)
	if m.help.IsActive() {
		main = tui.DefaultContainer.
			Width(m.width).
			Height(mainHeight).
			Render(m.help.View())
	} else {
		main = tui.DefaultContainer.
			Width(m.width).
			Height(mainHeight).
			Render(m.content.View())
	}
	content := lipgloss.JoinVertical(lipgloss.Top,
		m.header.View().Content,
		main,
		m.footer.View().Content,
	)

	v := tea.NewView(content)
	v.AltScreen = true
	return v
}

func (m *model) registerConversation(convID string, respCh <-chan *agentv1.HiveSessionResponse) {
	m.conversationID = convID
	m.responseCh = respCh
	m.streamListenerStarted = true
	m.requestToTask = map[string]string{}
	m.startedTasks = map[string]struct{}{}
}

func (m *model) resetConversation() {
	m.conversationID = ""
	m.responseCh = nil
	m.streamListenerStarted = false
	m.requestToTask = map[string]string{}
	m.startedTasks = map[string]struct{}{}
	m.startStreamListener()
}
