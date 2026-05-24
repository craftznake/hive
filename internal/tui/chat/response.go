package chat

import (
	"fmt"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/hnimtadd/hive/internal/tui"
)

type chatResponseModel struct {
	id      string
	status  string
	content string // Unified field for both streaming and final content
	error   string
	state   state

	width int
}
type state int

const (
	stateThinking state = iota
	stateSucceed
	stateError
)

func newChatResponseModel(id string, width int) *chatResponseModel {
	return &chatResponseModel{
		id:    id,
		width: width,
		state: stateThinking,
	}
}

// Init implements [tui.Model].
func (m *chatResponseModel) Init() tea.Cmd {
	return tui.NoopCmd
}

// Update implements [tui.Model].
func (m *chatResponseModel) Update(msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
	case StreamStartMsg:
		m.state = stateThinking
	case StreamChunkMsg:
		m.status = msg.Status
		if m.content != "" {
			m.content += "\n"
		}
		if msg.Status != "" {
			m.content += fmt.Sprintf("[%s] %s", msg.Status, msg.Content)
		} else {
			m.content += msg.Content
		}
	case StreamCompleteMsg:
		if msg.Success {
			m.state = stateSucceed
			m.content = msg.Content
		} else {
			m.state = stateError
			m.error = msg.Error.Error()
		}
	}
	return tui.NoopCmd
}

// View implements [tui.Model].
func (m *chatResponseModel) View() string {
	// Configure card style based on role
	var (
		headerTitle string
		content     string
	)

	headerStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(tui.Blue).
		Background(tui.InputBg).
		Padding(0, 1)
	contentStyle := lipgloss.NewStyle().
		Width(m.width-2).
		Background(tui.InputBg).
		Foreground(tui.Foreground).
		Padding(0, 1)
	cardBorder := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(tui.Blue).
		Width(m.width)

	switch m.state {
	case stateThinking:
		headerTitle = "thinking..."
		headerStyle = headerStyle.Foreground(tui.Accent)
		cardBorder = cardBorder.BorderForeground(tui.Accent)
		content = m.content
	case stateError:
		headerTitle = "error"
		headerStyle = headerStyle.Foreground(tui.Red)
		cardBorder = cardBorder.BorderForeground(tui.Red)
		content = m.error
	case stateSucceed:
		headerTitle = "Output"
		headerStyle = headerStyle.Foreground(tui.Green)
		cardBorder = cardBorder.BorderForeground(tui.Green)
		content = m.content
	default:
		return ""
	}
	// Build the card
	header := headerStyle.Render(headerTitle)
	body := contentStyle.Render(content)

	// Combine header and body, then wrap in border
	card := lipgloss.JoinVertical(lipgloss.Left, header, body)
	return cardBorder.Render(card)
}
