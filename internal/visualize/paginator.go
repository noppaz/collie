package visualize

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/paginator"

	tea "github.com/charmbracelet/bubbletea"
)

func PaginatorCreator(items []string, perPage int) error {
	p := paginator.New()
	p.Type = paginator.Arabic
	p.PerPage = perPage
	p.SetTotalPages(len(items))

	model := PaginatorModel{
		paginator: p,
		items:     items,
	}

	paginate := tea.NewProgram(model)
	if _, err := paginate.Run(); err != nil {
		return err
	}
	return nil
}

type PaginatorModel struct {
	items     []string
	paginator paginator.Model
}

func (m PaginatorModel) Init() tea.Cmd {
	return nil
}

func (m PaginatorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		}
	}
	m.paginator, cmd = m.paginator.Update(msg)
	return m, cmd
}

func (m PaginatorModel) View() string {
	var b strings.Builder
	start, end := m.paginator.GetSliceBounds(len(m.items))
	for _, item := range m.items[start:end] {
		b.WriteString(fmt.Sprintf("\n%s\n", item))
	}
	b.WriteString("  " + m.paginator.View())
	b.WriteString("\n\n  h/l ←/→ page • q: quit\n")
	return b.String()
}
