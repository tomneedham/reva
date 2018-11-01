package project

import (
	"context"
)

type (
	Project struct {
		Name         string
		Path         string
		Owner        string
		AdminGroup   string
		ReadersGroup string
		WritersGroup string
	}

	ProjectManager interface {
		GetAllProjects(ctx context.Context) ([]*Project, error)
		GetProject(ctx context.Context, name string) (*Project, error)
	}
)
