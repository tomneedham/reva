package auth_manager_impersonate

import (
	"context"

	"github.com/cernbox/reva/pkg/auth"
)

type mgr struct{}

func New() auth.AuthManager {
	return &mgr{}
}

func (m *mgr) Authenticate(ctx context.Context, clientID, clientSecret string) error {
	return nil
}
