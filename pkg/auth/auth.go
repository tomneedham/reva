package auth

import (
	"context"
)

type AuthManager interface {
	Authenticate(ctx context.Context, clientID, clientSecret string) error
}
