package user

import (
	"context"
)

const userKey string = "reva-user"

type User struct {
	Account string
	Groups  []string
}

func ContextGetUser(ctx context.Context) (*User, bool) {
	u, ok := ctx.Value(userKey).(*User)
	return u, ok
}

func ContextSetUser(ctx context.Context, u *User) context.Context {
	return context.WithValue(ctx, userKey, u)
}

type UserManager interface {
	GetUserGroups(ctx context.Context, username string) ([]string, error)
	IsInGroup(ctx context.Context, username, group string) (bool, error)
}
