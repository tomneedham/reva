package user

import (
	"context"
)

type key int

const userKey key = iota

// User represents a userof the system.
type User struct {
	Account string
	Groups  []string
}

// ContextGetUser returns the user if set in the given context.
func ContextGetUser(ctx context.Context) (*User, bool) {
	u, ok := ctx.Value(userKey).(*User)
	return u, ok
}

// ContextSetUser stores the user in the context.
func ContextSetUser(ctx context.Context, u *User) context.Context {
	return context.WithValue(ctx, userKey, u)
}

// Manager is the interface to implement to manipulate users.
type Manager interface {
	GetUserGroups(ctx context.Context, username string) ([]string, error)
	IsInGroup(ctx context.Context, username, group string) (bool, error)
}
