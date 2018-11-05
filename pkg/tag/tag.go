package tag

import (
	"context"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/user"
)

const (
	// TypeFile specifies tha the tag is attached to a file.
	TypeFile = "file"
	// TypeDirectory specifies that the tag is attached to a directory.
	TypeDirectory = "directory"
)

type (
	// Type represents the type of tag (file, directory, ...).
	Type string

	// Manager is the interface to manipulate tags.
	Manager interface {
		GetTagsForKey(ctx context.Context, u *user.User, key string) ([]*Tag, error)
		SetTag(ctx context.Context, u *user.User, key, val string, md *storage.MD) error
		UnSetTag(ctx context.Context, u *user.User, key, val string, md *storage.MD) error
	}

	// Tag represents a tag.
	Tag struct {
		ID       int64
		Owner    string
		Filename string
		Type     Type
		Key      string
		Value    string
	}
)
