package tag

import (
	"context"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/user"
)

const (
	TagTypeFile      = "file"
	TagTypeDirectory = "directory"
)

type (
	TagType string

	TagManager interface {
		GetTagsForKey(ctx context.Context, u *user.User, key string) ([]*Tag, error)
		SetTag(ctx context.Context, u *user.User, key, val string, md *storage.MD) error
		UnSetTag(ctx context.Context, u *user.User, key, val string, md *storage.MD) error
	}

	Tag struct {
		ID       int64
		Owner    string
		Filename string
		Type     TagType
		Key      string
		Value    string
	}
)
