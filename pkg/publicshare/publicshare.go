package publicshare

import (
	"context"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/user"
)

const (
	ACLModeReadOnly  ACLMode = "read-only"
	ACLModeReadWrite ACLMode = "read-write"

	ACLTypeDirectory ACLType = "directory"
	ACLTypeFile      ACLType = "file"
)

type (
	PublicShareManager interface {
		CreatePublicShare(ctx context.Context, u *user.User, md *storage.MD, a *ACL) (*PublicShare, error)
		UpdatePublicShare(ctx context.Context, u *user.User, id string, up *UpdatePolicy, a *ACL) (*PublicShare, error)
		GetPublicShare(ctx context.Context, u *user.User, id string) (*PublicShare, error)
		ListPublicShares(ctx context.Context, u *user.User, md *storage.MD) ([]*PublicShare, error)
		RevokePublicShare(ctx context.Context, u *user.User, id string) error

		GetPublicShareByToken(ctx context.Context, token string) (*PublicShare, error)
	}

	PublicShare struct {
		ID          string
		Token       string
		Filename    string
		Modified    uint64
		Owner       string
		DisplayName string
		ACL         *ACL
	}

	ACL struct {
		Password   string
		Expiration uint64
		SetMode    bool
		Mode       ACLMode
		Type       ACLType
	}

	UpdatePolicy struct {
		SetPassword   bool
		SetExpiration bool
		SetMode       bool
	}

	ACLMode string
	ACLType string
)

/*
AuthenticatePublicShare(ctx context.Context, token, password string) (*PublicShare, error)
	IsPublicShareProtected(ctx context.Context, token string) (bool, error)
*/
