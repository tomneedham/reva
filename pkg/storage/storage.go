package storage

import (
	"context"
	"io"
)

// ACLMode represents the mode for the ACL (read, write, ...).
type ACLMode uint32

// ACLType represents the type of the ACL (user, group, ...).
type ACLType string

const (
	// ACLModeRead specifies read permissions.
	ACLModeRead = ACLMode(1) // 1
	// ACLModeWrite specifies write-permissions.
	ACLModeWrite = ACLMode(1 << 1) // 2

	// ACLTypeUser specifies that the acl is set for an individual user.
	ACLTypeUser ACLType = "user"
	// ACLTypeGroup specifies that the acl is set for a group.
	ACLTypeGroup ACLType = "group"
)

// Storage is the interface to implement access to the storage.
type Storage interface {
	CreateDir(ctx context.Context, fn string) error
	Delete(ctx context.Context, fn string) error
	Move(ctx context.Context, old, new string) error
	GetMD(ctx context.Context, fn string) (*MD, error)
	ListFolder(ctx context.Context, fn string) ([]*MD, error)
	Upload(ctx context.Context, fn string, r io.ReadCloser) error
	Download(ctx context.Context, fn string) (io.ReadCloser, error)
	ListRevisions(ctx context.Context, fn string) ([]*Revision, error)
	DownloadRevision(ctx context.Context, fn, key string) (io.ReadCloser, error)
	RestoreRevision(ctx context.Context, fn, key string) error
	ListRecycle(ctx context.Context, fn string) ([]*RecycleItem, error)
	RestoreRecycleItem(ctx context.Context, key string) error
	EmptyRecycle(ctx context.Context, fn string) error
	GetPathByID(ctx context.Context, id string) (string, error)
	SetACL(ctx context.Context, fn string, a *ACL) error
	UnsetACL(ctx context.Context, fn string, a *ACL) error
	UpdateACL(ctx context.Context, fn string, a *ACL) error
	GetQuota(ctx context.Context, fn string) (int, int, error)
}

// MD represents the metadata about a file/directory.
type MD struct {
	ID          string
	Path        string
	Size        uint64
	Mtime       uint64
	IsDir       bool
	Etag        string
	Checksum    string
	DerefPath   string
	IsReadOnly  bool
	IsShareable bool
	Mime        string

	Sys       []byte
	TreeCount uint64

	EosFile     string
	EosInstance string

	ShareTarget string
}

// ACL represents an ACL to persist on the storage.
type ACL struct {
	Target string
	Type   ACLType
	Mode   ACLMode
}

// RecycleItem represents an entry in the recycle bin of the user.
type RecycleItem struct {
	RestorePath string
	RestoreKey  string
	Size        uint64
	DelMtime    uint64
	IsDir       bool
}

// Revision represents a version of the file in the past.
type Revision struct {
	RevKey string
	Size   uint64
	Mtime  uint64
	IsDir  bool
}
