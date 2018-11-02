package storage

import (
	"context"
	"io"
)

type ACLMode uint32
type ACLType string

const (
	ACLModeRead  = ACLMode(1)      // 1
	ACLModeWrite = ACLMode(1 << 1) // 2

	ACLTypeUser  ACLType = "user"
	ACLTypeGroup ACLType = "group"
)

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
	MigId       string
	MigPath     string
}

type ACL struct {
	Target string
	Type   ACLType
	Mode   ACLMode
}

type RecycleItem struct {
	RestorePath string
	RestoreKey  string
	Size        uint64
	DelMtime    uint64
	IsDir       bool
}

type Revision struct {
	RevKey string
	Size   uint64
	Mtime  uint64
	IsDir  bool
}

// Mount contains the information about a mount.
// Similar to "struct mntent" in /usr/include/mntent.h.
// See also getent(8).
// A Mount exposes two mount points, one fn based and another namespace based.
// A fn-based mount point can be '/home', a namespaced mount-point can be 'home:1234'
type Mount interface {
	Storage
	GetMountPoint() string
	GetMountPointId() string
	GetMountOptions() *MountOptions
	GetStorage() Storage
}

type MountTable struct {
	Mounts []*MountTableEntry `json:"mounts"`
}

type MountTableEntry struct {
	MountPoint      string            `json:"mount_point"`
	MountID         string            `json:"mount_id"`
	MountOptions    *MountOptions     `json:"mount_options"`
	StorageDriver   string            `json:"storage_driver"`
	StorageOptions  interface{}       `json:"storage_options"`
	StorageWrappers []*StorageWrapper `json:"storage_wrappers"`
}

type StorageWrapper struct {
	Priority int         `json:"priority"`
	Name     string      `json:"name"`
	Options  interface{} `json:"options"`
}

// A VirtualStorage is similar to the
// Linux VFS (Virtual File Switch).
type VirtualStorage interface {
	AddMount(ctx context.Context, mount Mount) error
	RemoveMount(ctx context.Context, mountPoint string) error
	ListMounts(ctx context.Context) ([]Mount, error)
	GetMount(fn string) (Mount, error)
	Storage
}

type MountOptions struct {
	ReadOnly        bool `json:"read_only"`
	SharingDisabled bool `json:"sharing_disabled"`
}
