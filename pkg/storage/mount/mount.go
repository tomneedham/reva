package mount

import (
	"github.com/cernbox/reva/pkg/storage"
)

type mount struct {
	name, dir string
	fs        storage.FS
	opts      *storage.MountOptions
}

func New() storage.Mount {
	return &mount{}
}

func (m *mount) GetFSName() string {
	return m.name
}

func (m *mount) GetDir() string                    { return m.dir }
func (m *mount) GetFS() storage.FS                 { return m.fs }
func (m *mount) GetOptions() *storage.MountOptions { return m.opts }
