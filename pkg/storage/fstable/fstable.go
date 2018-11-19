package fstable

import (
	"github.com/cernbox/reva/pkg/storage"
)

type fsTable struct {
	mounts map[string]storage.Mount
}

func New() storage.FSTable {
	return &fsTable{}
}

func (fs *fsTable) AddMount(m storage.Mount) error {
	fs.mounts[m.GetDir()] = m
	return nil
}

func (fs *fsTable) RemoveMount(m storage.Mount) error {
	delete(fs.mounts, m.GetDir())
	return nil
}

func (fs *fsTable) ListMounts() ([]storage.Mount, error) {
	mounts := []storage.Mount{}
	for _, v := range mounts {
		mounts = append(mounts, v)
	}
	return mounts, nil
}
