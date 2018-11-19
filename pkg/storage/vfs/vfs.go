package vfs

import (
	"context"
	"github.com/cernbox/reva/pkg/storage"
	"github.com/pkg/errors"
	"path"
	"strings"
)

type vfs struct {
	fsTable storage.FSTable
}

func New(fsTable storage.FSTable) storage.FS {
	fs := &vfs{fsTable: fsTable}
	return fs
}

func validate(fn string) error {
	if !strings.HasPrefix(fn, "/") {
		return invalidFilenameError(fn)
	}
}

// findMount finds the mount for the given filename.
// it assumes the filename starts with slash and the path is
// already cleaned. The cases to handle are the following:
// - /
// - /docs
// - /docs/
// - /docs/one
func (fs *vfs) findMount(fn string) (storage.Mount, string, error) {
	// len tokens is always >=2 based on the assumption that
	// fn always starts with leading slash (/).
	tokens := strings.Split(fn, "/")
	dir := path.Join("/", tokens[1])
	m, ok := fs.mounts[dir]
	if !ok {
		return nil, "", mountNotFoundError(dir)
	}
	fsfn := path.Join("/", strings.TrimPrefix(fn, dir))
	return m, fsfn, nil
}

func (fs *vfs) CreateDir(ctx context.Context, fn string) error {
	if err := validate(fn); err != nil {
		return errors.Wrap(err, "vfs: invalid fn")
	}

	mount, fsfn, err := fs.findMount(fn)
	if err != nil {
		return errors.Wrap(err, "vfs: mount not found")
	}

	fs := mount.GetFS()
	if err := fs.CreateDir(ctx, fsfn); err != nil {
		return errors.Wrap(err, "vfs: error creating dir")
	}
	return nil
}

func (fs *vfs) Delete(ctx context.Context, fn string) error {
	if err := validate(fn); err != nil {
		return errors.Wrap(err, "vfs: invalid fn")
	}

	mount, fsfn, err := fs.findMount(fn)
	if err != nil {
		return errors.Wrap(err, "vfs: mount not found")
	}

	fs := mount.GetFS()
	if err := fs.Delete(ctx, fsfn); err != nil {
		return errors.Wrap(err, "vfs: error deleting dir")
	}
	return nil
}

func (fs *vfs) Delete(ctx context.Context, fn string) error {
	if err := validate(fn); err != nil {
		return errors.Wrap(err, "vfs: invalid fn")
	}

	mount, fsfn, err := fs.findMount(fn)
	if err != nil {
		return errors.Wrap(err, "vfs: mount not found")
	}

	fs := mount.GetFS()
	if err := fs.Delete(ctx, fsfn); err != nil {
		return errors.Wrap(err, "vfs: error deleting dir")
	}
	return nil
}

type invalidFilenameError string
type mountNotFoundError string

func (e invalidFilenameError) Error() string { return e }
func (e invalidFilenameError) IsNotFound()   {}
func (e mountNotFoundError) Error() string   { return e }
func (e mountNotFoundError) IsNotFound()     {}
