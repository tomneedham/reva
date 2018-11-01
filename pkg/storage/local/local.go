package local

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/cernbox/reva/pkg/storage"
)

type Options struct {
	// Namespace for path operations
	Namespace string `json:"fnspace"`
}

func (opt *Options) init() {
	opt.Namespace = path.Clean(path.Join("/", opt.Namespace))
	if opt.Namespace == "" {
		opt.Namespace = "/"
	}
}

func New(opt *Options) storage.Storage {
	opt.init()
	s := new(localStorage)
	s.fnspace = opt.Namespace
	return s
}

func (fs *localStorage) addNamespace(p string) string {
	np := path.Join(fs.fnspace, p)
	return np
}

func (fs *localStorage) removeNamespace(np string) string {
	p := strings.TrimPrefix(np, fs.fnspace)
	if p == "" {
		p = "/"
	}
	return p
}

type localStorage struct {
	fnspace string
}

func (fs *localStorage) convertToFileInfoWithNamespace(osFileInfo os.FileInfo, np string) *storage.MD {
	fi := &storage.MD{}
	fi.IsDir = osFileInfo.IsDir()
	fi.Path = fs.removeNamespace(path.Join("/", np))
	fi.Size = uint64(osFileInfo.Size())
	fi.ID = fi.Path
	fi.Etag = fmt.Sprintf("%d", osFileInfo.ModTime().Unix())
	return fi
}

func (fs *localStorage) GetPathByID(ctx context.Context, id string) (string, error) {
	return "", notSupportedError("op not supported")
}

func (fs *localStorage) SetACL(ctx context.Context, path string, a *storage.ACL) error {
	return notSupportedError("op not supported")
}

func (fs *localStorage) UnsetACL(ctx context.Context, path string, a *storage.ACL) error {
	return notSupportedError("op not supported")
}
func (fs *localStorage) UpdateACL(ctx context.Context, path string, a *storage.ACL) error {
	return notSupportedError("op not supported")
}

func (fs *localStorage) GetQuota(ctx context.Context, fn string) (int, int, error) {
	return 0, 0, nil
}

func (fs *localStorage) CreateDir(ctx context.Context, fn string) error {
	fn = fs.addNamespace(fn)
	return os.Mkdir(fn, 0644)
}

func (fs *localStorage) Delete(ctx context.Context, fn string) error {
	fn = fs.addNamespace(fn)
	err := os.Remove(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return notFoundError("no such file or directory")
		}
		return err
	}
	return nil
}

func (fs *localStorage) Move(ctx context.Context, oldName, newName string) error {
	oldName = fs.addNamespace(oldName)
	newName = fs.addNamespace(newName)
	return os.Rename(oldName, newName)
}

func (fs *localStorage) GetMD(ctx context.Context, fn string) (*storage.MD, error) {
	fn = fs.addNamespace(fn)
	osFileInfo, err := os.Stat(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, notFoundError("no such file or directory")
		}
		return nil, err
	}
	return fs.convertToFileInfoWithNamespace(osFileInfo, fn), nil
}

func (fs *localStorage) ListFolder(ctx context.Context, fn string) ([]*storage.MD, error) {
	fn = fs.addNamespace(fn)
	osFileInfos, err := ioutil.ReadDir(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, notFoundError("no such file or directory")
		}
		return nil, err
	}
	finfos := []*storage.MD{}
	for _, osFileInfo := range osFileInfos {
		finfos = append(finfos, fs.convertToFileInfoWithNamespace(osFileInfo, path.Join(fn, osFileInfo.Name())))
	}
	return finfos, nil
}

func (fs *localStorage) Upload(ctx context.Context, fn string, r io.ReadCloser) error {
	fn = fs.addNamespace(fn)
	// we cannot rely on /tmp as it can live in another partition and we can
	// hit invalid cross-device link errors, so we create the tmp file in the same directory
	// the file is supposed to be written.
	tmp, err := ioutil.TempFile(path.Dir(fn), ".alustotmp-")
	if err != nil {
		return err
	}
	_, err = io.Copy(tmp, r)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), fn); err != nil {
		if os.IsNotExist(err) {
			return notFoundError("no such file or directory")
		}
		return err
	}
	return nil
}

func (fs *localStorage) Download(ctx context.Context, fn string) (io.ReadCloser, error) {
	fn = fs.addNamespace(fn)
	r, err := os.Open(fn)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, notFoundError("no such file or directory")
		}
	}
	return r, nil
}

func (fs *localStorage) ListRevisions(ctx context.Context, path string) ([]*storage.Revision, error) {
	return nil, notSupportedError("op not supported")
}

func (fs *localStorage) DownloadRevision(ctx context.Context, path, revisionKey string) (io.ReadCloser, error) {
	return nil, notSupportedError("op not supported")
}

func (fs *localStorage) RestoreRevision(ctx context.Context, path, revisionKey string) error {
	return notSupportedError("op not supported")
}

func (fs *localStorage) EmptyRecycle(ctx context.Context, path string) error {
	return notSupportedError("op not supported")
}

func (fs *localStorage) ListRecycle(ctx context.Context, path string) ([]*storage.RecycleItem, error) {
	return nil, notSupportedError("op not supported")
}

func (fs *localStorage) RestoreRecycleItem(ctx context.Context, restoreKey string) error {
	return notSupportedError("op not supported")
}

type notSupportedError string
type notFoundError string

func (e notSupportedError) Error() string   { return string(e) }
func (e notSupportedError) IsNotSupported() {}
func (e notFoundError) Error() string       { return string(e) }
func (e notFoundError) IsNotFound()         {}
