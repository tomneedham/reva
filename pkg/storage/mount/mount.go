package mount

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/pkg/errors"
)

// New will return a new mount with specific mount options.
func New(mountID, mountPoint string, opts *storage.MountOptions, s storage.Storage) storage.Mount {
	mountPoint = path.Clean(mountPoint)
	if mountPoint != "/" {
		mountPoint = strings.TrimSuffix(mountPoint, "/")
	}

	if opts == nil {
		opts = &storage.MountOptions{}
	}

	m := &mount{storage: s,
		mountPoint:   mountPoint,
		mountOptions: opts,
	}
	m.mountPointId = mountID + ":"
	return m
}

type mount struct {
	storage      storage.Storage
	mountPoint   string
	mountPointId string
	mountOptions *storage.MountOptions
	logger       *logger
}

func (m *mount) isReadOnly() bool {
	return m.mountOptions.ReadOnly
}

func (m *mount) isSharingEnabled() bool {
	if m.isReadOnly() {
		return false
	}
	return !m.mountOptions.SharingDisabled
}

func (m *mount) GetMountPoint() string                  { return m.mountPoint }
func (m *mount) GetMountPointId() string                { return m.mountPointId }
func (m *mount) GetMountOptions() *storage.MountOptions { return m.mountOptions }
func (m *mount) GetStorage() storage.Storage            { return m.storage }

func (m *mount) GetQuota(ctx context.Context, fn string) (int, int, error) {
	p, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return 0, 0, err
	}
	return m.storage.GetQuota(ctx, p)
}

func (m *mount) GetPathByID(ctx context.Context, id string) (string, error) {
	id, err := m.getInternalIDPath(ctx, id)
	if err != nil {
		return "", err
	}
	p, err := m.storage.GetPathByID(ctx, id)
	if err != nil {
		return "", err
	}
	return path.Join(m.GetMountPoint(), p), nil
}

func (m *mount) SetACL(ctx context.Context, fn string, a *storage.ACL) error {
	if !m.isSharingEnabled() {
		err := permissionDeniedError("sharing is disabled")
		return errors.Wrapf(err, "mount: permission denied to set acl for fn=%s", fn)
	}
	p, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return err
	}
	return m.storage.SetACL(ctx, p, a)
}

func (m *mount) UpdateACL(ctx context.Context, fn string, a *storage.ACL) error {
	if !m.isSharingEnabled() {
		err := permissionDeniedError("sharing is disabled")
		return errors.Wrapf(err, "mount: permission denied to set acl for fn=%s", fn)
	}
	p, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return err
	}
	return m.storage.UpdateACL(ctx, p, a)
}

func (m *mount) UnsetACL(ctx context.Context, fn string, a *storage.ACL) error {
	if !m.isSharingEnabled() {
		err := permissionDeniedError("sharing is disabled")
		return errors.Wrapf(err, "mount: permission denied to set acl for fn=%s", fn)
	}
	p, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return err
	}
	return m.storage.UnsetACL(ctx, p, a)
}

func (m *mount) CreateDir(ctx context.Context, fn string) error {
	if m.isReadOnly() {
		err := permissionDeniedError("mount is read-only")
		return errors.Wrapf(err, "mount: create dir denied for fn=%s because mount is read-only", fn)
	}
	p, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return err
	}
	return m.storage.CreateDir(ctx, p)
}

func (m *mount) Delete(ctx context.Context, fn string) error {
	if m.isReadOnly() {
		err := permissionDeniedError("mount is read-only")
		return errors.Wrapf(err, "mount: delete denied for fn=%s because mount is read-only", fn)
	}
	p, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return err
	}
	return m.storage.Delete(ctx, p)
}

func (m *mount) Move(ctx context.Context, oldPath, newPath string) error {
	if m.isReadOnly() {
		err := permissionDeniedError("mount is read-only")
		return errors.Wrapf(err, "mount: move denied for oldPath=%s newPath=%s because mount is read-only", oldPath, newPath)
	}
	op, _, err := m.getInternalPath(ctx, oldPath)
	if err != nil {
		return err
	}
	np, _, err := m.getInternalPath(ctx, newPath)
	if err != nil {
		return err
	}
	return m.storage.Move(ctx, op, np)
}
func (m *mount) GetMD(ctx context.Context, p string) (*storage.MD, error) {
	internalPath, mountPrefix, err := m.getInternalPath(ctx, p)
	if err != nil {
		return nil, err
	}

	fi, err := m.storage.GetMD(ctx, internalPath)
	if err != nil {
		return nil, err
	}

	internalPath = path.Clean(fi.Path)
	fi.Path = path.Join(mountPrefix, internalPath)

	m.logger.logf(ctx, "translate fn from inner=%s to outter=%s", internalPath, fi.Path)

	fi.ID = m.GetMountPointId() + fi.ID
	if fi.IsShareable {
		fi.IsShareable = m.isSharingEnabled()
	}
	if !fi.IsReadOnly {
		fi.IsReadOnly = m.isReadOnly()
	}

	return fi, nil
}

func (m *mount) ListFolder(ctx context.Context, p string) ([]*storage.MD, error) {
	internalPath, mountPrefix, err := m.getInternalPath(ctx, p)
	if err != nil {
		return nil, err
	}

	finfos, err := m.storage.ListFolder(ctx, internalPath)
	if err != nil {
		return nil, err
	}

	for _, f := range finfos {
		if f.DerefPath != "" {
			f.DerefPath = path.Join(m.GetMountPoint(), path.Clean(f.DerefPath))
		}
		internalPath := path.Clean(f.Path)
		// add mount prefix
		f.Path = path.Join(mountPrefix, internalPath)
		m.logger.logf(ctx, "fn translate from inner=%s to outter=%s", internalPath, f.Path)
		f.ID = m.GetMountPointId() + f.ID
		if f.IsShareable {
			f.IsShareable = m.isSharingEnabled()
		}
		if !f.IsReadOnly {
			f.IsReadOnly = m.isReadOnly()
		}
	}

	return finfos, nil
}

func (m *mount) Upload(ctx context.Context, fn string, r io.ReadCloser) error {
	if m.isReadOnly() {
		err := permissionDeniedError("mount is read-only")
		return errors.Wrapf(err, "mount: create dir denied for fn=%s because mount is read-only", fn)
	}
	internalPath, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return err
	}
	return m.storage.Upload(ctx, internalPath, r)
}

func (m *mount) Download(ctx context.Context, fn string) (io.ReadCloser, error) {
	internalPath, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return nil, err
	}
	return m.storage.Download(ctx, internalPath)
}

func (m *mount) ListRevisions(ctx context.Context, fn string) ([]*storage.Revision, error) {
	internalPath, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return nil, err
	}
	return m.storage.ListRevisions(ctx, internalPath)
}

func (m *mount) DownloadRevision(ctx context.Context, fn, revisionKey string) (io.ReadCloser, error) {
	internalPath, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return nil, err
	}
	return m.storage.DownloadRevision(ctx, internalPath, revisionKey)
}

func (m *mount) RestoreRevision(ctx context.Context, fn, revisionKey string) error {
	if m.isReadOnly() {
		err := permissionDeniedError("mount is read-only")
		return errors.Wrapf(err, "mount: create dir denied for fn=%s because mount is read-only", fn)
	}
	internalPath, _, err := m.getInternalPath(ctx, fn)
	if err != nil {
		return err
	}
	return m.storage.RestoreRevision(ctx, internalPath, revisionKey)
}

func (m *mount) EmptyRecycle(ctx context.Context, fn string) error {
	if m.isReadOnly() {
		err := permissionDeniedError("mount is read-only")
		return errors.Wrapf(err, "mount: create dir denied for fn=%s because mount is read-only", fn)
	}
	return m.storage.EmptyRecycle(ctx, fn)
}

func (m *mount) ListRecycle(ctx context.Context, p string) ([]*storage.RecycleItem, error) {
	entries, err := m.storage.ListRecycle(ctx, p)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		e.RestoreKey = fmt.Sprintf("%s%s", m.mountPointId, e.RestoreKey)
		e.RestorePath = path.Join(m.mountPoint, e.RestorePath)
	}
	return entries, nil
}

func (m *mount) RestoreRecycleItem(ctx context.Context, restoreKey string) error {
	if m.isReadOnly() {
		err := permissionDeniedError("mount is read-only")
		return errors.Wrapf(err, "mount: restore recycle denied for key=%s because mount is read-only", restoreKey)
	}
	internalRestoreKey, err := m.getInternalRestoreKey(ctx, restoreKey)
	if err != nil {
		return err
	}
	return m.storage.RestoreRecycleItem(ctx, internalRestoreKey)
}

func (m *mount) getInternalIDPath(ctx context.Context, p string) (string, error) {
	// home:387/docs
	tokens := strings.Split(p, "/")
	if len(tokens) != 1 {
		err := pathInvalidError("fn is not id-based: " + p)
		return "", errors.Wrap(err, "path is invalid")
	}

	mount := tokens[0]
	if mount == "" {
		err := pathInvalidError("fn is not id-based: " + p)
		return "", errors.Wrap(err, "path is invalid")
	}

	tokens = strings.Split(mount, ":")
	if len(tokens) != 2 {
		err := pathInvalidError("fn is not id-based: " + p)
		return "", errors.Wrap(err, "path is invalid")
	}
	return tokens[1], nil
}

func (m *mount) getInternalRestoreKey(ctx context.Context, restoreKey string) (string, error) {
	if strings.HasPrefix(restoreKey, m.mountPointId) {
		internalRestoreKey := strings.TrimPrefix(restoreKey, m.mountPointId)
		return internalRestoreKey, nil
	}
	err := pathInvalidError("mount: invalid restore key for this mount")
	return "", errors.Wrap(err, "mount: invalid path")

}
func (m *mount) getInternalPath(ctx context.Context, p string) (string, string, error) {
	if strings.HasPrefix(p, m.mountPoint) {
		internalPath := path.Join("/", strings.TrimPrefix(p, m.mountPoint))
		return internalPath, m.mountPoint, nil
	}
	err := pathInvalidError("mount: invalid fn for this mount. mountpoint:" + m.mountPoint + " fn:" + p)
	return "", "", errors.Wrap(err, "mount: invalid path")
}

type logger struct {
	out io.Writer
	key interface{}
}

func (l *logger) log(ctx context.Context, msg string) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "eosclient: trace=%s %s", trace, msg)
}

func (l *logger) logf(ctx context.Context, msg string, params ...interface{}) {
	trace := l.getTraceFromCtx(ctx)
	fmt.Fprintf(l.out, "eosclient: trace=%s %s", trace, msg)
}

func (l *logger) getTraceFromCtx(ctx context.Context) string {
	trace, _ := ctx.Value(l.key).(string)
	if trace == "" {
		trace = "notrace"
	}
	return trace
}

type permissionDeniedError string
type pathInvalidError string

func (e permissionDeniedError) Error() string       { return string(e) }
func (e permissionDeniedError) IsPermissionDenied() {}

func (e pathInvalidError) Error() string { return string(e) }
