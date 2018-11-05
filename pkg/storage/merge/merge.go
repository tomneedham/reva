package merge

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/cernbox/reva/pkg/logger"
	"github.com/cernbox/reva/pkg/storage"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
)

type ms struct {
	logger *logger.Logger
	mounts []*mount
}

// New returns an implementation of the storage.Storage interface that merges one or more storages.
func New(logOut io.Writer, logKey interface{}) storage.Storage {
	ms := &ms{
		logger: logger.New(logOut, "ms", logKey),
		mounts: []*mount{},
	}

	return ms
}

func (m *ms) listMounts(ctx context.Context) ([]*mount, error) {
	return m.mounts, nil
}

func (m *ms) RegisterStorage(ctx context.Context, mount *mount) error {
	if err := validatePath(mount.getMountPath()); err != nil {
		return err
	}

	/*
		TODO(labkode): double check
			if mount.getMountPath() == "/" {
				err := storage.NewError(storage.PathInvalidError).WithMessage("mount point cannot be /")
				m.l.Error("", zap.Error(err))
				return err
			}
	*/

	// TODO(labkode): add check for duplicate mounts
	m.mounts = append(m.mounts, mount)
	return nil
}

func (m *ms) getMount(p string) (*mount, error) {
	// TODO(labkode): if more than 2 matches, check for longest
	p = path.Clean(p)
	if err := validatePath(p); err != nil {
		return nil, err
	}

	for _, m := range m.mounts {
		if strings.HasPrefix(p, m.getMountPath()) {
			return m, nil
		}
		if strings.HasPrefix(p, m.getMountID()) {
			return m, nil
		}
	}

	err := notFoundError(p)
	return nil, err
}

func (m *ms) RemoveMount(ctx context.Context, mountPath string) error {
	for i, mount := range m.mounts {
		if mount.getMountPath() == mountPath {
			m.mounts = append(m.mounts[:i], m.mounts[i+1])
		}
	}
	return nil
}

func (m *ms) GetPathByID(ctx context.Context, id string) (string, error) {
	id = path.Clean(id)
	if !m.isIDPath(id) {
		err := pathInvalidError(id)
		//err := storage.NewError(storage.PathInvalidError).WithMessage("path is not id-based: " + id)
		return "", err
	}
	mount, err := m.getMount(id)
	if err != nil {
		return "", err
	}
	return mount.GetPathByID(ctx, id)
}

func (m *ms) SetACL(ctx context.Context, path string, a *storage.ACL) error {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return err
	}
	return mount.SetACL(ctx, derefPath, a)

}

func (m *ms) UnsetACL(ctx context.Context, path string, a *storage.ACL) error {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return err
	}
	return mount.UnsetACL(ctx, derefPath, a)
}

func (m *ms) UpdateACL(ctx context.Context, path string, a *storage.ACL) error {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return err
	}
	return mount.UpdateACL(ctx, derefPath, a)
}

func (m *ms) GetQuota(ctx context.Context, path string) (int, int, error) {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return 0, 0, err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return 0, 0, err
	}
	return mount.GetQuota(ctx, derefPath)
}

func (m *ms) CreateDir(ctx context.Context, path string) error {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return err
	}
	return mount.CreateDir(ctx, derefPath)
}

func (m *ms) Delete(ctx context.Context, path string) error {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return err
	}
	return mount.Delete(ctx, derefPath)
}

func (m *ms) Move(ctx context.Context, oldPath, newPath string) error {
	derefOldPath, err := m.getDereferencedPath(ctx, oldPath)
	if err != nil {
		return err
	}
	derefNewPath, err := m.getDereferencedPath(ctx, newPath)
	if err != nil {
		return err
	}

	//TODO(labkode): handle 3rd party copy between two different mount points
	fromMount, err := m.getMount(derefOldPath)
	if err != nil {
		return err
	}
	toMount, err := m.getMount(derefNewPath)
	if err != nil {
		return err
	}
	if fromMount.getMountPath() == toMount.getMountPath() {
		err := fromMount.Move(ctx, derefOldPath, derefNewPath)
		return err
	}

	err = notSupportedError("move")
	return err
}
func (m *ms) GetMD(ctx context.Context, path string) (*storage.MD, error) {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return nil, err
	}

	/*
		if derefPath == "/" {
			return m.inspectRootNode(ctx)
		}
	*/

	mount, err := m.getMount(derefPath)
	if err != nil {
		return nil, err
	}
	md, err := mount.GetMD(ctx, derefPath)
	if err != nil {
		return nil, err
	}
	return md, nil
}

func (m *ms) inspectRootNode(ctx context.Context) (*storage.MD, error) {
	// TODO(labkode): generate the ETAG from concatenation of sorted children etag
	// TODO(labkode): generated the mtime as most recent from childlren
	// TODO(labkode): generate size as sum of sizes from children

	/*
		mds, err := m.listRootNode(ctx)
		if err != nil {
			return nil, err
		}
	*/
	uuid := uuid.Must(uuid.NewV4())
	etag := uuid.String()

	md := &storage.MD{
		Path:  "/",
		Size:  0,
		Etag:  etag,
		IsDir: true,
		ID:    "root",
	}
	return md, nil
}

func (m *ms) listRootNode(ctx context.Context) ([]*storage.MD, error) {
	finfos := []*storage.MD{}
	for _, m := range m.mounts {
		finfo, err := m.GetMD(ctx, m.getMountPath())
		if err != nil {
			// we skip wrong entries from the root node
			continue
		}
		finfos = append(finfos, finfo)
	}
	return finfos, nil
}
func (m *ms) ListFolder(ctx context.Context, p string) ([]*storage.MD, error) {
	derefPath, err := m.getDereferencedPath(ctx, p)
	if err != nil {
		return nil, err
	}
	/*
		if derefPath == "/" {
			return m.listRootNode(ctx)
		}
	*/

	mount, err := m.getMount(derefPath)
	if err != nil {
		return nil, err
	}

	mds, err := mount.ListFolder(ctx, derefPath)
	if err != nil {
		return nil, err
	}
	return mds, nil
}

func (m *ms) Upload(ctx context.Context, path string, r io.ReadCloser) error {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return err
	}
	err = mount.Upload(ctx, derefPath, r)
	if err != nil {
		return err
	}
	return nil
}

func (m *ms) Download(ctx context.Context, path string) (io.ReadCloser, error) {

	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return nil, err
	}

	mount, err := m.getMount(derefPath)
	if err != nil {
		return nil, err
	}
	r, err := mount.Download(ctx, derefPath)
	if err != nil {
		return nil, err

	}
	return r, nil
}

func (m *ms) ListRevisions(ctx context.Context, path string) ([]*storage.Revision, error) {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return nil, err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return nil, err
	}
	revs, err := mount.ListRevisions(ctx, derefPath)
	if err != nil {
		return nil, err

	}
	return revs, nil
}

func (m *ms) DownloadRevision(ctx context.Context, path, revisionKey string) (io.ReadCloser, error) {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return nil, err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return nil, err
	}
	r, err := mount.DownloadRevision(ctx, derefPath, revisionKey)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (m *ms) RestoreRevision(ctx context.Context, path, revisionKey string) error {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return err
	}
	err = mount.RestoreRevision(ctx, derefPath, revisionKey)
	if err != nil {
		return err
	}
	return nil
}

func (m *ms) EmptyRecycle(ctx context.Context, path string) error {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return err
	}
	err = mount.EmptyRecycle(ctx, derefPath)
	if err != nil {
		return err
	}
	return nil
}

func (m *ms) ListRecycle(ctx context.Context, path string) ([]*storage.RecycleItem, error) {
	derefPath, err := m.getDereferencedPath(ctx, path)
	if err != nil {
		return nil, err
	}
	mount, err := m.getMount(derefPath)
	if err != nil {
		return nil, err
	}
	entries, err := mount.ListRecycle(ctx, derefPath)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (m *ms) RestoreRecycleItem(ctx context.Context, restoreKey string) error {
	mount, err := m.getMount(restoreKey)
	if err != nil {
		return err
	}

	return mount.RestoreRecycleItem(ctx, restoreKey)
}

func validatePath(p string) error {
	if strings.HasPrefix(p, "/") {
		return nil
	}

	// it can be a namespaced path like home:123
	tokens := strings.Split(p, "/")
	if len(tokens) == 0 {
		err := pathInvalidError("fn does not start with / or mount:id syntax")
		return err

	}
	// home:123
	mount := tokens[0]
	tokens = strings.Split(mount, ":")
	if len(tokens) < 2 {
		err := pathInvalidError("fn does not start with / or mount:id syntax")
		return err

	}
	if tokens[1] == "" {
		err := pathInvalidError("fn does not start with / or mount:id syntax")
		return err
	}
	return nil
}

func (m *ms) isTreePath(path string) bool {
	return strings.HasPrefix(path, "/")
}

// isIDPath checks if the path is id-based, i.e. home:123/docs
func (m *ms) isIDPath(id string) bool {
	if strings.HasPrefix(id, "/") {
		return false
	}

	tokens := strings.Split(id, ":")
	if len(tokens) < 2 {
		return false

	}
	if tokens[1] == "" {
		return false
	}

	if strings.Contains(tokens[1], "/") {
		return false // is mixed-path
	}

	return true
}

func (m *ms) isMixedPath(p string) (bool, string, string) {
	if strings.HasPrefix(p, "/") {
		return false, "", ""
	}

	tokens := strings.Split(p, ":")
	if len(tokens) < 2 {
		return false, "", ""

	}
	if tokens[1] == "" {
		return false, "", ""
	}

	otokens := strings.Split(strings.Join(tokens[1:], ":"), "/")
	id := tokens[0] + ":" + otokens[0]

	return true, id, path.Join(otokens[1:]...)
}

func (m *ms) getDereferencedPath(ctx context.Context, p string) (string, error) {
	p = path.Clean(p)
	if m.isTreePath(p) {
		return p, nil
	}

	if m.isIDPath(p) {
		return m.GetPathByID(ctx, p)
	}

	if ok, id, tail := m.isMixedPath(p); ok {
		derefPath, err := m.GetPathByID(ctx, id)
		if err != nil {
			return "cannot get path by id", err
		}
		return path.Join(derefPath, tail), nil
	}
	err := pathInvalidError("fn is does not match path layouts: slash, id or mixed")
	return "", err
}

type mount struct {
	storage   storage.Storage
	mountPath string
	mountID   string
	options   *options
	logger    *logger.Logger
}

func (m *mount) isReadOnly() bool {
	return m.options.readOnly
}

func (m *mount) isSharingEnabled() bool {
	if m.isReadOnly() {
		return false
	}
	return !m.options.sharingDisabled
}

func (m *mount) getMountPath() string        { return m.mountPath }
func (m *mount) getMountID() string          { return m.mountID }
func (m *mount) getOptions() *options        { return m.options }
func (m *mount) getStorage() storage.Storage { return m.storage }

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
	return path.Join(m.getMountPath(), p), nil
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

	m.logger.Logf(ctx, "translate fn from inner=%s to outter=%s", internalPath, fi.Path)

	fi.ID = m.getMountID() + fi.ID
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
			f.DerefPath = path.Join(m.getMountPath(), path.Clean(f.DerefPath))
		}
		internalPath := path.Clean(f.Path)
		// add mount prefix
		f.Path = path.Join(mountPrefix, internalPath)
		m.logger.Logf(ctx, "fn translate from inner=%s to outter=%s", internalPath, f.Path)
		f.ID = m.getMountID() + f.ID
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
		e.RestoreKey = fmt.Sprintf("%s%s", m.mountID, e.RestoreKey)
		e.RestorePath = path.Join(m.mountPath, e.RestorePath)
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
	if strings.HasPrefix(restoreKey, m.mountID) {
		internalRestoreKey := strings.TrimPrefix(restoreKey, m.mountID)
		return internalRestoreKey, nil
	}
	err := pathInvalidError("mount: invalid restore key for this mount")
	return "", errors.Wrap(err, "mount: invalid path")

}
func (m *mount) getInternalPath(ctx context.Context, p string) (string, string, error) {
	if strings.HasPrefix(p, m.mountPath) {
		internalPath := path.Join("/", strings.TrimPrefix(p, m.mountPath))
		return internalPath, m.mountPath, nil
	}
	err := pathInvalidError("mount: invalid fn for this mount. mountpoint:" + m.mountPath + " fn:" + p)
	return "", "", errors.Wrap(err, "mount: invalid path")
}

type notFoundError string
type permissionDeniedError string
type pathInvalidError string
type notSupportedError string

func (e notFoundError) Error() string               { return string(e) }
func (e notFoundError) IsNotFound()                 {}
func (e permissionDeniedError) Error() string       { return string(e) }
func (e permissionDeniedError) IsPermissionDenied() {}

func (e notSupportedError) Error() string { return string(e) }

func (e pathInvalidError) Error() string { return string(e) }

type options struct {
	readOnly        bool
	sharingDisabled bool
}
