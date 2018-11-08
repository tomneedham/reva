package eos

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/cernbox/reva/pkg/eosclient"
	"github.com/cernbox/reva/pkg/logger"
	"github.com/cernbox/reva/pkg/mime"
	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/user"
	"github.com/pkg/errors"
)

var hiddenReg = regexp.MustCompile(`\.sys\..#.`)

type contextUserRequiredErr string

func (err contextUserRequiredErr) Error() string   { return string(err) }
func (err contextUserRequiredErr) IsUserRequired() {}

type eosStorage struct {
	c             *eosclient.Client
	mountpoint    string
	logger        *logger.Logger
	showHiddenSys bool
}

// Options are the configuration options to pass to the New function.
type Options struct {
	// Namespace for fn operations
	Namespace string `json:"namespace"`

	// Where to write the logs
	LogOut io.Writer

	// LogKey key to use for storing log traces
	LogKey interface{}

	// Location of the eos binary.
	// Default is /usr/bin/eos.
	EosBinary string `json:"eos_binary"`

	// Location of the xrdcopy binary.
	// Default is /usr/bin/xrdcopy.
	XrdcopyBinary string `json:"xrdcopy_binary"`

	// URL of the Master EOS MGM.
	// Default is root://eos-test.org
	MasterURL string `json:"master_url"`

	// URL of the Slave EOS MGM.
	// Default is root://eos-test.org
	SlaveURL string `json:"slave_url"`

	// Location on the local fs where to store reads.
	// Defaults to os.TempDir()
	CacheDirectory string `json:"cache_directory"`

	// Enables logging of the commands executed
	// Defaults to false
	EnableLogging bool `json:"enable_logging"`

	// ShowHiddenSysFiles shows internal EOS files like
	// .sys.v# and .sys.a# files.
	ShowHiddenSysFiles bool `json:"show_hidden_sys_files"`
}

func getUser(ctx context.Context) (*user.User, error) {
	u, ok := user.ContextGetUser(ctx)
	if !ok {
		err := errors.Wrap(contextUserRequiredErr("userrequired"), "storage_eos: error getting user from ctx")
		return nil, err
	}
	return u, nil
}

func (opt *Options) init() {
	opt.Namespace = path.Clean(opt.Namespace)
	if !strings.HasPrefix(opt.Namespace, "/") {
		opt.Namespace = "/"
	}

	if opt.EosBinary == "" {
		opt.EosBinary = "/usr/bin/eos"
	}

	if opt.XrdcopyBinary == "" {
		opt.XrdcopyBinary = "/usr/bin/xrdcopy"
	}

	if opt.MasterURL == "" {
		opt.MasterURL = "root://eos-example.org"
	}

	if opt.SlaveURL == "" {
		opt.SlaveURL = opt.MasterURL
	}

	if opt.CacheDirectory == "" {
		opt.CacheDirectory = os.TempDir()
	}
}

// New returns a new implementation of the storage.Storage interface that connects to EOS.
func New(opt *Options) (storage.Storage, error) {
	opt.init()

	eosClientOpts := &eosclient.Options{
		XrdcopyBinary:  opt.XrdcopyBinary,
		URL:            opt.MasterURL,
		EosBinary:      opt.EosBinary,
		CacheDirectory: opt.CacheDirectory,
		LogOutput:      opt.LogOut,
		TraceKey:       opt.LogKey,
	}

	eosClient, err := eosclient.New(eosClientOpts)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: error creating eosclient")
	}

	logger := logger.New(opt.LogOut, "eos", opt.LogKey)

	eosStorage := &eosStorage{
		c:             eosClient,
		logger:        logger,
		mountpoint:    opt.Namespace,
		showHiddenSys: opt.ShowHiddenSysFiles,
	}

	return eosStorage, nil
}

func (fs *eosStorage) getInternalPath(ctx context.Context, fn string) string {
	internalPath := path.Join(fs.mountpoint, fn)
	msg := fmt.Sprintf("func=getInternalPath outter=%s inner=%s", fn, internalPath)
	fs.logger.Log(ctx, msg)
	return internalPath
}

func (fs *eosStorage) removeNamespace(ctx context.Context, np string) string {
	p := strings.TrimPrefix(np, fs.mountpoint)
	if p == "" {
		p = "/"
	}

	msg := fmt.Sprintf("func=removeNamespace inner=%s outter=%s", np, p)
	fs.logger.Log(ctx, msg)
	return p
}

func (fs *eosStorage) GetPathByID(ctx context.Context, id string) (string, error) {
	u, err := getUser(ctx)
	if err != nil {
		return "", errors.Wrap(err, "storage_eos: no user in ctx")
	}

	// parts[0] = 868317, parts[1] = photos, ...
	parts := strings.Split(id, "/")
	fileID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return "", errors.Wrap(err, "storage_eos: error parsing fileid string")
	}

	eosFileInfo, err := fs.c.GetFileInfoByInode(ctx, u.Account, fileID)
	if err != nil {
		return "", errors.Wrap(err, "storage_eos: error getting file info by inode")
	}

	fi := fs.convertToMD(ctx, eosFileInfo)
	return fi.Path, nil
}

func (fs *eosStorage) SetACL(ctx context.Context, fn string, a *storage.ACL) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}

	fn = fs.getInternalPath(ctx, fn)

	eosACL := fs.getEosACL(a)

	err = fs.c.AddACL(ctx, u.Account, fn, eosACL)
	if err != nil {
		return errors.Wrap(err, "storage_eos: error adding acl")
	}

	return nil
}

func getEosACLType(acl *storage.ACL) eosclient.ACLType {
	switch acl.Type {
	case storage.ACLTypeUser:
		return eosclient.ACLTypeUser
	case storage.ACLTypeGroup:
		return eosclient.ACLTypeGroup
	}

	panic(acl)
}

func getEosACLPerm(a *storage.ACL) eosclient.ACLMode {
	if (a.Mode & storage.ACLModeWrite) == 1 {
		return eosclient.ACLModeReadWrite
	}
	return eosclient.ACLModeRead
}

func (fs *eosStorage) getEosACL(a *storage.ACL) *eosclient.ACL {
	eosACL := &eosclient.ACL{Target: a.Target}
	eosACL.Mode = getEosACLPerm(a)
	eosACL.Type = getEosACLType(a)
	return eosACL
}

func (fs *eosStorage) UnsetACL(ctx context.Context, fn string, a *storage.ACL) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}

	eosACLType := getEosACLType(a)

	fn = fs.getInternalPath(ctx, fn)

	err = fs.c.RemoveACL(ctx, u.Account, fn, eosACLType, a.Target)
	if err != nil {
		return errors.Wrap(err, "storage_eos: error removing acl")
	}
	return nil
}

func (fs *eosStorage) UpdateACL(ctx context.Context, fn string, a *storage.ACL) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}

	eosACL := fs.getEosACL(a)

	fn = fs.getInternalPath(ctx, fn)
	err = fs.c.AddACL(ctx, u.Account, fn, eosACL)
	if err != nil {
		return errors.Wrap(err, "storage_eos: error updating acl")
	}
	return nil
}

func (fs *eosStorage) GetMD(ctx context.Context, fn string) (*storage.MD, error) {
	u, err := getUser(ctx)
	if err != nil {
		return nil, err
	}

	fn = fs.getInternalPath(ctx, fn)
	eosFileInfo, err := fs.c.GetFileInfoByPath(ctx, u.Account, fn)
	if err != nil {
		return nil, err
	}
	fi := fs.convertToMD(ctx, eosFileInfo)
	return fi, nil
}

func (fs *eosStorage) ListFolder(ctx context.Context, fn string) ([]*storage.MD, error) {
	u, err := getUser(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: no user in ctx")
	}

	fn = fs.getInternalPath(ctx, fn)
	eosFileInfos, err := fs.c.List(ctx, u.Account, fn)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: errong listing")
	}

	finfos := []*storage.MD{}
	for _, eosFileInfo := range eosFileInfos {
		// filter out sys files
		if !fs.showHiddenSys {
			base := path.Base(eosFileInfo.File)
			if hiddenReg.MatchString(base) {
				continue
			}

		}
		finfos = append(finfos, fs.convertToMD(ctx, eosFileInfo))
	}
	return finfos, nil
}

func (fs *eosStorage) GetQuota(ctx context.Context, fn string) (int, int, error) {
	u, err := getUser(ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "storage_eos: no user in ctx")
	}
	fn = fs.getInternalPath(ctx, fn)
	return fs.c.GetQuota(ctx, u.Account, fn)
}

func (fs *eosStorage) CreateDir(ctx context.Context, fn string) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}
	fn = fs.getInternalPath(ctx, fn)
	return fs.c.CreateDir(ctx, u.Account, fn)
}

func (fs *eosStorage) Delete(ctx context.Context, fn string) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}
	fn = fs.getInternalPath(ctx, fn)
	return fs.c.Remove(ctx, u.Account, fn)
}

func (fs *eosStorage) Move(ctx context.Context, oldPath, newPath string) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}
	oldPath = fs.getInternalPath(ctx, oldPath)
	newPath = fs.getInternalPath(ctx, newPath)
	return fs.c.Rename(ctx, u.Account, oldPath, newPath)
}

func (fs *eosStorage) Download(ctx context.Context, fn string) (io.ReadCloser, error) {
	u, err := getUser(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: no user in ctx")
	}
	fn = fs.getInternalPath(ctx, fn)
	return fs.c.Read(ctx, u.Account, fn)
}

func (fs *eosStorage) Upload(ctx context.Context, fn string, r io.ReadCloser) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}
	fn = fs.getInternalPath(ctx, fn)
	return fs.c.Write(ctx, u.Account, fn, r)
}

func (fs *eosStorage) ListRevisions(ctx context.Context, fn string) ([]*storage.Revision, error) {
	u, err := getUser(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: no user in ctx")
	}
	fn = fs.getInternalPath(ctx, fn)
	eosRevisions, err := fs.c.ListVersions(ctx, u.Account, fn)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: error listing versions")
	}
	revisions := []*storage.Revision{}
	for _, eosRev := range eosRevisions {
		rev := fs.convertToRevision(ctx, eosRev)
		revisions = append(revisions, rev)
	}
	return revisions, nil
}

func (fs *eosStorage) DownloadRevision(ctx context.Context, fn, revisionKey string) (io.ReadCloser, error) {
	u, err := getUser(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: no user in ctx")
	}
	fn = fs.getInternalPath(ctx, fn)
	return fs.c.ReadVersion(ctx, u.Account, fn, revisionKey)
}

func (fs *eosStorage) RestoreRevision(ctx context.Context, fn, revisionKey string) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}
	fn = fs.getInternalPath(ctx, fn)
	return fs.c.RollbackToVersion(ctx, u.Account, fn, revisionKey)
}

func (fs *eosStorage) EmptyRecycle(ctx context.Context, fn string) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}
	return fs.c.PurgeDeletedEntries(ctx, u.Account)
}

func (fs *eosStorage) ListRecycle(ctx context.Context, fn string) ([]*storage.RecycleItem, error) {
	u, err := getUser(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: no user in ctx")
	}
	eosDeletedEntries, err := fs.c.ListDeletedEntries(ctx, u.Account)
	if err != nil {
		return nil, errors.Wrap(err, "storage_eos: error listing deleted entries")
	}
	recycleEntries := []*storage.RecycleItem{}
	for _, entry := range eosDeletedEntries {
		if !fs.showHiddenSys {
			base := path.Base(entry.RestorePath)
			if hiddenReg.MatchString(base) {
				continue
			}

		}
		recycleItem := fs.convertToRecycleItem(ctx, entry)
		recycleEntries = append(recycleEntries, recycleItem)
	}
	return recycleEntries, nil
}

func (fs *eosStorage) RestoreRecycleItem(ctx context.Context, key string) error {
	u, err := getUser(ctx)
	if err != nil {
		return errors.Wrap(err, "storage_eos: no user in ctx")
	}
	return fs.c.RestoreDeletedEntry(ctx, u.Account, key)
}

func (fs *eosStorage) convertToRecycleItem(ctx context.Context, eosDeletedItem *eosclient.DeletedEntry) *storage.RecycleItem {
	recycleItem := &storage.RecycleItem{
		RestorePath: fs.removeNamespace(ctx, eosDeletedItem.RestorePath),
		RestoreKey:  eosDeletedItem.RestoreKey,
		Size:        eosDeletedItem.Size,
		DelMtime:    eosDeletedItem.DeletionMTime,
		IsDir:       eosDeletedItem.IsDir,
	}
	return recycleItem
}

func (fs *eosStorage) convertToRevision(ctx context.Context, eosFileInfo *eosclient.FileInfo) *storage.Revision {
	md := fs.convertToMD(ctx, eosFileInfo)
	revision := &storage.Revision{
		RevKey: path.Base(md.Path),
		Size:   md.Size,
		Mtime:  md.Mtime,
		IsDir:  md.IsDir,
	}
	return revision
}
func (fs *eosStorage) convertToMD(ctx context.Context, eosFileInfo *eosclient.FileInfo) *storage.MD {
	finfo := new(storage.MD)
	finfo.ID = fmt.Sprintf("%d", eosFileInfo.Inode)
	finfo.Path = fs.removeNamespace(ctx, eosFileInfo.File)
	finfo.Mtime = eosFileInfo.MTime
	finfo.IsDir = eosFileInfo.IsDir
	finfo.Etag = eosFileInfo.ETag
	if finfo.IsDir {
		finfo.TreeCount = eosFileInfo.TreeCount
		finfo.Size = eosFileInfo.TreeSize
	} else {
		finfo.Size = eosFileInfo.Size
	}
	finfo.EosFile = eosFileInfo.File
	finfo.EosInstance = eosFileInfo.Instance
	finfo.Mime = mime.Detect(finfo.IsDir, finfo.Path)
	finfo.IsShareable = true
	return finfo
}
