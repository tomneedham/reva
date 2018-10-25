package storage_ocm

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/cernbox/reva/api"
	"github.com/studio-b12/gowebdav"
	"go.uber.org/zap"
)

type Options struct {
	Logger *zap.Logger
}

func (opt *Options) init() {
}

func New(opt *Options) (api.Storage, error) {
	opt.init()
	s := new(localStorage)
	s.logger = opt.Logger
	return s, nil
}

type localStorage struct {
	logger *zap.Logger
}

func (fs *localStorage) convertToFileInfoWithNamespace(osFileInfo os.FileInfo, np string) *api.Metadata {
	fi := &api.Metadata{}
	fi.IsDir = osFileInfo.IsDir()
	fi.Path = path.Join("/", np)
	fi.Size = uint64(osFileInfo.Size())
	fi.Id = fi.Path
	fi.Etag = fmt.Sprintf("%d", osFileInfo.ModTime().Unix())
	return fi
}

func (fs *localStorage) GetPathByID(ctx context.Context, id string) (string, error) {
	return "", api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) SetACL(ctx context.Context, path string, readOnly bool, recipient *api.ShareRecipient, shareList []*api.FolderShare) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) UnsetACL(ctx context.Context, path string, recipient *api.ShareRecipient, shareList []*api.FolderShare) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}
func (fs *localStorage) UpdateACL(ctx context.Context, path string, readOnly bool, recipient *api.ShareRecipient, shareList []*api.FolderShare) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) GetQuota(ctx context.Context, name string) (int, int, error) {
	// TODO(labkode): add quota check
	return 0, 0, nil
}

func (fs *localStorage) CreateDir(ctx context.Context, name string) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) Delete(ctx context.Context, name string) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) Move(ctx context.Context, oldName, newName string) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) GetMetadata(ctx context.Context, name string) (*api.Metadata, error) {

	ocmPath := fs.getOCMPath(name)
	dav := gowebdav.NewClient("https://"+ocmPath.BaseURL, ocmPath.Token, ocmPath.Token)

	osFileInfo, err := dav.Stat(ocmPath.FileTarget)
	if err != nil {
		if os.IsNotExist(err) {
			fs.logger.Error("IS NOT EXIST", zap.String("NAME", name))
			return nil, api.NewError(api.StorageNotFoundErrorCode).WithMessage(err.Error())
		}
		fs.logger.Error("CANNOT STAT", zap.String("NAME", name))
		return nil, err
	}
	return fs.convertToFileInfoWithNamespace(osFileInfo, name), nil
}

func (fs *localStorage) ListFolder(ctx context.Context, name string) ([]*api.Metadata, error) {

	ocmPath := fs.getOCMPath(name)
	dav := gowebdav.NewClient("https://"+ocmPath.BaseURL, ocmPath.Token, ocmPath.Token)

	osFileInfos, err := dav.ReadDir(ocmPath.FileTarget)
	if err != nil {
		if os.IsNotExist(err) {
			fs.logger.Error("IS NOT EXIST", zap.String("NAME", name))
			return nil, api.NewError(api.StorageNotFoundErrorCode).WithMessage(err.Error())
		}
		fs.logger.Error("CANNOT READ DIR", zap.String("NAME", name))
		return nil, err
	}
	finfos := []*api.Metadata{}
	for _, osFileInfo := range osFileInfos {
		finfos = append(finfos, fs.convertToFileInfoWithNamespace(osFileInfo, path.Join(ocmPath.FileTarget, osFileInfo.Name())))
	}
	return finfos, nil
}

func (fs *localStorage) Upload(ctx context.Context, name string, r io.ReadCloser) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) Download(ctx context.Context, name string) (io.ReadCloser, error) {

	ocmPath := fs.getOCMPath(name)
	dav := gowebdav.NewClient("https://"+ocmPath.BaseURL, ocmPath.Token, ocmPath.Token)

	r, err := dav.ReadStream(ocmPath.FileTarget)
	if err != nil {
		if os.IsNotExist(err) {
			fs.logger.Error("IS NOT EXIST", zap.String("NAME", name))
			return nil, api.NewError(api.StorageNotFoundErrorCode)
		}
		fs.logger.Error("ERROR DOWNLOADING", zap.String("NAME", name))
	}
	return r, nil
}

func (fs *localStorage) ListRevisions(ctx context.Context, path string) ([]*api.Revision, error) {
	return nil, api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) DownloadRevision(ctx context.Context, path, revisionKey string) (io.ReadCloser, error) {
	return nil, api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) RestoreRevision(ctx context.Context, path, revisionKey string) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) EmptyRecycle(ctx context.Context, path string) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) ListRecycle(ctx context.Context, path string) ([]*api.RecycleEntry, error) {
	return nil, api.NewError(api.StorageNotSupportedErrorCode)
}

func (fs *localStorage) RestoreRecycleEntry(ctx context.Context, restoreKey string) error {
	return api.NewError(api.StorageNotSupportedErrorCode)
}

type ocmPath struct {
	BaseURL    string
	Token      string
	FileTarget string
}

func (fs *localStorage) getOCMPath(originalPath string) *ocmPath {

	values := strings.Split(originalPath, ";")

	return &ocmPath{
		BaseURL:    values[0],
		Token:      values[1],
		FileTarget: values[2],
	}
}
