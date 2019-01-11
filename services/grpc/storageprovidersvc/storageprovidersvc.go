package storageprovidersvc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cernbox/reva/pkg/err"
	"github.com/cernbox/reva/pkg/log"
	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/storage/local"

	"github.com/cernbox/go-cs3apis/cs3/rpc"
	"github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"

	"github.com/gofrs/uuid"
	"github.com/mitchellh/mapstructure"
	"golang.org/x/net/context"
)

var logger = log.New("storageprovidersvc")
var errors = err.New("storageprovidersvc")

type config struct {
	Driver    string                 `mapstructure:"driver"`
	TmpFolder string                 `mapstructure:"tmp_folder"`
	EOS       map[string]interface{} `mapstructure:"eos"`
	S3        map[string]interface{} `mapstructure:"s3"`
	Local     map[string]interface{} `mapstructure:"local"`
}

type service struct {
	storage   storage.FS
	tmpFolder string
}

func parseConfig(m map[string]interface{}) (*config, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		return nil, err
	}
	return c, nil
}

func getFS(c *config) (storage.FS, error) {
	switch c.Driver {
	case "local":
		return local.New(c.Local)
	case "":
		return nil, fmt.Errorf("driver is empty")
	default:
		return nil, fmt.Errorf("driver not found: %s", c.Driver)
	}
}

func New(m map[string]interface{}) (storageproviderv0alphapb.StorageProviderServiceServer, error) {

	c, err := parseConfig(m)
	if err != nil {
		return nil, errors.Wrap(err, "storageprovidersvc: unable to parse config")
	}

	// use os temporary folder if empty
	tmpFolder := c.TmpFolder
	if tmpFolder == "" {
		tmpFolder = os.TempDir()
	}

	fs, err := getFS(c)
	if err != nil {
		return nil, errors.Wrap(err, "storageprovidersvc: unable to obtain a filesystem")
	}

	service := &service{
		storage:   fs,
		tmpFolder: tmpFolder,
	}

	return service, nil
}

func (s *service) CreateDirectory(ctx context.Context, req *storageproviderv0alphapb.CreateDirectoryRequest) (*storageproviderv0alphapb.CreateDirectoryResponse, error) {
	fn := req.GetFilename()
	if err := s.storage.CreateDir(ctx, fn); err != nil {
		err := errors.Wrap(err, "storageprovidersvc: error creating folder "+fn)
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.CreateDirectoryResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.CreateDirectoryResponse{Status: status}
	return res, nil
}

func (s *service) Delete(ctx context.Context, req *storageproviderv0alphapb.DeleteRequest) (*storageproviderv0alphapb.DeleteResponse, error) {
	fn := req.GetFilename()

	if err := s.storage.Delete(ctx, fn); err != nil {
		err := errors.Wrap(err, "storageprovidersvc: error deleting file")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.DeleteResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.DeleteResponse{Status: status}
	return res, nil
}

func (s *service) Move(ctx context.Context, req *storageproviderv0alphapb.MoveRequest) (*storageproviderv0alphapb.MoveResponse, error) {
	source := req.GetSourceFilename()
	target := req.GetTargetFilename()

	if err := s.storage.Move(ctx, source, target); err != nil {
		err := errors.Wrap(err, "storageprovidersvc: error moving file")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.MoveResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.MoveResponse{Status: status}
	return res, nil
}

func (s *service) Stat(ctx context.Context, req *storageproviderv0alphapb.StatRequest) (*storageproviderv0alphapb.StatResponse, error) {
	fn := req.GetFilename()

	md, err := s.storage.GetMD(ctx, fn)
	if err != nil {
		err := errors.Wrap(err, "storageprovidersvc: error stating file")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.StatResponse{Status: status}
		return res, nil
	}

	meta := toMeta(md)
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.StatResponse{Status: status, Metadata: meta}
	return res, nil
}

func toPerm(p *storage.Permissions) *storageproviderv0alphapb.Permissions {
	return &storageproviderv0alphapb.Permissions{
		Read:  p.Read,
		Write: p.Write,
		Share: p.Share,
	}
}

func toMeta(md *storage.MD) *storageproviderv0alphapb.Metadata {
	perm := toPerm(md.Permissions)
	meta := &storageproviderv0alphapb.Metadata{
		Filename:    md.Path,
		Checksum:    md.Checksum,
		Etag:        md.Etag,
		Id:          md.ID,
		IsDir:       md.IsDir,
		Mime:        md.Mime,
		Mtime:       md.Mtime,
		Size:        md.Size,
		Permissions: perm,
	}

	return meta
}

func (s *service) List(req *storageproviderv0alphapb.ListRequest, stream storageproviderv0alphapb.StorageProviderService_ListServer) error {
	ctx := stream.Context()
	fn := req.GetFilename()

	mds, err := s.storage.ListFolder(ctx, fn)
	if err != nil {
		err := errors.Wrap(err, "storageprovidersvc: error listing folder")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ListResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list response")
		}
	}

	for _, md := range mds {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		meta := toMeta(md)
		res := &storageproviderv0alphapb.ListResponse{
			Status:   status,
			Metadata: meta,
		}

		if err := stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list response")
		}
	}

	return nil
}

func (s *service) getSessionFolder(sessionID string) string {
	return filepath.Join(s.tmpFolder, sessionID)
}

func (s *service) StartWriteSession(ctx context.Context, req *storageproviderv0alphapb.StartWriteSessionRequest) (*storageproviderv0alphapb.StartWriteSessionResponse, error) {
	sessionID := uuid.Must(uuid.NewV4()).String()

	// create temporary folder with sesion id to store
	// future writes.
	sessionFolder := s.getSessionFolder(sessionID)
	if err := os.Mkdir(sessionFolder, 0755); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error creating session folder")
		logger.Error(ctx, err)

		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.StartWriteSessionResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.StartWriteSessionResponse{Status: status, SessionId: sessionID}
	return res, nil
}

func (s *service) Write(stream storageproviderv0alphapb.StorageProviderService_WriteServer) error {
	ctx := stream.Context()
	numChunks := 0
	var writtenBytes int64 = 0

	for {
		req, err := stream.Recv()

		if err == io.EOF {
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
			res := &storageproviderv0alphapb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "storageprovidersvc: error closing stream for write")
				return err
			}
			return nil
		}

		if err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error receiving write request")
			logger.Error(ctx, err)

			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "storageprovidersvc: error closing stream for write")
				return err
			}
			return nil
		}

		sessionFolder := s.getSessionFolder(req.SessionId)
		chunkFile := filepath.Join(sessionFolder, fmt.Sprintf("%d-%d", req.Offset, req.Length))

		fd, err := os.OpenFile(chunkFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
		defer fd.Close()
		if err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error creating chunk file")
			logger.Error(ctx, err)

			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "storageprovidersvc: error closing stream for write")
				return err
			}
			return nil
		}

		reader := bytes.NewReader(req.Data)
		n, err := io.CopyN(fd, reader, int64(req.Length))
		if err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error writing chunk file")
			logger.Error(ctx, err)

			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "storageprovidersvc: error closing stream for write")
				return err
			}
			return nil
		}

		numChunks++
		writtenBytes += n
		fd.Close()
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.WriteResponse{Status: status, WrittenBytes: uint64(writtenBytes), NumberChunks: uint64(numChunks)}
	if err := stream.SendAndClose(res); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error closing stream for write")
		return err
	}
	return nil
}

func (s *service) FinishWriteSession(ctx context.Context, req *storageproviderv0alphapb.FinishWriteSessionRequest) (*storageproviderv0alphapb.FinishWriteSessionResponse, error) {
	sessionFolder := s.getSessionFolder(req.SessionId)

	fd, err := os.Open(sessionFolder)
	defer fd.Close()
	if os.IsNotExist(err) {
		err = errors.Wrap(err, "storageprovidersvc: error opening session folder")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	defer os.RemoveAll(sessionFolder) // remove txFolder once assembled file is returned

	// list all the chunk files in the directory
	names, err := fd.Readdirnames(0)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error listing session folder")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	// sort the chunks so they are in order when they need to be assembled.
	names = s.getSortedChunkSlice(names)

	//l.Debug("chunk sorted names", zap.String("names", fmt.Sprintf("%+v", names)))
	//l.Info("number of chunks", zap.String("nchunks", fmt.Sprintf("%d", len(names))))

	rand := uuid.Must(uuid.NewV4()).String()
	assembledFilename := filepath.Join(sessionFolder, fmt.Sprintf("assembled-%s", rand))
	//l.Info("", zap.String("assembledfn", assembledFilename))

	assembledFile, err := os.OpenFile(assembledFilename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error opening assembly file")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	for _, n := range names {
		//l.Debug("processing chunk", zap.String("name", n), zap.Int("int", i))
		chunkFilename := filepath.Join(sessionFolder, n)
		//l.Info(fmt.Sprintf("processing chunk %d", i), zap.String("chunk", chunkFilename))

		chunkInfo, err := parseChunkFilename(filepath.Base(chunkFilename))
		if err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error parsing chunk fn")
			logger.Error(ctx, err)
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
			res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
			return res, nil
		}

		chunk, err := os.Open(chunkFilename)
		defer chunk.Close()
		if err != nil {
			return nil, err
		}
		n, err := io.CopyN(assembledFile, chunk, int64(chunkInfo.ClientLength))
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n != int64(chunkInfo.ClientLength) {
			return nil, fmt.Errorf("chunk size in disk is different from chunk size sent from client. Read: %d Sent: %d", n, chunkInfo.ClientLength)
		}
		chunk.Close()
	}
	assembledFile.Close()

	fd, err = os.Open(assembledFilename)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error opening assembled file")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	if err := s.storage.Upload(ctx, req.Filename, fd); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error  uploading assembled file")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.FinishWriteSessionResponse{Status: status}
	return res, nil
}

func (s *service) getSortedChunkSlice(names []string) []string {
	// sort names numerically by chunk
	sort.Slice(names, func(i, j int) bool {
		previous := names[i]
		next := names[j]

		previousOffset, err := strconv.ParseInt(strings.Split(previous, "-")[0], 10, 64)
		if err != nil {
			panic("chunk name cannot be casted to int: " + previous)
		}
		nextOffset, err := strconv.ParseInt(strings.Split(next, "-")[0], 10, 64)
		if err != nil {
			panic("chunk name cannot be casted to int: " + next)
		}
		return previousOffset < nextOffset
	})
	return names
}

type chunkInfo struct {
	Offset       uint64
	ClientLength uint64
}

func parseChunkFilename(fn string) (*chunkInfo, error) {
	parts := strings.Split(fn, "-")
	if len(parts) < 2 {
		return nil, fmt.Errorf("chunk fn is wrong: %s", fn)
	}

	offset, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	clientLength, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	return &chunkInfo{Offset: offset, ClientLength: clientLength}, nil
}

func (s *service) Read(req *storageproviderv0alphapb.ReadRequest, stream storageproviderv0alphapb.StorageProviderService_ReadServer) error {
	ctx := stream.Context()
	fd, err := s.storage.Download(ctx, req.Filename)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error downloading file")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ReadResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming read response")
		}
		return nil
	}

	// close fd when finish reading
	// continue on failure
	defer func() {
		if err := fd.Close(); err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error closing fd after reading - leak")
			logger.Error(ctx, err)
		}
	}()

	// send data chunks of maximum 3 MiB
	buffer := make([]byte, 1024*1024*3)
	for {
		n, err := fd.Read(buffer)
		if n > 0 {
			dc := &storageproviderv0alphapb.DataChunk{Data: buffer[:n], Length: uint64(n)}
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
			res := &storageproviderv0alphapb.ReadResponse{Status: status, DataChunk: dc}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "storageprovidersvc: error streaming read response")
			}
		}

		// nothing more to send
		if err == io.EOF {
			break
		}

		if err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error reading from fd")
			logger.Error(ctx, err)
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.ReadResponse{Status: status}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "storageprovidersvc: error streaming read response")
			}
			return nil
		}
	}

	return nil
}

func (s *service) ListVersions(req *storageproviderv0alphapb.ListVersionsRequest, stream storageproviderv0alphapb.StorageProviderService_ListVersionsServer) error {
	ctx := stream.Context()
	revs, err := s.storage.ListRevisions(ctx, req.Filename)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error listing revisions")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ListVersionsResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list versions response")
		}
		return nil
	}

	for _, rev := range revs {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		version := &storageproviderv0alphapb.Version{
			Key:   rev.RevKey,
			IsDir: rev.IsDir,
			Mtime: rev.Mtime,
			Size:  rev.Size,
		}
		res := &storageproviderv0alphapb.ListVersionsResponse{Status: status, Version: version}
		if err := stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list versions response")
		}
	}
	return nil
}

func (s *service) ReadVersion(req *storageproviderv0alphapb.ReadVersionRequest, stream storageproviderv0alphapb.StorageProviderService_ReadVersionServer) error {
	ctx := stream.Context()
	fd, err := s.storage.DownloadRevision(ctx, req.Filename, req.VersionKey)
	defer func() {
		if err := fd.Close(); err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error closing fd for version file - leak")
			logger.Error(ctx, err)
			// continue
		}
	}()

	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error downloading revision")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ReadVersionResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming read version response")
		}
		return nil
	}

	// send data chunks of maximum 1 MiB
	buffer := make([]byte, 1024*1024*3)
	for {
		n, err := fd.Read(buffer)
		if n > 0 {
			dc := &storageproviderv0alphapb.DataChunk{Data: buffer[:n], Length: uint64(n)}
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
			res := &storageproviderv0alphapb.ReadVersionResponse{Status: status, DataChunk: dc}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "storageprovidersvc: error streaming read version response")
			}
		}

		// nothing more to send
		if err == io.EOF {
			break
		}

		if err != nil {
			err = errors.Wrap(err, "storageprovidersvc: error reading from fd")
			logger.Error(ctx, err)
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storageproviderv0alphapb.ReadVersionResponse{Status: status}
			if err = stream.Send(res); err != nil {
				return errors.Wrap(err, "storageprovidersvc: error streaming read response")
			}
			return nil
		}
	}

	return nil

}

func (s *service) RestoreVersion(ctx context.Context, req *storageproviderv0alphapb.RestoreVersionRequest) (*storageproviderv0alphapb.RestoreVersionResponse, error) {
	if err := s.storage.RestoreRevision(ctx, req.Filename, req.VersionKey); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error restoring version")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.RestoreVersionResponse{Status: status}
		return res, nil
	}
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
	res := &storageproviderv0alphapb.RestoreVersionResponse{Status: status}
	return res, nil
}

func (s *service) ListRecycle(req *storageproviderv0alphapb.ListRecycleRequest, stream storageproviderv0alphapb.StorageProviderService_ListRecycleServer) error {
	ctx := stream.Context()
	fn := req.GetFilename()

	items, err := s.storage.ListRecycle(ctx, fn)
	if err != nil {
		err := errors.Wrap(err, "storageprovidersvc: error listing recycle")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.ListRecycleResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list recycle response")
		}
	}

	for _, item := range items {
		recycleItem := &storageproviderv0alphapb.RecycleItem{
			Filename: item.RestorePath,
			Key:      item.RestoreKey,
			Size:     item.Size,
			Deltime:  item.DelMtime,
			IsDir:    item.IsDir,
		}
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storageproviderv0alphapb.ListRecycleResponse{
			Status:      status,
			RecycleItem: recycleItem,
		}

		if err := stream.Send(res); err != nil {
			return errors.Wrap(err, "storageprovidersvc: error streaming list recycle response")
		}
	}

	return nil
}

func (s *service) RestoreRecycleItem(ctx context.Context, req *storageproviderv0alphapb.RestoreRecycleItemRequest) (*storageproviderv0alphapb.RestoreRecycleItemResponse, error) {
	if err := s.storage.RestoreRecycleItem(ctx, req.Filename, req.RestoreKey); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error restoring recycle item")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.RestoreRecycleItemResponse{Status: status}
		return res, nil
	}
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.RestoreRecycleItemResponse{Status: status}
	return res, nil
}

func (s *service) PurgeRecycle(ctx context.Context, req *storageproviderv0alphapb.PurgeRecycleRequest) (*storageproviderv0alphapb.PurgeRecycleResponse, error) {
	if err := s.storage.EmptyRecycle(ctx, req.Filename); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error purging recycle")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.PurgeRecycleResponse{Status: status}
		return res, nil
	}
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.PurgeRecycleResponse{Status: status}
	return res, nil
}

func (s *service) SetACL(ctx context.Context, req *storageproviderv0alphapb.SetACLRequest) (*storageproviderv0alphapb.SetACLResponse, error) {
	fn := req.Filename
	aclTarget := req.Acl.Target
	aclMode := s.getPermissions(req.Acl.Mode)
	aclType := s.getTargetType(req.Acl.Type)

	// check mode is valid
	if aclMode == storage.ACLModeInvalid {
		logger.Println(ctx, "acl mode is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl mode is invalid"}
		res := &storageproviderv0alphapb.SetACLResponse{Status: status}
		return res, nil
	}

	// check targetType is valid
	if aclType == storage.ACLTypeInvalid {
		logger.Println(ctx, "acl  type is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl type is invalid"}
		res := &storageproviderv0alphapb.SetACLResponse{Status: status}
		return res, nil
	}

	acl := &storage.ACL{
		Target: aclTarget,
		Mode:   aclMode,
		Type:   aclType,
	}

	err := s.storage.SetACL(ctx, fn, acl)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error setting acl")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.SetACLResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.SetACLResponse{Status: status}
	return res, nil
}

func (s *service) getTargetType(t storageproviderv0alphapb.ACLType) storage.ACLType {
	switch t {
	case storageproviderv0alphapb.ACLType_ACL_TYPE_USER:
		return storage.ACLTypeUser
	case storageproviderv0alphapb.ACLType_ACL_TYPE_GROUP:
		return storage.ACLTypeGroup
	default:
		return storage.ACLTypeInvalid
	}
}

func (s *service) getPermissions(mode storageproviderv0alphapb.ACLMode) storage.ACLMode {
	switch mode {
	case storageproviderv0alphapb.ACLMode_ACL_MODE_READONLY:
		return storage.ACLModeReadOnly
	case storageproviderv0alphapb.ACLMode_ACL_MODE_READWRITE:
		return storage.ACLModeReadWrite
	default:
		return storage.ACLModeInvalid
	}
}

func (s *service) UpdateACL(ctx context.Context, req *storageproviderv0alphapb.UpdateACLRequest) (*storageproviderv0alphapb.UpdateACLResponse, error) {
	fn := req.Filename
	target := req.Acl.Target
	mode := s.getPermissions(req.Acl.Mode)
	targetType := s.getTargetType(req.Acl.Type)

	// check mode is valid
	if mode == storage.ACLModeInvalid {
		logger.Println(ctx, "acl mode is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl mode is invalid"}
		res := &storageproviderv0alphapb.UpdateACLResponse{Status: status}
		return res, nil
	}

	// check targetType is valid
	if targetType == storage.ACLTypeInvalid {
		logger.Println(ctx, "acl  type is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl type is invalid"}
		res := &storageproviderv0alphapb.UpdateACLResponse{Status: status}
		return res, nil
	}

	acl := &storage.ACL{
		Target: target,
		Mode:   mode,
		Type:   targetType,
	}

	if err := s.storage.UpdateACL(ctx, fn, acl); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error updating acl")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.UpdateACLResponse{Status: status}
		return res, nil
	}
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.UpdateACLResponse{Status: status}
	return res, nil
}

func (s *service) UnsetACL(ctx context.Context, req *storageproviderv0alphapb.UnsetACLRequest) (*storageproviderv0alphapb.UnsetACLResponse, error) {
	fn := req.Filename
	aclTarget := req.Acl.Target
	aclType := s.getTargetType(req.Acl.Type)

	// check targetType is valid
	if aclType == storage.ACLTypeInvalid {
		logger.Println(ctx, "acl  type is invalid")
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INVALID_ARGUMENT, Message: "acl type is invalid"}
		res := &storageproviderv0alphapb.UnsetACLResponse{Status: status}
		return res, nil
	}

	acl := &storage.ACL{
		Target: aclTarget,
		Type:   aclType,
	}

	if err := s.storage.UnsetACL(ctx, fn, acl); err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error unsetting acl")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.UnsetACLResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.UnsetACLResponse{Status: status}
	return res, nil
}

func (s *service) GetQuota(ctx context.Context, req *storageproviderv0alphapb.GetQuotaRequest) (*storageproviderv0alphapb.GetQuotaResponse, error) {
	total, used, err := s.storage.GetQuota(ctx, req.Filename)
	if err != nil {
		err = errors.Wrap(err, "storageprovidersvc: error getting quota")
		logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storageproviderv0alphapb.GetQuotaResponse{Status: status}
		return res, nil
	}
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storageproviderv0alphapb.GetQuotaResponse{Status: status, TotalBytes: uint64(total), UsedBytes: uint64(used)}
	return res, nil
}

/*
func (s *service) RestoreRevision(ctx context.Context, req *storageproviderv0alphapb.RevisionReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.RestoreRevision(ctx, req.Path, req.RevKey); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) UpdateACL(ctx context.Context, req *storageproviderv0alphapb.ACLReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	err := s.s.UpdateACL(ctx, req.Path, req.ReadOnly, req.Recipient, req.Shares)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) SetACL(ctx context.Context, req *storageproviderv0alphapb.ACLReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	err := s.s.SetACL(ctx, req.Path, req.ReadOnly, req.Recipient, req.Shares)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) UnsetACL(ctx context.Context, req *storageproviderv0alphapb.ACLReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	err := s.s.UnsetACL(ctx, req.Path, req.Recipient, req.Shares)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) RestoreRecycleEntry(ctx context.Context, req *storageproviderv0alphapb.RecycleEntryReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.RestoreRecycleEntry(ctx, req.RestoreKey); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) ReadRevision(req *storageproviderv0alphapb.RevisionReq, stream storageproviderv0alphapb.Storage_ReadRevisionServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	readCloser, err := s.s.DownloadRevision(ctx, req.Path, req.RevKey)
	defer func() {
		l.Debug("closing fd when reading version for path: " + req.Path)
		if err := readCloser.Close(); err != nil {
			l.Error("error closing fd", zap.Error(err))
		}
	}()
	if err != nil {
		l.Error("", zap.Error(err))
		return err
	}

	bufferedReader := bufio.NewReaderSize(readCloser, 1024*1024*3)

	// send data chunks of maximum 1 MiB
	buffer := make([]byte, 1024*1024*3)
	for {
		n, err := bufferedReader.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			l.Error("", zap.Error(err))
			return err
		}
		dc := &storageproviderv0alphapb.DataChunk{Data: buffer, Length: uint64(n)}
		dcRes := &storageproviderv0alphapb.DataChunkResponse{DataChunk: dc}
		if err := stream.Send(dcRes); err != nil {
			l.Error("", zap.Error(err))
			return nil
		}
	}
	return nil
}

func (s *service) ReadFile(req *storageproviderv0alphapb.PathReq, stream storageproviderv0alphapb.Storage_ReadFileServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	readCloser, err := s.s.Download(ctx, req.Path)
	if err != nil {
		l.Error("error reading file from fs", zap.Error(err))
		return err
	}
	defer func() {
		l.Debug("closing fd when reading for path: " + req.Path)
		if err := readCloser.Close(); err != nil {
			l.Error("error closing fd", zap.Error(err))
		}
	}()

	// send data chunks of maximum 3 MiB
	buffer := make([]byte, 1024*1024*3)
	for {
		n, err := readCloser.Read(buffer)
		if n > 0 {
			dc := &storageproviderv0alphapb.DataChunk{Data: buffer[:n], Length: uint64(n)}
			dcRes := &storageproviderv0alphapb.DataChunkResponse{DataChunk: dc}
			if err := stream.Send(dcRes); err != nil {
				l.Error("", zap.Error(err))
				return nil
			}

		}
		if err == io.EOF {
			break

		}
		if err != nil {
			l.Error("error when reading from readcloser", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *service) ListRevisions(req *storageproviderv0alphapb.PathReq, stream storageproviderv0alphapb.Storage_ListRevisionsServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	res, err := s.s.ListRevisions(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		return err
	}
	for _, rev := range res {
		revRes := &storageproviderv0alphapb.RevisionResponse{Revision: rev}
		if err := stream.Send(revRes); err != nil {
			l.Error("", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *service) ListRecycle(req *storageproviderv0alphapb.PathReq, stream storageproviderv0alphapb.Storage_ListRecycleServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	entries, err := s.s.ListRecycle(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		return err
	}
	for _, e := range entries {
		recycleEntryRes := &storageproviderv0alphapb.RecycleEntryResponse{RecycleEntry: e}
		if err := stream.Send(recycleEntryRes); err != nil {
			l.Error("", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *service) ListFolder(req *storageproviderv0alphapb.PathReq, stream storageproviderv0alphapb.Storage_ListFolderServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	mds, err := s.s.ListFolder(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		status := storageproviderv0alphapb.GetStatus(err)
		mdRes := &storageproviderv0alphapb.MetadataResponse{Status: status}
		if err := stream.Send(mdRes); err != nil {
			return err
		}
		return nil
	}
	for _, md := range mds {
		mdRes := &storageproviderv0alphapb.MetadataResponse{Metadata: md}
		if err := stream.Send(mdRes); err != nil {
			l.Error("", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *service) GetQuota(ctx context.Context, req *storageproviderv0alphapb.QuotaReq) (*storageproviderv0alphapb.QuotaResponse, error) {
	l := ctx_zap.Extract(ctx)
	total, used, err := s.s.GetQuota(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		status := storageproviderv0alphapb.GetStatus(err)
		quotaRes := &storageproviderv0alphapb.QuotaResponse{Status: status}
		return quotaRes, nil
	}
	return &storageproviderv0alphapb.QuotaResponse{TotalBytes: int64(total), UsedBytes: int64(used)}, nil

}

func (s *service) CreateDir(ctx context.Context, req *storageproviderv0alphapb.PathReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.CreateDir(ctx, req.Path); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) Delete(ctx context.Context, req *storageproviderv0alphapb.PathReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.Delete(ctx, req.Path); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) Inspect(ctx context.Context, req *storageproviderv0alphapb.PathReq) (*storageproviderv0alphapb.MetadataResponse, error) {
	l := ctx_zap.Extract(ctx)
	md, err := s.s.GetMetadata(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		status := storageproviderv0alphapb.GetStatus(err)
		mdRes := &storageproviderv0alphapb.MetadataResponse{Status: status}
		return mdRes, nil
	}
	mdRes := &storageproviderv0alphapb.MetadataResponse{Metadata: md}
	return mdRes, nil
}

func (s *service) EmptyRecycle(ctx context.Context, req *storageproviderv0alphapb.PathReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.EmptyRecycle(ctx, req.Path); err != nil {
		l.Error("", zap.Error(err))
		status := storageproviderv0alphapb.GetStatus(err)
		return &storageproviderv0alphapb.EmptyResponse{Status: status}, nil
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) WriteChunk(stream storageproviderv0alphapb.Storage_WriteChunkServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	numChunks := uint64(0)
	totalSize := uint64(0)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			l.Error("", zap.Error(err))
			return err
		}
		txFolder := s.getTxFolder(req.TxId)
		if _, err := os.Stat(txFolder); err != nil {
			l.Error("", zap.Error(err))
			return err
		}

		chunkFile := filepath.Join(txFolder, fmt.Sprintf("%d-%d", req.Offset, req.Length))
		fd, err := os.OpenFile(chunkFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
		defer fd.Close()
		if err != nil {
			l.Error("", zap.Error(err))
			return err
		}
		reader := bytes.NewReader(req.Data)
		n, err := io.CopyN(fd, reader, int64(req.Length))
		if err != nil {
			l.Error("", zap.Error(err))
			return err
		}
		numChunks++
		totalSize += uint64(n)
		fd.Close()
	}

	writeSummary := &storageproviderv0alphapb.WriteSummary{Nchunks: numChunks, TotalSize: totalSize}
	writeSummaryRes := &storageproviderv0alphapb.WriteSummaryResponse{WriteSummary: writeSummary}
	return stream.SendAndClose(writeSummaryRes)
}

func (s *service) StartWriteTx(ctx context.Context, req *storageproviderv0alphapb.EmptyReq) (*storageproviderv0alphapb.TxInfoResponse, error) {
	l := ctx_zap.Extract(ctx)
	// create a temporary folder with the TX ID
	uuid := uuid.Must(uuid.NewV4())
	txID := uuid.String()
	txFolder := s.getTxFolder(txID)
	if err := os.Mkdir(txFolder, 0755); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	txInfo := &storageproviderv0alphapb.TxInfo{TxId: txID}
	txInfoRes := &storageproviderv0alphapb.TxInfoResponse{TxInfo: txInfo}
	return txInfoRes, nil
}

type chunkInfo struct {
	Offset       uint64
	ClientLength uint64
}

func parseChunkFilename(fn string) (*chunkInfo, error) {
	parts := strings.Split(fn, "-")
	if len(parts) < 2 {
		return nil, fmt.Errorf("chunk fn is wrong: %s", fn)
	}

	offset, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	clientLength, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	return &chunkInfo{Offset: offset, ClientLength: clientLength}, nil
}

func (s *service) getSortedChunkSlice(names []string) []string {
	// sort names numerically by chunk
	sort.Slice(names, func(i, j int) bool {
		previous := names[i]
		next := names[j]

		previousOffset, err := strconv.ParseInt(strings.Split(previous, "-")[0], 10, 64)
		if err != nil {
			panic("chunk name cannot be casted to int: " + previous)
		}
		nextOffset, err := strconv.ParseInt(strings.Split(next, "-")[0], 10, 64)
		if err != nil {
			panic("chunk name cannot be casted to int: " + next)
		}
		return previousOffset < nextOffset
	})
	return names
}

func (s *service) FinishWriteTx(ctx context.Context, req *storageproviderv0alphapb.TxEnd) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	txFolder := s.getTxFolder(req.TxId)
	fd, err := os.Open(txFolder)
	defer fd.Close()
	if os.IsNotExist(err) {
		return nil, err
	}
	defer os.RemoveAll(txFolder) // remove txFolder once assembled file is returned

	// list all the chunks in the directory
	names, err := fd.Readdirnames(0)
	if err != nil {
		return &storageproviderv0alphapb.EmptyResponse{}, err
	}

	names = s.getSortedChunkSlice(names)

	l.Debug("chunk sorted names", zap.String("names", fmt.Sprintf("%+v", names)))
	l.Info("number of chunks", zap.String("nchunks", fmt.Sprintf("%d", len(names))))

	uuid := uuid.Must(uuid.NewV4())
	rand := uuid.String()
	assembledFilename := filepath.Join(txFolder, fmt.Sprintf("assembled-%s", rand))
	l.Info("", zap.String("assembledfn", assembledFilename))

	assembledFile, err := os.OpenFile(assembledFilename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	for i, n := range names {
		l.Debug("processing chunk", zap.String("name", n), zap.Int("int", i))
		chunkFilename := filepath.Join(txFolder, n)
		l.Info(fmt.Sprintf("processing chunk %d", i), zap.String("chunk", chunkFilename))

		chunkInfo, err := parseChunkFilename(filepath.Base(chunkFilename))
		if err != nil {
			return &storageproviderv0alphapb.EmptyResponse{}, err
		}
		chunk, err := os.Open(chunkFilename)
		defer chunk.Close()
		if err != nil {
			return nil, err
		}
		n, err := io.CopyN(assembledFile, chunk, int64(chunkInfo.ClientLength))
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n != int64(chunkInfo.ClientLength) {
			return nil, fmt.Errorf("chunk size in disk is different from chunk size sent from client. Read: %d Sent: %d", n, chunkInfo.ClientLength)
		}
		chunk.Close()
	}
	assembledFile.Close()

	fd, err = os.Open(assembledFilename)
	if err != nil {
		l.Error("")
		return nil, err
	}

	if err := s.s.Upload(ctx, req.Path, fd); err != nil {
		return nil, err
	}

	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) Move(ctx context.Context, req *storageproviderv0alphapb.MoveReq) (*storageproviderv0alphapb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.Move(ctx, req.OldPath, req.NewPath); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storageproviderv0alphapb.EmptyResponse{}, nil
}

func (s *service) getTxFolder(txID string) string {
	return filepath.Join(s.tmpFolder, txID)
}
*/
