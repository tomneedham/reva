package storageservice

import (
	//"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cernbox/cernboxapis/gen/proto/go/cernbox/rpc"
	"github.com/cernbox/cernboxapis/gen/proto/go/cernbox/storage/v1"
	"github.com/cernbox/reva/pkg/logger"
	"github.com/cernbox/reva/pkg/storage"

	"github.com/gofrs/uuid"
	//"github.com/grpc-ecosystem/go-grpc-middleware/tags/zap"
	//"go.uber.org/zap"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type service struct {
	storage   storage.Storage
	tmpFolder string
	logger    *logger.Logger
}

// New implements the StorageService of the CERNBox API.
//
//service StorageService {
//  rpc CreateDirectory(CreateDirectoryRequest) returns (CreateDirectoryResponse);
//  rpc Delete(DeleteRequest) returns (DeleteResponse);
//  rpc Move(MoveRequest) returns (MoveResponse);
//  rpc Stat(StatRequest) returns (StatResponse);
//  rpc List(ListRequest) returns (streams ListResponse);
//  rpc StartWriteSession(StartWriteSessionRequest) returns (StartWriteSessionResponse);
//  rpc Write(WriteRequest) returns (WriteResponse);
//  rpc FinishWriteSession(FinishWriteSessionRequest) returns (FinishWriteSessionResponse);
//  rpc Read(ReadRequest) returns (stream ReadResponse);
//  rpc ListVersions(ListVersionsRequest) returns (stream ListVersionsResponse);
//  rpc ReadVersion(ReadVersionRequest) returns (stream ReadVersionResponse);
//  rpc RestoreVersion(RestoreVersionRequest) returns (RestoreVersionResponse);
//  rpc ListRecycle(ListRecycleRequest) returns (stream ListRecycleResponse);
//  rpc RestoreRecycleItem(RestoreRecycleItemRequest) returns (RestoreRecycleItemResponse);
//  rpc PurgeRecycle(PurgeRecycleRequest) returns (PurgeRecycleResponse);
//  rpc SetACL(SetACLRequest) returns (SetACLResponse);
//  rpc UpdateACL(UpdateACLRequest) returns (UpdateACLResponse);
//  rpc UnsetACL(UnsetACLRequest) returns (UnsetACLResponse);
//  rpc GetQuota(GetQuotaRequest) returns (GetQuotaResponse);
//}
func New(s storage.Storage, tmpFolder string, logOut io.Writer, logKey interface{}) interface{} {
	logger := logger.New(logOut, "storageservice", logKey)

	// use os temporary folder if empty
	if tmpFolder == "" {
		tmpFolder = os.TempDir()
	}

	service := &service{
		storage:   s,
		tmpFolder: tmpFolder,
		logger:    logger,
	}

	return service
}

func (s *service) CreateDirectory(ctx context.Context, req *storagev1pb.CreateDirectoryRequest) (*storagev1pb.CreateDirectoryResponse, error) {
	s.logger.Logf(ctx, "CreateDirectory: %+v", req)
	filename := req.GetFilename()
	if err := s.storage.CreateDir(ctx, filename); err != nil {
		err := errors.Wrap(err, "storageservice: error creating folder")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storagev1pb.CreateDirectoryResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storagev1pb.CreateDirectoryResponse{Status: status}
	return res, nil
}

func (s *service) Delete(ctx context.Context, req *storagev1pb.DeleteRequest) (*storagev1pb.DeleteResponse, error) {
	filename := req.GetFilename()

	if err := s.storage.Delete(ctx, filename); err != nil {
		err := errors.Wrap(err, "storageservice: error deleting file")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storagev1pb.DeleteResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storagev1pb.DeleteResponse{Status: status}
	return res, nil
}

func (s *service) Move(ctx context.Context, req *storagev1pb.MoveRequest) (*storagev1pb.MoveResponse, error) {
	source := req.GetSourceFilename()
	target := req.GetTargetFilename()

	if err := s.storage.Move(ctx, source, target); err != nil {
		err := errors.Wrap(err, "storageservice: error moving file")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storagev1pb.MoveResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storagev1pb.MoveResponse{Status: status}
	return res, nil
}

func (s *service) Stat(ctx context.Context, req *storagev1pb.StatRequest) (*storagev1pb.StatResponse, error) {
	filename := req.GetFilename()

	md, err := s.storage.GetMD(ctx, filename)
	if err != nil {
		err := errors.Wrap(err, "storageservice: error stating file")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storagev1pb.StatResponse{Status: status}
		return res, nil
	}

	meta := toMeta(md)
	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storagev1pb.StatResponse{Status: status, Metadata: meta}
	return res, nil
}

func toPerm(p *storage.Permissions) *storagev1pb.Permissions {
	return &storagev1pb.Permissions{
		Read:  p.Read,
		Write: p.Write,
		Share: p.Share,
	}
}

func toMeta(md *storage.MD) *storagev1pb.Metadata {
	perm := toPerm(md.Permissions)
	meta := &storagev1pb.Metadata{
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

func (s *service) List(req *storagev1pb.ListRequest, stream storagev1pb.StorageService_ListServer) error {
	ctx := stream.Context()
	filename := req.GetFilename()

	mds, err := s.storage.ListFolder(ctx, filename)
	if err != nil {
		err := errors.Wrap(err, "storageservice: error listing folder")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storagev1pb.ListResponse{Status: status}
		if err = stream.Send(res); err != nil {
			return errors.Wrap(err, "storageservice: error streaming list response")
		}
	}

	for _, md := range mds {
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		meta := toMeta(md)
		res := &storagev1pb.ListResponse{
			Status:   status,
			Metadata: meta,
		}

		if err := stream.Send(res); err != nil {
			return errors.Wrap(err, "storageservice: error streaming list response")
		}
	}

	return nil
}

func (s *service) getSessionFolder(sessionID string) string {
	return filepath.Join(s.tmpFolder, sessionID)
}

func (s *service) StartWriteSession(ctx context.Context, req *storagev1pb.StartWriteSessionRequest) (*storagev1pb.StartWriteSessionResponse, error) {
	sessionID := uuid.Must(uuid.NewV4()).String()

	// create temporary folder with sesion id to store
	// future writes.
	sessionFolder := s.getSessionFolder(sessionID)
	if err := os.Mkdir(sessionFolder, 0755); err != nil {
		err = errors.Wrap(err, "storageservice: error creating session folder")
		s.logger.Error(ctx, err)

		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storagev1pb.StartWriteSessionResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storagev1pb.StartWriteSessionResponse{Status: status, SessionId: sessionID}
	return res, nil
}

func (s *service) Write(stream storagev1pb.StorageService_WriteServer) error {
	ctx := stream.Context()
	numChunks := 0
	var writtenBytes int64 = 0

	for {
		req, err := stream.Recv()

		if err == io.EOF {
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
			res := &storagev1pb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "storageservice: error closing stream for write")
				return err
			}
			return nil
		}

		if err != nil {
			err = errors.Wrap(err, "storageservice: error receiving write request")
			s.logger.Error(ctx, err)

			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storagev1pb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "storageservice: error closing stream for write")
				return err
			}
			return nil
		}

		sessionFolder := s.getSessionFolder(req.SessionId)
		chunkFile := filepath.Join(sessionFolder, fmt.Sprintf("%d-%d", req.Offset, req.Length))

		fd, err := os.OpenFile(chunkFile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
		defer fd.Close()
		if err != nil {
			err = errors.Wrap(err, "storageservice: error creating chunk file")
			s.logger.Error(ctx, err)

			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storagev1pb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "storageservice: error closing stream for write")
				return err
			}
			return nil
		}

		reader := bytes.NewReader(req.Data)
		n, err := io.CopyN(fd, reader, int64(req.Length))
		if err != nil {
			err = errors.Wrap(err, "storageservice: error writing chunk file")
			s.logger.Error(ctx, err)

			status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
			res := &storagev1pb.WriteResponse{Status: status}
			if err = stream.SendAndClose(res); err != nil {
				err = errors.Wrap(err, "storageservice: error closing stream for write")
				return err
			}
			return nil
		}

		numChunks++
		writtenBytes += n
		fd.Close()
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storagev1pb.WriteResponse{Status: status, WrittenBytes: uint64(writtenBytes), NumberChunks: uint64(numChunks)}
	if err := stream.SendAndClose(res); err != nil {
		err = errors.Wrap(err, "storageservice: error closing stream for write")
		return err
	}
	return nil
}

func (s *service) FinishWriteSession(ctx context.Context, req *storagev1pb.FinishWriteSessionRequest) (*storagev1pb.FinishWriteSessionResponse, error) {
	sessionFolder := s.getSessionFolder(req.SessionId)

	fd, err := os.Open(sessionFolder)
	defer fd.Close()
	if os.IsNotExist(err) {
		err = errors.Wrap(err, "storageservice: error opening session folder")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_INTERNAL}
		res := &storagev1pb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	defer os.RemoveAll(sessionFolder) // remove txFolder once assembled file is returned

	// list all the chunk files in the directory
	names, err := fd.Readdirnames(0)
	if err != nil {
		err = errors.Wrap(err, "storageservice: error listing session folder")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storagev1pb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	// sort the chunks so they are in order when they need to be assembled.
	names = s.getSortedChunkSlice(names)

	//l.Debug("chunk sorted names", zap.String("names", fmt.Sprintf("%+v", names)))
	//l.Info("number of chunks", zap.String("nchunks", fmt.Sprintf("%d", len(names))))

	rand := uuid.Must(uuid.NewV4()).String()
	assembledFilename := filepath.Join(sessionFolder, fmt.Sprintf("assembled-%s", rand))
	//l.Info("", zap.String("assembledfilename", assembledFilename))

	assembledFile, err := os.OpenFile(assembledFilename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		err = errors.Wrap(err, "storageservice: error opening assembly file")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storagev1pb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	for _, n := range names {
		//l.Debug("processing chunk", zap.String("name", n), zap.Int("int", i))
		chunkFilename := filepath.Join(sessionFolder, n)
		//l.Info(fmt.Sprintf("processing chunk %d", i), zap.String("chunk", chunkFilename))

		chunkInfo, err := parseChunkFilename(filepath.Base(chunkFilename))
		if err != nil {
			err = errors.Wrap(err, "storageservice: error parsing chunk filename")
			s.logger.Error(ctx, err)
			status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
			res := &storagev1pb.FinishWriteSessionResponse{Status: status}
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
		err = errors.Wrap(err, "storageservice: error opening assembled file")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storagev1pb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	if err := s.storage.Upload(ctx, req.Filename, fd); err != nil {
		err = errors.Wrap(err, "storageservice: error  uploading assembled file")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
		res := &storagev1pb.FinishWriteSessionResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &storagev1pb.FinishWriteSessionResponse{Status: status}
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
		return nil, fmt.Errorf("chunk filename is wrong: %s", fn)
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

func (s *service) Read(ctx context.Context, req *storagev1pb.ReadRequest, stream storagev1pb.StorageService_ReadServer) error {
}

/*
func (s *service) RestoreRevision(ctx context.Context, req *storagev1pb.RevisionReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.RestoreRevision(ctx, req.Path, req.RevKey); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) UpdateACL(ctx context.Context, req *storagev1pb.ACLReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	err := s.s.UpdateACL(ctx, req.Path, req.ReadOnly, req.Recipient, req.Shares)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) SetACL(ctx context.Context, req *storagev1pb.ACLReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	err := s.s.SetACL(ctx, req.Path, req.ReadOnly, req.Recipient, req.Shares)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) UnsetACL(ctx context.Context, req *storagev1pb.ACLReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	err := s.s.UnsetACL(ctx, req.Path, req.Recipient, req.Shares)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) RestoreRecycleEntry(ctx context.Context, req *storagev1pb.RecycleEntryReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.RestoreRecycleEntry(ctx, req.RestoreKey); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) ReadRevision(req *storagev1pb.RevisionReq, stream storagev1pb.Storage_ReadRevisionServer) error {
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
		dc := &storagev1pb.DataChunk{Data: buffer, Length: uint64(n)}
		dcRes := &storagev1pb.DataChunkResponse{DataChunk: dc}
		if err := stream.Send(dcRes); err != nil {
			l.Error("", zap.Error(err))
			return nil
		}
	}
	return nil
}

func (s *service) ReadFile(req *storagev1pb.PathReq, stream storagev1pb.Storage_ReadFileServer) error {
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
			dc := &storagev1pb.DataChunk{Data: buffer[:n], Length: uint64(n)}
			dcRes := &storagev1pb.DataChunkResponse{DataChunk: dc}
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

func (s *service) ListRevisions(req *storagev1pb.PathReq, stream storagev1pb.Storage_ListRevisionsServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	res, err := s.s.ListRevisions(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		return err
	}
	for _, rev := range res {
		revRes := &storagev1pb.RevisionResponse{Revision: rev}
		if err := stream.Send(revRes); err != nil {
			l.Error("", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *service) ListRecycle(req *storagev1pb.PathReq, stream storagev1pb.Storage_ListRecycleServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	entries, err := s.s.ListRecycle(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		return err
	}
	for _, e := range entries {
		recycleEntryRes := &storagev1pb.RecycleEntryResponse{RecycleEntry: e}
		if err := stream.Send(recycleEntryRes); err != nil {
			l.Error("", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *service) ListFolder(req *storagev1pb.PathReq, stream storagev1pb.Storage_ListFolderServer) error {
	ctx := stream.Context()
	l := ctx_zap.Extract(ctx)
	mds, err := s.s.ListFolder(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		status := storagev1pb.GetStatus(err)
		mdRes := &storagev1pb.MetadataResponse{Status: status}
		if err := stream.Send(mdRes); err != nil {
			return err
		}
		return nil
	}
	for _, md := range mds {
		mdRes := &storagev1pb.MetadataResponse{Metadata: md}
		if err := stream.Send(mdRes); err != nil {
			l.Error("", zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *service) GetQuota(ctx context.Context, req *storagev1pb.QuotaReq) (*storagev1pb.QuotaResponse, error) {
	l := ctx_zap.Extract(ctx)
	total, used, err := s.s.GetQuota(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		status := storagev1pb.GetStatus(err)
		quotaRes := &storagev1pb.QuotaResponse{Status: status}
		return quotaRes, nil
	}
	return &storagev1pb.QuotaResponse{TotalBytes: int64(total), UsedBytes: int64(used)}, nil

}

func (s *service) CreateDir(ctx context.Context, req *storagev1pb.PathReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.CreateDir(ctx, req.Path); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) Delete(ctx context.Context, req *storagev1pb.PathReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.Delete(ctx, req.Path); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) Inspect(ctx context.Context, req *storagev1pb.PathReq) (*storagev1pb.MetadataResponse, error) {
	l := ctx_zap.Extract(ctx)
	md, err := s.s.GetMetadata(ctx, req.Path)
	if err != nil {
		l.Error("", zap.Error(err))
		status := storagev1pb.GetStatus(err)
		mdRes := &storagev1pb.MetadataResponse{Status: status}
		return mdRes, nil
	}
	mdRes := &storagev1pb.MetadataResponse{Metadata: md}
	return mdRes, nil
}

func (s *service) EmptyRecycle(ctx context.Context, req *storagev1pb.PathReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.EmptyRecycle(ctx, req.Path); err != nil {
		l.Error("", zap.Error(err))
		status := storagev1pb.GetStatus(err)
		return &storagev1pb.EmptyResponse{Status: status}, nil
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) WriteChunk(stream storagev1pb.Storage_WriteChunkServer) error {
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

	writeSummary := &storagev1pb.WriteSummary{Nchunks: numChunks, TotalSize: totalSize}
	writeSummaryRes := &storagev1pb.WriteSummaryResponse{WriteSummary: writeSummary}
	return stream.SendAndClose(writeSummaryRes)
}

func (s *service) StartWriteTx(ctx context.Context, req *storagev1pb.EmptyReq) (*storagev1pb.TxInfoResponse, error) {
	l := ctx_zap.Extract(ctx)
	// create a temporary folder with the TX ID
	uuid := uuid.Must(uuid.NewV4())
	txID := uuid.String()
	txFolder := s.getTxFolder(txID)
	if err := os.Mkdir(txFolder, 0755); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	txInfo := &storagev1pb.TxInfo{TxId: txID}
	txInfoRes := &storagev1pb.TxInfoResponse{TxInfo: txInfo}
	return txInfoRes, nil
}

type chunkInfo struct {
	Offset       uint64
	ClientLength uint64
}

func parseChunkFilename(fn string) (*chunkInfo, error) {
	parts := strings.Split(fn, "-")
	if len(parts) < 2 {
		return nil, fmt.Errorf("chunk filename is wrong: %s", fn)
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

func (s *service) FinishWriteTx(ctx context.Context, req *storagev1pb.TxEnd) (*storagev1pb.EmptyResponse, error) {
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
		return &storagev1pb.EmptyResponse{}, err
	}

	names = s.getSortedChunkSlice(names)

	l.Debug("chunk sorted names", zap.String("names", fmt.Sprintf("%+v", names)))
	l.Info("number of chunks", zap.String("nchunks", fmt.Sprintf("%d", len(names))))

	uuid := uuid.Must(uuid.NewV4())
	rand := uuid.String()
	assembledFilename := filepath.Join(txFolder, fmt.Sprintf("assembled-%s", rand))
	l.Info("", zap.String("assembledfilename", assembledFilename))

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
			return &storagev1pb.EmptyResponse{}, err
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

	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) Move(ctx context.Context, req *storagev1pb.MoveReq) (*storagev1pb.EmptyResponse, error) {
	l := ctx_zap.Extract(ctx)
	if err := s.s.Move(ctx, req.OldPath, req.NewPath); err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	return &storagev1pb.EmptyResponse{}, nil
}

func (s *service) getTxFolder(txID string) string {
	return filepath.Join(s.tmpFolder, txID)
}
*/
