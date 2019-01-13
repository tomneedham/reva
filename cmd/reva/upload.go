package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"

	rpcpb "github.com/cernbox/go-cs3apis/cs3/rpc"
	storageproviderv0alphapb "github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
)

func uploadCommand() *command {
	cmd := newCommand("upload")
	cmd.Action = func() error {
		fn := "/"
		if cmd.NArg() < 2 {
			fmt.Println(cmd.Usage())
			os.Exit(1)
		}

		fn = cmd.Args()[0]
		target := cmd.Args()[1]

		fmt.Printf("Uploading %s to %s\n", fn, target)

		fd, err := os.Open(fn)
		if err != nil {
			return err
		}
		defer fd.Close()

		client, err := getStorageProviderClient()
		if err != nil {
			return err
		}

		req1 := &storageproviderv0alphapb.StartWriteSessionRequest{}
		ctx := context.Background()
		res1, err := client.StartWriteSession(ctx, req1)
		if err != nil {
			return err
		}

		if res1.Status.Code != rpcpb.Code_CODE_OK {
			return formatError(res1.Status)
		}

		sessID := res1.SessionId
		fmt.Println("Write session ID: ", sessID)

		ctx = context.Background()
		stream, err := client.Write(ctx)
		if err != nil {
			return err
		}

		xs := md5.New()
		nchunks, offset := 0, 0
		// TODO(labkode): change buffer size in configuration
		bufferSize := 1024 * 1024 * 3
		buffer := make([]byte, bufferSize)
		for {
			n, err := fd.Read(buffer)
			if n > 0 {
				xs.Write(buffer[:n])
				req := &storageproviderv0alphapb.WriteRequest{
					Data:      buffer[:n],
					Length:    uint64(n),
					Offset:    uint64(offset),
					SessionId: sessID,
				}
				if err := stream.Send(req); err != nil {
					return err
				}
				nchunks++
				offset += n
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}

		res2, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}

		if res2.Status.Code != rpcpb.Code_CODE_OK {
			return formatError(res2.Status)
		}

		wb := res2.WrittenBytes

		fmt.Println("Written bytes: ", wb, " NumChunks: ", nchunks, " MD5: ", fmt.Sprintf("%x", xs.Sum(nil)))

		req3 := &storageproviderv0alphapb.FinishWriteSessionRequest{
			Filename:  target,
			SessionId: sessID,
			Checksum:  fmt.Sprintf("md5:%x", xs.Sum(nil)),
		}
		ctx = context.Background()
		res3, err := client.FinishWriteSession(ctx, req3)
		if err != nil {
			return err
		}

		if res3.Status.Code != rpcpb.Code_CODE_OK {
			return formatError(res3.Status)
		}

		fmt.Println("Upload succeed")
		return nil
	}
	return cmd
}
