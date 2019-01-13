package main

import (
	"context"
	"fmt"
	"io"

	rpcpb "github.com/cernbox/go-cs3apis/cs3/rpc"
	storageproviderv0alphapb "github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
)

func lsCommand() *command {
	cmd := newCommand("ls")
	longFlag := cmd.Bool("l", false, "long listing")
	cmd.Action = func() error {
		fn := "/"
		if cmd.NArg() >= 1 {
			fn = cmd.Args()[0]
		}

		client, err := getStorageProviderClient()
		if err != nil {
			return err
		}

		req := &storageproviderv0alphapb.ListRequest{
			Filename: fn,
		}

		ctx := context.Background()
		stream, err := client.List(ctx, req)
		if err != nil {
			return err
		}

		mds := []*storageproviderv0alphapb.Metadata{}
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if res.Status.Code != rpcpb.Code_CODE_OK {
				return formatError(res.Status)
			}
			mds = append(mds, res.Metadata)
		}

		for _, md := range mds {
			if *longFlag {
				fmt.Printf("%+v %d %d %s\n", md.Permissions, md.Mtime, md.Size, md.Filename)
			} else {
				fmt.Println(md.Filename)
			}
		}
		return nil
	}
	return cmd
}