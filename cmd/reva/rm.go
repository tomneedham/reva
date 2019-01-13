package main

import (
	"context"

	rpcpb "github.com/cernbox/go-cs3apis/cs3/rpc"
	storageproviderv0alphapb "github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
)

func rmCommand() *command {
	cmd := newCommand("rm")
	cmd.Description = func() string { return "removes a file or folder" }
	cmd.Action = func() error {
		fn := "/"
		if cmd.NArg() >= 1 {
			fn = cmd.Args()[0]
		}
		ctx := context.Background()
		client, err := getStorageProviderClient()
		if err != nil {
			return err
		}

		req := &storageproviderv0alphapb.DeleteRequest{Filename: fn}
		res, err := client.Delete(ctx, req)
		if err != nil {
			return err
		}

		if res.Status.Code != rpcpb.Code_CODE_OK {
			return formatError(res.Status)
		}

		return nil
	}
	return cmd
}
