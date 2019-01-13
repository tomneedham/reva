package main

import (
	"fmt"

	"github.com/pkg/errors"

	authv0alphapb "github.com/cernbox/go-cs3apis/cs3/auth/v0alpha"
	rpcpb "github.com/cernbox/go-cs3apis/cs3/rpc"
	storageproviderv0alphapb "github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
	"google.golang.org/grpc"
)

func getStorageProviderClient() (storageproviderv0alphapb.StorageProviderServiceClient, error) {
	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	return storageproviderv0alphapb.NewStorageProviderServiceClient(conn), nil
}

func getAuthClient() (authv0alphapb.AuthServiceClient, error) {
	conn, err := getConn()
	if err != nil {
		return nil, err
	}
	return authv0alphapb.NewAuthServiceClient(conn), nil
}

func getConn() (*grpc.ClientConn, error) {
	return grpc.Dial(conf.Host, grpc.WithInsecure())
}

func formatError(status *rpcpb.Status) error {
	switch status.Code {
	case rpcpb.Code_CODE_NOT_FOUND:
		return errors.New("error: not found")

	default:
		return errors.New(fmt.Sprintf("apierror: code=%v msg=%s", status.Code, status.Message))
	}
}
