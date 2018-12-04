package main

import (
	"context"
	"net"
	"os"

	"github.com/cernbox/reva/pkg/log"

	"github.com/cernbox/reva/pkg/storage"
	"github.com/cernbox/reva/pkg/storage/fstable"
	"github.com/cernbox/reva/pkg/storage/local"

	"github.com/cernbox/cs3apis/gen/proto/go/cs3/storage/v1"
	"github.com/cernbox/reva/services/interceptors"
	"github.com/cernbox/reva/services/storageservice"

	"github.com/cernbox/gohub/goconfig"
	"google.golang.org/grpc"
)

var gc *goconfig.GoConfig

var logger = log.New("main")
var ctx = context.Background()

func init() {
	log.Enable("storageservice")

	gc = goconfig.New()
	gc.SetConfigName("reva-storaged")
	gc.AddConfigurationPaths("/etc/reva-storaged")
	gc.Add("tcp-address", "localhost:9002", "tcp addresss to listen for connections")
	gc.Add("app-log", "stderr", "file to log application information")
	gc.Add("tls-cert", "/etc/grid-security/hostcert.pem", "TLS certificate to encrypt connections.")
	gc.Add("tls-key", "/etc/grid-security/hostkey.pem", "TLS private key to encrypt connections.")
	gc.Add("tls-enable", false, "Enable TLS for encrypting connections.")

	gc.Add("token-manager", "demo", "token manager implementation to use")
	gc.Add("user-manager", "demo", "user manager implementation to use")
	gc.Add("auth-manager", "demo", "auth manager implementation to use")

	gc.BindFlags()
	gc.ReadConfig()
}

func main() {
	//fsTable := getFSTable()
	fs := local.New("/tmp")
	service := storageservice.New(fs, "/tmp")

	lis, err := net.Listen("tcp", gc.GetString("tcp-address"))
	if err != nil {
		logger.Error(ctx, err)
		os.Exit(1)
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(interceptors.TraceUnaryServerInterceptor()),
		grpc.StreamInterceptor(interceptors.TraceStreamServerInterceptor()),
	}
	server := grpc.NewServer(opts...)
	storagev1pb.RegisterStorageServiceServer(server, service)
	logger.Println(ctx, "listening")
	server.Serve(lis)
}

func getFSTable() storage.FSTable {
	return fstable.New()
}
