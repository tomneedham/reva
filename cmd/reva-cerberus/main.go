package main

import (
	"net"
	"os"

	"github.com/cernbox/reva/pkg/auth"
	authdemo "github.com/cernbox/reva/pkg/auth/manager/demo"
	"github.com/cernbox/reva/pkg/token"
	tokendemo "github.com/cernbox/reva/pkg/token/manager/demo"
	"github.com/cernbox/reva/pkg/user"
	userdemo "github.com/cernbox/reva/pkg/user/manager/demo"

	"github.com/cernbox/cs3apis/gen/proto/go/cs3/auth/v1"
	"github.com/cernbox/reva/services/authservice"

	"github.com/cernbox/gohub/goconfig"
	"google.golang.org/grpc"
)

var gc *goconfig.GoConfig

func init() {

	gc = goconfig.New()
	gc.SetConfigName("reva-cerberus")
	gc.AddConfigurationPaths("/etc/reva-cerberus")
	gc.Add("tcp-address", "localhost:9001", "tcp addresss to listen for connections")
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

	authMgr := getAuthManager()
	tokenMgr := getTokenManager()
	userMgr := getUserManager()
	service := authservice.New(authMgr, tokenMgr, userMgr, os.Stdout, "authservice")

	lis, err := net.Listen("tcp", gc.GetString("tcp-address"))
	if err != nil {
		panic(err)
	}

	server := grpc.NewServer(nil)
	authv1pb.RegisterAuthServiceServer(server, service)
	server.Serve(lis)
}

func getUserManager() user.Manager   { return userdemo.New() }
func getAuthManager() auth.Manager   { return authdemo.New() }
func getTokenManager() token.Manager { return tokendemo.New() }
