package main

import (
	"fmt"
	"net"
	"net/http"
	"plugin"
	"reflect"

	"github.com/cernbox/gohub/goconfig"
	"github.com/cernbox/gohub/gologger"

	"github.com/cernbox/reva/api"
	"github.com/cernbox/reva/api/linkfs"
	"github.com/cernbox/reva/api/mount"
	"github.com/cernbox/reva/api/vfs"
	"github.com/cernbox/reva/reva-server/svcs/authsvc"
	"github.com/cernbox/reva/reva-server/svcs/previewsvc"
	"github.com/cernbox/reva/reva-server/svcs/sharesvc"
	"github.com/cernbox/reva/reva-server/svcs/storagesvc"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/satori/go.uuid"
	"go.uber.org/zap"
)

func main() {
	gc := goconfig.New()
	gc.SetConfigName("reva-server")
	gc.AddConfigurationPaths("/etc/reva-server")
	gc.Add("tcp-address", "localhost:9999", "tcp address to listen for connections.")
	gc.Add("sign-key", "bar", "the key to sign the JWT token.")
	gc.Add("app-log", "stderr", "file to log application information")
	gc.Add("http-log", "stderr", "file to log http log information")
	gc.Add("log-level", "info", "log level to use (debug, info, warn, error)")
	gc.Add("tls-cert", "/etc/grid-security/hostcert.pem", "TLS certificate to encrypt connections.")
	gc.Add("tls-key", "/etc/grid-security/hostkey.pem", "TLS private key to encrypt connections.")
	gc.Add("tls-enable", false, "Enable TLS for encrypting connections.")
	gc.Add("storage-plugin", "/usr/lib/reva/storage-plugin-eos.so", "location of the shared object containing the Storage interface implementation.")
	gc.Add("authmanager-plugin", "/usr/lib/reva/authmanager-plugin-ldap.so", "location of the shared object containing the AuthManager interface implementation.")
	gc.Add("tokenmanager-plugin", "/usr/lib/reva/tokenmanager-plugin-jwt.so", "location of the shared object containing the TokenManager interface implementation.")
	//	gc.Add("sharemanager-plugin", "/usr/lib/reva/sharemanager-plugin-memory.so", "location of the shared object containing the ShareManager interface implementation.")
	gc.Add("publiclinkmanager-plugin", "/usr/lib/reva/publiclinkmanager-plugin-memory.so", "location of the shared object containing the ShareManager interface implementation.")

	gc.BindFlags()
	gc.ReadConfig()

	registerPluginConfigs(gc)

	gc.BindFlags()  // rebind plugins flags.
	gc.ReadConfig() // reload config to take into account plugin config parameters.
	gc.PrintConfig()
	//gc.ExecuteActionFlagsIfAny()

	logger := gologger.New(gc.GetString("log-level"), gc.GetString("app-log"))

	storage := getStoragePlugin(gc, logger)
	tokenManager := getTokenManagerPlugin(gc, logger)
	authManager := getAuthManagerPlugin(gc, logger)

	virtualStorage := vfs.NewVFS(logger)
	publicLinkManager := getPublicLinkManagerPlugin(gc, logger, virtualStorage)

	linksFS := linkfs.NewLinkFS(virtualStorage, publicLinkManager, logger)
	linkMount := mount.New(linksFS, "/publiclinks")

	m := mount.New(storage, "/eos")
	virtualStorage.AddMount(context.Background(), m)
	virtualStorage.AddMount(context.Background(), linkMount)

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_opentracing.StreamServerInterceptor(),
			grpc_prometheus.StreamServerInterceptor,
			grpc_zap.StreamServerInterceptor(logger),
			grpc_auth.StreamServerInterceptor(getAuthFunc(tokenManager)),
			grpc_recovery.StreamServerInterceptor(),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_opentracing.UnaryServerInterceptor(),
			grpc_prometheus.UnaryServerInterceptor,
			grpc_zap.UnaryServerInterceptor(logger),
			grpc_auth.UnaryServerInterceptor(getAuthFunc(tokenManager)),
			grpc_recovery.UnaryServerInterceptor(),
		)),
	)

	// register prometheus metrics
	grpc_prometheus.Register(server)
	http.Handle("/metrics", promhttp.Handler())

	api.RegisterAuthServer(server, authsvc.New(authManager, tokenManager))
	api.RegisterStorageServer(server, storagesvc.New(virtualStorage))
	api.RegisterShareServer(server, sharesvc.New(publicLinkManager))
	api.RegisterPreviewServer(server, previewsvc.New())

	lis, err := net.Listen("tcp", gc.GetString("tcp-address"))
	if err != nil {
		logger.Fatal("failed to listen", zap.Error(err))
	}
	go func() {
		http.ListenAndServe(":1092", nil)
	}()

	server.Serve(lis)
}

func getAuthFunc(tm api.TokenManager) func(context.Context) (context.Context, error) {
	return func(ctx context.Context) (context.Context, error) {
		token, err := grpc_auth.AuthFromMD(ctx, "bearer")
		if err != nil {
			return nil, err
		}

		user, err := tm.VerifyToken(ctx, token)
		if err != nil {
			return nil, grpc.Errorf(codes.Unauthenticated, "invalid auth token: %v", err)
		}

		grpc_ctxtags.Extract(ctx).Set("auth.accountid", user.AccountId)
		uuid, _ := uuid.NewV4()
		tid := uuid.String()
		grpc_ctxtags.Extract(ctx).Set("tid", tid)
		newCtx := api.ContextSetUser(ctx, user)
		return newCtx, nil
	}
}

func registerPluginConfigs(gc *goconfig.GoConfig) error {
	sharedObjects := []string{
		gc.GetString("storage-plugin"),
		gc.GetString("tokenmanager-plugin"),
		gc.GetString("authmanager-plugin"),
		gc.GetString("publiclinkmanager-plugin"),
	}

	for _, so := range sharedObjects {
		plug, err := plugin.Open(so)
		if err != nil {
			return err
		}

		regF, err := plug.Lookup("RegisterConfig")
		if err != nil {
			return err
		}

		registerFunc, ok := regF.(func(*goconfig.GoConfig))
		if !ok {
			return fmt.Errorf("unexpected type from symbol")
		}

		registerFunc(gc)
	}
	return nil
}

func getStoragePlugin(gc *goconfig.GoConfig, logger *zap.Logger) api.Storage {
	so := gc.GetString("storage-plugin")
	plug, err := plugin.Open(so)
	if err != nil {
		panic(err)
	}

	newFunc, err := plug.Lookup("New")
	if err != nil {
		panic(err)
	}

	newStorageFunc, ok := newFunc.(func(*goconfig.GoConfig, *zap.Logger) (api.Storage, error))
	if !ok {
		panic(fmt.Errorf("unexpected type from symbol: %s,", reflect.TypeOf(newFunc)))
	}

	storage, err := newStorageFunc(gc, logger)
	if err != nil {
		panic(err)
	}
	return storage
}

func getAuthManagerPlugin(gc *goconfig.GoConfig, logger *zap.Logger) api.AuthManager {
	so := gc.GetString("authmanager-plugin")
	plug, err := plugin.Open(so)
	if err != nil {
		panic(err)
	}

	newFunc, err := plug.Lookup("New")
	if err != nil {
		panic(err)
	}

	newCastedFunc, ok := newFunc.(func(*goconfig.GoConfig, *zap.Logger) (api.AuthManager, error))
	if !ok {
		panic(fmt.Errorf("unexpected type from symbol"))
	}

	authManager, err := newCastedFunc(gc, logger)
	if err != nil {
		panic(err)
	}
	return authManager
}

func getTokenManagerPlugin(gc *goconfig.GoConfig, logger *zap.Logger) api.TokenManager {
	so := gc.GetString("tokenmanager-plugin")
	plug, err := plugin.Open(so)
	if err != nil {
		panic(err)
	}

	newFunc, err := plug.Lookup("New")
	if err != nil {
		panic(err)
	}

	newCastedFunc, ok := newFunc.(func(*goconfig.GoConfig, *zap.Logger) (api.TokenManager, error))
	if !ok {
		panic(fmt.Errorf("unexpected type from symbol"))
	}

	tokenManager, err := newCastedFunc(gc, logger)
	if err != nil {
		panic(err)
	}
	return tokenManager
}

func getPublicLinkManagerPlugin(gc *goconfig.GoConfig, logger *zap.Logger, vs api.VirtualStorage) api.PublicLinkManager {
	so := gc.GetString("publiclinkmanager-plugin")
	plug, err := plugin.Open(so)
	if err != nil {
		panic(err)
	}

	newFunc, err := plug.Lookup("New")
	if err != nil {
		panic(err)
	}

	newCastedFunc, ok := newFunc.(func(*goconfig.GoConfig, *zap.Logger, api.VirtualStorage) (api.PublicLinkManager, error))
	if !ok {
		panic(fmt.Errorf("unexpected type from symbol: %s", reflect.TypeOf(newFunc)))
	}

	publicLinkManager, err := newCastedFunc(gc, logger, vs)
	if err != nil {
		panic(err)
	}
	return publicLinkManager
}
