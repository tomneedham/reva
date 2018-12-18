package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/cernbox/reva/pkg/config"
	"github.com/cernbox/reva/pkg/log"

	"github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
	"github.com/cernbox/reva/services/interceptors"
	"github.com/cernbox/reva/services/storageprovidersvc"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	defaultConfig = "/etc/revad.yaml"
	configFile    = defaultConfig
	logger        = log.New("main")
	ctx           = context.Background()
)

func main() {
	versionFlag := flag.Bool("v", false, "show version and exit")
	testFlag := flag.Bool("t", false, "test configuration and exit")
	signalFlag := flag.String("s", "", "send signal to a master process: stop, quit, reopen, reload")
	fileFlag := flag.String("c", defaultConfig, "set configuration file")

	flag.Parse()

	if *versionFlag {
		fmt.Println("revad v0alpha.0.24")
		os.Exit(1)
	}

	if *fileFlag != "" {
		configFile = *fileFlag
	}

	if *testFlag {
		fmt.Println("testing configuration file: ", configFile)
		os.Exit(1)
	}

	if *signalFlag != "" {
		fmt.Println("signaling master process")
		os.Exit(1)
	}

	// loadConfig will exit on error reading or parsing the configuration
	// directives in the provided configuratin file.
	cfg, err := config.LoadFromFile(configFile)
	if err != nil {
		logger.Println(ctx, "error parsing configuration file: ", err)
		os.Exit(1)
	}

	logger.Println(ctx, "config loaded from file: "+configFile)
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				interceptors.TraceUnaryServerInterceptor(),
				interceptors.LogUnaryServerInterceptor(),
				grpc_prometheus.UnaryServerInterceptor,
				grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(recoveryFunc)))),
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				interceptors.TraceStreamServerInterceptor(),
				grpc_prometheus.StreamServerInterceptor,
				grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(recoveryFunc)))),
	}
	server := grpc.NewServer(opts...)

	logger.Printf(ctx, "going to listen on network=%s address=%s", cfg.Network, cfg.Address)
	lis, err := net.Listen(cfg.Network, cfg.Address)
	if err != nil {
		logger.Error(ctx, err)
		os.Exit(1)
	}

	// server prometheus metrics
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(":8080", nil)

	logger.Printf(ctx, "enabled services: %+v", cfg.Services)
	for _, svc := range cfg.Services {
		switch svc {
		case "storage_provider_svc":
			service := storageprovidersvc.New(cfg)
			storageproviderv0alphapb.RegisterStorageProviderServiceServer(server, service)

		}
	}

	logger.Printf(ctx, "grpc at %s:///%s", cfg.Network, cfg.Address)
	logger.Printf(ctx, "prometheus at %s:///%s", "tcp", "localhost:8080")

	server.Serve(lis)

}

func recoveryFunc(ctx context.Context, p interface{}) (err error) {
	logger.Panic(ctx, fmt.Sprintf("%+v", p))
	return grpc.Errorf(codes.Internal, "%s", p)
}
