package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

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
	graceful      = os.Getenv("GRACEFUL") == "true"
	parentPID     = -1

	versionFlag = flag.Bool("v", false, "show version and exit")
	testFlag    = flag.Bool("t", false, "test configuration and exit")
	signalFlag  = flag.String("s", "", "send signal to a master process: stop, quit, reopen, reload")
	fileFlag    = flag.String("c", defaultConfig, "set configuration file")
)

func main() {

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

	if graceful {
		parentPID = syscall.Getppid()
		logger.Println(ctx, "child process: forked from parent process with pid=", parentPID)
	} else {
		logger.Println(ctx, "main process")
	}

	// loadConfig will exit on error reading or parsing the configuration
	// directives in the provided configuratin file.
	cfg, err := config.LoadFromFile(configFile)
	if err != nil {
		logger.Println(ctx, "error parsing configuration file: ", err)
		os.Exit(1)
	}

	// validate configuration
	if cfg.Network != "unix" && cfg.Network != "tcp" {
		logger.Println(ctx, "network socket must be tcp socket or unix domain socket")
		os.Exit(1)
	}

	logger.Println(ctx, "settings loaded from file:"+configFile)
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

	var ln net.Listener
	if graceful {
		logger.Println(ctx, "graceful is set, inheriting parent listener fd")
		fd := os.NewFile(3, "") // 3 because ExtraFile passed to new process
		ln, err = net.FileListener(fd)
		if err == nil {
			// kill parent
			logger.Printf(ctx, "killing parent pid gracefull with SIGQUIT: %d", parentPID)
			syscall.Kill(parentPID, syscall.SIGQUIT)
		} else {
			// continue to creating new fd
			logger.Println(ctx, "error inheriting parent fd listener socket: ", err)
		}
	}

	if ln == nil { // create new fd only if we are in a non-forked process or inheriting failed
		ln, err = net.Listen(cfg.Network, cfg.Address)
		if err != nil {
			logger.Println(ctx, "error listening: ", err)
			os.Exit(1)
		}
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

	go server.Serve(ln)

	waitForSignals(ctx, cfg.Network, cfg.Address, ln, server)
}

func recoveryFunc(ctx context.Context, p interface{}) (err error) {
	logger.Panic(ctx, fmt.Sprintf("%+v", p))
	return grpc.Errorf(codes.Internal, "%s", p)
}

func getListenerFile(ln net.Listener) (*os.File, error) {
	switch t := ln.(type) {
	case *net.TCPListener:
		return t.File()
	case *net.UnixListener:
		return t.File()
	}
	return nil, fmt.Errorf("unsupported listener: %T", ln)
}

func forkChild(ln net.Listener) (*os.Process, error) {
	// Get the file descriptor for the listener and marshal the metadata to pass
	// to the child in the environment.
	fd, err := getListenerFile(ln)
	if err != nil {
		return nil, err
	}

	// Pass stdin, stdout, and stderr along with the listener file to the child
	files := []*os.File{
		os.Stdin,
		os.Stdout,
		os.Stderr,
		fd,
	}

	// Get current environment and add in the listener to it.
	environment := append(os.Environ(), "GRACEFUL=true")

	// Get current process name and directory.
	execName, err := os.Executable()
	if err != nil {
		return nil, err
	}
	execDir := filepath.Dir(execName)

	// Spawn child process.
	p, err := os.StartProcess(execName, os.Args, &os.ProcAttr{
		Dir:   execDir,
		Env:   environment,
		Files: files,
		Sys:   &syscall.SysProcAttr{},
	})
	if err != nil {
		return nil, err
	}

	return p, nil
}

func waitForSignals(ctx context.Context, network, addr string, ln net.Listener, server *grpc.Server) {
	signalCh := make(chan os.Signal, 1024)
	signal.Notify(signalCh, syscall.SIGHUP, syscall.SIGUSR2, syscall.SIGINT, syscall.SIGQUIT)
	for {
		select {
		case s := <-signalCh:
			logger.Printf(ctx, "%v signal received", s)
			switch s {
			case syscall.SIGHUP, syscall.SIGUSR2:
				logger.Println(ctx, "graceful restart with configuration changes")
				// Fork a child process.
				p, err := forkChild(ln)
				if err != nil {
					logger.Println(ctx, "unable to fork child process: ", err)
				} else {
					logger.Println(ctx, "child forked with new pid", p.Pid)
				}

			case syscall.SIGQUIT:
				logger.Println(ctx, "performing graceful shutdown with deadline to 10 seconds")
				go func() {
					count := 10
					for range time.Tick(time.Second) {
						logger.Printf(ctx, "shuting down in %d seconds", count-1)
						count--
						if count <= 0 {
							logger.Println(ctx, "deadline reached before all connections could have been drained")
							server.Stop()
							os.Exit(1)
						}
					}
				}()
				server.GracefulStop()
				logger.Println(ctx, "graceful shutdown performed")
				os.Exit(0)
			case syscall.SIGINT, syscall.SIGTERM:
				logger.Println(ctx, "fast shutdown")
				logger.Println(ctx, "aborting all connections")
				server.Stop()
				os.Exit(0)
			}
		}
	}
}
