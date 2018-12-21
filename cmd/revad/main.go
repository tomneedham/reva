package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/cernbox/reva/pkg/log"
	//	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"net"
	//"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cernbox/reva/cmd/revad/config"
	"github.com/cernbox/reva/cmd/revad/grpcsvr"
)

var (
	logger    = log.New("main")
	ctx       = context.Background()
	parentPID = -1

	versionFlag = flag.Bool("v", false, "show version and exit")
	testFlag    = flag.Bool("t", false, "test configuration and exit")
	signalFlag  = flag.String("s", "", "send signal to a master process: stop, quit, reopen, reload")
	fileFlag    = flag.String("c", "/etc/revad/revad.toml", "set configuration file")
)

func main() {
	logger.Println(ctx, "init: REVA started")
	logger.Println(ctx, "logging enabled for the following packages ", log.ListEnabledPackages())
	checkFlags()
	readConfig()
	grpcSvr := getGRPCServer()

	go func() {
		ch := grpcSvr.Start()
		if err := <-ch; err != nil {
			logger.Error(ctx, err)
			os.Exit(1)
		}
	}()

	// server prometheus metrics
	//http.Handle("/metrics", promhttp.Handler())
	//go http.ListenAndServe(":8080", nil)

	waitForSignals(grpcSvr)
}

func recoveryFunc(ctx context.Context, p interface{}) (err error) {
	logger.Panic(ctx, fmt.Sprintf("%+v", p))
	return grpc.Errorf(codes.Internal, "%s", p)
}

func getGRPCServer() *grpcsvr.Server {
	s, err := grpcsvr.New(config.Get("grpc"))
	if err != nil {
		logger.Error(ctx, err)
		os.Exit(1)
	}
	return s
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

	// TODO(labkode): if the process dies (because config changed and is wrong
	// we need to return an error
	if err != nil {
		return nil, err
	}

	return p, nil
}

func waitForSignals(server *grpcsvr.Server) {
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
				p, err := forkChild(server.Listener())
				if err != nil {
					logger.Println(ctx, "unable to fork child process: ", err)
				} else {
					logger.Printf(ctx, "child forked with new pid %d", p.Pid)
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

func checkFlags() {
	flag.Parse()

	if *versionFlag {
		fmt.Println("revad v0alpha.0.24")
		os.Exit(1)
	}

	if *fileFlag != "" {
		config.SetFile(*fileFlag)
	}

	if *testFlag {
		fmt.Println("testing configuration file: ", *fileFlag)
		os.Exit(1)
	}

	if *signalFlag != "" {
		fmt.Println("signaling master process")
		os.Exit(1)
	}
}

func readConfig() {
	err := config.Read()
	if err != nil {
		logger.Println(ctx, "unable to read configuration file:", *fileFlag, err)
		os.Exit(1)
	}
	//logger.Println(ctx, config.Dump())
}
