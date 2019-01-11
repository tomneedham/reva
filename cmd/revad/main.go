package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/cernbox/reva/pkg/err"
	"github.com/cernbox/reva/pkg/log"

	"github.com/cernbox/reva/cmd/revad/config"
	"github.com/cernbox/reva/cmd/revad/grace"
	"github.com/cernbox/reva/cmd/revad/grpcsvr"
	"github.com/cernbox/reva/cmd/revad/httpsvr"
)

var (
	errors = err.New("main")
	logger = log.New("main")
	ctx    = context.Background()

	versionFlag = flag.Bool("v", false, "show version and exit")
	testFlag    = flag.Bool("t", false, "test configuration and exit")
	signalFlag  = flag.String("s", "", "send signal to a master process: stop, quit, reopen, reload")
	fileFlag    = flag.String("c", "/etc/revad/revad.toml", "set configuration file")
	pidFlag     = flag.String("p", "/var/run/revad.pid", "pid file")

	// provided at compile time
	GitCommit, GitBranch, GitState, GitSummary, BuildDate, Version string
)

func main() {
	checkFlags()
	writePIDFile()
	readConfig()

	logger.Println(ctx, "reva is booting")
	logger.Println(ctx, "logging enabled for the following packages ", log.ListEnabledPackages())

	grpcSvr := getGRPCServer()
	httpSvr := getHTTPServer()
	servers := []grace.Server{grpcSvr, httpSvr}
	listeners, err := grace.GetListeners(servers)
	if err != nil {
		logger.Error(ctx, err)
		grace.Exit(1)
	}

	go func() {
		if err := grpcSvr.Start(listeners[0]); err != nil {
			err = errors.Wrap(err, "error starting grpc server")
			logger.Error(ctx, err)
			grace.Exit(1)
		}
	}()

	go func() {
		if err := httpSvr.Start(listeners[1]); err != nil {
			err = errors.Wrap(err, "error starting http server")
			logger.Error(ctx, err)
			grace.Exit(1)
		}
	}()

	grace.TrapSignals()
}

func getGRPCServer() *grpcsvr.Server {
	s, err := grpcsvr.New(config.Get("grpc"))
	if err != nil {
		logger.Error(ctx, err)
		grace.Exit(1)
	}
	return s
}

func getHTTPServer() *httpsvr.Server {
	s, err := httpsvr.New(config.Get("http"))
	if err != nil {
		logger.Error(ctx, err)
		grace.Exit(1)
	}
	return s
}

func checkFlags() {
	flag.Parse()

	if *versionFlag {
		msg := "Version: %s\n"
		msg += "GitCommit: %s\n"
		msg += "GitBranch: %s\n"
		msg += "GitSummary: %s\n"
		msg += "BuildDate: %s\n"
		fmt.Printf(msg, Version, GitCommit, GitBranch, GitSummary, BuildDate)
		grace.Exit(1)
	}

	if *fileFlag != "" {
		config.SetFile(*fileFlag)
	}

	if *testFlag {
		err := config.Read()
		if err != nil {
			logger.Println(ctx, "unable to read configuration file: ", *fileFlag, err)
			grace.Exit(1)
		}
		grace.Exit(0)
	}

	if *signalFlag != "" {
		fmt.Println("signaling master process")
		grace.Exit(1)
	}
}

func readConfig() {
	err := config.Read()
	if err != nil {
		logger.Println(ctx, "unable to read configuration file:", *fileFlag, err)
		grace.Exit(1)
	}
	//logger.Println(ctx, config.Dump())
}

func writePIDFile() {
	err := grace.WritePIDFile(*pidFlag)
	if err != nil {
		logger.Error(ctx, err)
		grace.Exit(1)
	}
}
