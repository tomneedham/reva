package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/cernbox/reva/pkg/log"
	"os"

	"github.com/cernbox/reva/cmd/revad/config"
	"github.com/cernbox/reva/cmd/revad/grace"
	"github.com/cernbox/reva/cmd/revad/grpcsvr"
	"github.com/cernbox/reva/cmd/revad/httpsvr"
)

var (
	logger = log.New("main")
	ctx    = context.Background()

	versionFlag = flag.Bool("v", false, "show version and exit")
	testFlag    = flag.Bool("t", false, "test configuration and exit")
	signalFlag  = flag.String("s", "", "send signal to a master process: stop, quit, reopen, reload")
	fileFlag    = flag.String("c", "/etc/revad/revad.toml", "set configuration file")
)

func main() {
	logger.Println(ctx, "reva is booting")
	logger.Println(ctx, "logging enabled for the following packages ", log.ListEnabledPackages())
	checkFlags()
	readConfig()

	grpcSvr := getGRPCServer()
	httpSvr := getHTTPServer()
	servers := []grace.Server{grpcSvr, httpSvr}
	listeners, err := grace.GetListeners(servers)
	if err != nil {
		logger.Error(ctx, err)
		os.Exit(1)
	}

	go func() {
		if err := grpcSvr.Start(listeners[0]); err != nil {
			logger.Error(ctx, err)
			os.Exit(1)
		}
	}()

	go func() {
		if err := httpSvr.Start(listeners[1]); err != nil {
			logger.Error(ctx, err)
			os.Exit(1)
		}
	}()

	grace.TrapSignals()
}

func getGRPCServer() *grpcsvr.Server {
	s, err := grpcsvr.New(config.Get("grpc"))
	if err != nil {
		logger.Error(ctx, err)
		os.Exit(1)
	}
	return s
}

func getHTTPServer() *httpsvr.Server {
	s, err := httpsvr.New(config.Get("http"))
	if err != nil {
		logger.Error(ctx, err)
		os.Exit(1)
	}
	return s
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
