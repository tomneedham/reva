package grace

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cernbox/reva/pkg/err"
	"github.com/cernbox/reva/pkg/log"
)

var (
	ctx       = context.Background()
	logger    = log.New("grpcsvr")
	errors    = err.New("grpcsvr")
	graceful  = os.Getenv("GRACEFUL") == "true"
	parentPID = os.Getppid()
	listeners = []net.Listener{}
	srvrs     = []Server{}
)

func newListener(network, addr string) (net.Listener, error) {
	return net.Listen(network, addr)
}

// return grpc listener first and http listener second.
func GetListeners(servers []Server) ([]net.Listener, error) {
	srvrs = servers
	lns := []net.Listener{}
	if graceful {
		logger.Println(ctx, "graceful restart, inheriting parent ln fds for grpc and http")
		count := 3
		for _, s := range servers {
			network, addr := s.Network(), s.Address()
			fd := os.NewFile(uintptr(count), "") // 3 because ExtraFile passed to new process
			count++
			ln, err := net.FileListener(fd)
			if err != nil {
				logger.Error(ctx, err)
				// create new fd
				ln, err := newListener(network, addr)
				if err != nil {
					return nil, err
				}
				lns = append(lns, ln)
			} else {
				lns = append(lns, ln)
			}

		}
		// kill parent
		logger.Printf(ctx, "killing parent pid gracefully with SIGQUIT: %d", parentPID)
		syscall.Kill(parentPID, syscall.SIGQUIT)
		listeners = lns
		return lns, nil
	} else {
		// create two listeners for grpc and http
		for _, s := range servers {
			network, addr := s.Network(), s.Address()
			ln, err := newListener(network, addr)
			if err != nil {
				return nil, err
			}
			lns = append(lns, ln)

		}
		listeners = lns
		return lns, nil
	}
}

type Server interface {
	Stop() error
	GracefulStop() error
	Network() string
	Address() string
}

func TrapSignals() {
	signalCh := make(chan os.Signal, 1024)
	signal.Notify(signalCh, syscall.SIGHUP, syscall.SIGUSR2, syscall.SIGINT, syscall.SIGQUIT)
	for {
		select {
		case s := <-signalCh:
			logger.Printf(ctx, "%v signal received", s)
			switch s {
			case syscall.SIGHUP, syscall.SIGUSR2:
				logger.Println(ctx, "preparing for a hot-reload, forking child process...")
				// Fork a child process.
				listeners := getListeners()
				p, err := forkChild(listeners...)
				if err != nil {
					logger.Println(ctx, "unable to fork child process: ", err)
				} else {
					logger.Printf(ctx, "child forked with new pid %d", p.Pid)
				}

			case syscall.SIGQUIT:
				logger.Println(ctx, "preparing for a graceful shutdown with deadline of 10 seconds")
				go func() {
					count := 10
					for range time.Tick(time.Second) {
						logger.Printf(ctx, "shuting down in %d seconds", count-1)
						count--
						if count <= 0 {
							logger.Println(ctx, "deadline reached before draining active conns, hard stoping ...")
							for _, s := range srvrs {
								s.Stop()
								logger.Printf(ctx, "fd to %s:%s abruptly closed", s.Network(), s.Address())
							}
							os.Exit(1)
						}
					}
				}()
				for _, s := range srvrs {
					logger.Printf(ctx, "fd to %s:%s gracefully closed ", s.Network(), s.Address())
					s.GracefulStop()
				}
				os.Exit(0)
			case syscall.SIGINT, syscall.SIGTERM:
				logger.Println(ctx, "preparing for hard shutdown, aborting all conns")
				for _, s := range srvrs {
					logger.Printf(ctx, "fd to %s:%s abruptly closed", s.Network(), s.Address())
					s.Stop()
				}
				os.Exit(0)
			}
		}
	}
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

func forkChild(lns ...net.Listener) (*os.Process, error) {
	// Get the file descriptor for the listener and marshal the metadata to pass
	// to the child in the environment.
	fds := []*os.File{}
	for _, ln := range lns {
		fd, err := getListenerFile(ln)
		if err != nil {
			return nil, err
		}
		fds = append(fds, fd)
	}

	// Pass stdin, stdout, and stderr along with the listener file to the child
	files := []*os.File{
		os.Stdin,
		os.Stdout,
		os.Stderr,
	}
	files = append(files, fds...)

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

func getListeners() []net.Listener {
	return listeners
}
