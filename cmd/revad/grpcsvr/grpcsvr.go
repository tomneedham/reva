package grpcsvr

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"

	"github.com/cernbox/go-cs3apis/cs3/auth/v0alpha"
	"github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
	"github.com/cernbox/reva/pkg/err"
	"github.com/cernbox/reva/pkg/log"
	"github.com/cernbox/reva/services/authsvc"
	"github.com/cernbox/reva/services/interceptors"
	"github.com/cernbox/reva/services/storageprovidersvc"
	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/mitchellh/mapstructure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	ctx      = context.Background()
	logger   = log.New("grpcsvr")
	errors   = err.New("grpcsvr")
	graceful = os.Getenv("GRACEFUL") == "true"
)

type config struct {
	Network            string                 `mapstructure:"network"`
	Address            string                 `mapstructure:"address"`
	ShutdownDeadline   int                    `mapstructure:"shutdown_deadline"`
	EnabledServices    []string               `mapstructure:"enabled_services"`
	StorageProviderSvc map[string]interface{} `mapstructure:"storage_provider_svc"`
	AuthSvc            map[string]interface{} `mapstructure:"auth_svc"`
}

type Server struct {
	s        *grpc.Server
	conf     *config
	listener net.Listener
}

func New(m map[string]interface{}) (*Server, error) {
	conf := &config{}
	if err := mapstructure.Decode(m, conf); err != nil {
		return nil, err
	}

	opts := getOpts()
	s := grpc.NewServer(opts...)

	return &Server{s: s, conf: conf}, nil
}

func (s *Server) Listener() net.Listener {
	return s.listener
}

func (s *Server) Start() chan error {
	ch := make(chan error, 1)
	go func() {
		if err := s.registerServices(); err != nil {
			err = errors.Wrap(err, "unable to register service")
			ch <- err
		}

		ln, err := s.getListener()
		if err != nil {
			err = errors.Wrap(err, "unable to get net listener")
			ch <- err
		}
		s.listener = ln

		err = s.s.Serve(s.listener)
		if err != nil {
			err = errors.Wrap(err, "serve failed")
			ch <- err
		} else {
			ch <- nil
		}
	}()
	return ch
}

func (s *Server) Stop() error {
	s.s.Stop()
	return nil
}

func (s *Server) GracefulStop() error {
	s.s.GracefulStop()
	return nil
}

func (s *Server) registerServices() error {
	enabled := []string{}
	for _, k := range s.conf.EnabledServices {
		switch k {
		case "storage_provider_svc":
			svc, err := storageprovidersvc.New(s.conf.StorageProviderSvc)
			if err != nil {
				return errors.Wrap(err, "unable to register service "+k)
			}
			storageproviderv0alphapb.RegisterStorageProviderServiceServer(s.s, svc)
			logger.Printf(ctx, "service %s registered", k)
			enabled = append(enabled, k)
		case "auth_svc":
			svc, err := authsvc.New(s.conf.AuthSvc)
			if err != nil {
				return errors.Wrap(err, "unable to register service "+k)
			}
			authv0alphapb.RegisterAuthServiceServer(s.s, svc)
			logger.Printf(ctx, "service %s registered", k)
			enabled = append(enabled, k)
		}
	}
	if len(enabled) == 0 {
		logger.Println(ctx, "no services enabled")
	} else {
		logger.Println(ctx, "grpc enabled for the following services ", enabled)
	}
	return nil
}

func (s *Server) getListener() (net.Listener, error) {
	parentPID := os.Getppid()
	var ln net.Listener
	if graceful {
		logger.Println(ctx, "graceful restart, inheriting parent listener fd")
		fd := os.NewFile(3, "") // 3 because ExtraFile passed to new process
		l, err := net.FileListener(fd)
		if err == nil {
			// kill parent
			logger.Printf(ctx, "killing parent pid gracefully with SIGQUIT: %d", parentPID)
			syscall.Kill(parentPID, syscall.SIGQUIT)
			ln = l
		} else {
			// continue to creating new fd
			logger.Println(ctx, "error inheriting parent fd listener socket: ", err)
		}
	}

	if ln == nil { // create new fd only if we are in a non-forked process or inheriting failed
		network, addr := s.conf.Network, s.conf.Address
		l, err := net.Listen(network, addr)
		if err != nil {
			return nil, err
		}
		ln = l

	}

	return ln, nil
}

func getOpts() []grpc.ServerOption {
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
	return opts
}

func recoveryFunc(ctx context.Context, p interface{}) (err error) {
	logger.Panic(ctx, fmt.Sprintf("%+v", p))
	return grpc.Errorf(codes.Internal, "%s", p)
}
