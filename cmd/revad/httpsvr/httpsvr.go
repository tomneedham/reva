package httpsvr

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/cernbox/reva/pkg/err"
	"github.com/cernbox/reva/pkg/log"
	"github.com/mitchellh/mapstructure"
)

var (
	ctx    = context.Background()
	logger = log.New("httpsvr")
	errors = err.New("httpsvr")
)

type config struct {
	Network            string                 `mapstructure:"network"`
	Address            string                 `mapstructure:"address"`
	EnabledServices    []string               `mapstructure:"enabled_services"`
	StorageProviderSvc map[string]interface{} `mapstructure:"storage_provider_svc"`
	AuthSvc            map[string]interface{} `mapstructure:"auth_svc"`
}

type Server struct {
	s        *http.Server
	conf     *config
	listener net.Listener
}

func New(m map[string]interface{}) (*Server, error) {
	conf := &config{}
	if err := mapstructure.Decode(m, conf); err != nil {
		return nil, err
	}

	s := &http.Server{}
	s.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		return
	})
	return &Server{s: s, conf: conf}, nil
}

func (s *Server) Start(ln net.Listener) error {
	s.listener = ln
	err := s.s.Serve(s.listener)
	if err == nil || err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) Stop() error {
	// TODO(labkode): set ctx deadline to zero
	ctx, _ = context.WithTimeout(ctx, time.Second)
	return s.s.Shutdown(ctx)
}

func (s *Server) Network() string {
	return s.conf.Network
}

func (s *Server) Address() string {
	return s.conf.Address
}

func (s *Server) GracefulStop() error {
	return s.s.Shutdown(ctx)
}
