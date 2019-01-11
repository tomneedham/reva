package httpsvr

import (
	"context"
	"net"
	"net/http"
	"time"
	
	"github.com/cernbox/reva/services/http/promsvc"
	"github.com/cernbox/reva/services/http/webuisvc"

	"github.com/cernbox/reva/pkg/err"
	"github.com/cernbox/reva/pkg/log"
	"github.com/mitchellh/mapstructure"
	"github.com/gorilla/mux"
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
	PrometheusSvc	   map[string]interface{} `mapstructure:"prometheus_svc"`
	WebUISvc	 map[string]interface{} `mapstructure:"webui_svc"`
}

type Server struct {
	httpServer        *http.Server
	conf     *config
	listener net.Listener
	router *mux.Router
}

func New(m map[string]interface{}) (*Server, error) {
	conf := &config{}
	if err := mapstructure.Decode(m, conf); err != nil {
		return nil, err
	}
	
	router := mux.NewRouter()
	httpServer  := &http.Server{Handler: router}
	return &Server{httpServer: httpServer, conf: conf, router: router}, nil
}

func (s *Server) Start(ln net.Listener) error {
	if err := s.registerServices(); err != nil {
		err = errors.Wrap(err, "unable to register http services")
		return err
	}

	s.listener = ln
	// always return non-nil error
	// when calling Shutdown, the error returned will be ErrServerClosed
	// TODO(labkode): wait for connections to close up to shutdown timeout
	err := s.httpServer.Serve(s.listener)
	if err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) registerServices() error {
	enabled := []string{}
	for _, k := range s.conf.EnabledServices {
		switch k {
		case "prometheus_svc":
			svc, err := promsvc.New(s.conf.PrometheusSvc)
			if err != nil {
				return errors.Wrap(err, "unable to register service "+k)
			}
			s.router.Handle(svc.GetPath(), svc.GetHandler())
			logger.Printf(ctx, "service %s registered", k)
			enabled = append(enabled, k)
		case "webui_svc":
			svc, err := webuisvc.New(s.conf.WebUISvc)
			if err != nil {
				return errors.Wrap(err, "unable to register service "+k)
			}
			s.router.Handle(svc.GetPath(), svc.GetHandler())
			logger.Printf(ctx, "service %s registered", k)
			enabled = append(enabled, k)
		}
	}
	if len(enabled) == 0 {
		logger.Println(ctx, "no http services enabled")
	} else {
		logger.Println(ctx, "http enabled for the following services ", enabled)
	}
	return nil
}

func (s *Server) Stop() error {
	ctx, _ = context.WithTimeout(ctx, time.Second*0)
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) Network() string {
	return s.conf.Network
}

func (s *Server) Address() string {
	return s.conf.Address
}

func (s *Server) GracefulStop() error {
	return s.httpServer.Shutdown(ctx)
}
