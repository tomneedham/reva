package promsvc

import (
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type svc struct {
	path string
	handler http.Handler
}

func New(m map[string]interface{}) (*svc, error) {
	return &svc{}, nil
}

func (s *svc) GetPath() string {
	return "/metrics"
}

func (s *svc) GetHandler() http.Handler {
	return promhttp.Handler()
}
