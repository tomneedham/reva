package webuisvc

import (
	"net/http"
)

type svc struct {
	path string
	handler http.Handler
}

func New(m map[string]interface{}) (*svc, error) {
	return &svc{}, nil
}

func (s *svc) GetPath() string {
	return "/ui"
}

func (s *svc) GetHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request){
		w.Write([]byte("<h1>Phoenix will go here</h1>"))
	})
}
