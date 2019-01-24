package ocdavsvc

import (
	"net/http"

	storageproviderv0alphapb "github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
	"github.com/cernbox/reva/pkg/log"
	"github.com/cernbox/reva/services/httpsvc"
	"github.com/mitchellh/mapstructure"
	"google.golang.org/grpc"
)

var logger = log.New("ocdavsvc")

type config struct {
	Prefix             string `mapstructure:"prefix"`
	StorageProviderSvc string `mapstructure:"storageprovidersvc"`
}

type svc struct {
	prefix             string
	handler            http.Handler
	storageProviderSvc string
	conn               *grpc.ClientConn
	client             storageproviderv0alphapb.StorageProviderServiceClient
}

// New returns a new ocdavsvc
func New(m map[string]interface{}) (httpsvc.Service, error) {
	conf := &config{}
	if err := mapstructure.Decode(m, conf); err != nil {
		return nil, err
	}

	s := &svc{prefix: conf.Prefix, storageProviderSvc: conf.StorageProviderSvc}
	s.setHandler()
	return s, nil
}

func (s *svc) Prefix() string {
	return s.prefix
}

func (s *svc) Handler() http.Handler {
	return s.handler
}

func (s *svc) setHandler() {
	s.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PROPFIND":
			s.doPropfind(w, r)
			return
		case "OPTIONS":
			s.doOptions(w, r)
			return
		case "HEAD":
			s.doHead(w, r)
			return
		case "GET":
			s.doGet(w, r)
		case "LOCK":
			s.doLock(w, r)
			return
		case "UNLOCK":
			s.doUnlock(w, r)
			return
		case "PROPPATCH":
			s.doProppatch(w, r)
			return
		case "MKCOL":
			s.doMkcol(w, r)
			return
		case "MOVE":
			s.doMove(w, r)
			return
		case "PUT":
			s.doPut(w, r)
			return
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func (s *svc) getConn() (*grpc.ClientConn, error) {
	if s.conn != nil {
		return s.conn, nil
	}

	conn, err := grpc.Dial(s.storageProviderSvc, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (s *svc) getClient() (storageproviderv0alphapb.StorageProviderServiceClient, error) {
	if s.client != nil {
		return s.client, nil
	}

	conn, err := s.getConn()
	if err != nil {
		return nil, err
	}
	s.client = storageproviderv0alphapb.NewStorageProviderServiceClient(conn)
	return s.client, nil
}
