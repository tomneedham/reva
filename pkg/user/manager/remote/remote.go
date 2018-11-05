package remote

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cernbox/reva/pkg/logger"
	"github.com/cernbox/reva/pkg/user"
)

// Options are the configuration options to pass to New for creating a new remote user manager.
type Options struct {
	RemoteURL                         string
	Secret                            string
	HTTPTransportMaxIddleConns        int
	HTTPTransportMaxIddleConnsPerHost int
	HTTPTransportDisableKeepAlives    bool
	HTTPTransportIddleConnTimeout     int
	HTTPTransportInsecureSkipVerify   bool
	LoggerOut                         io.Writer
	LoggerKey                         interface{}
}

func (opt *Options) init() {
	if opt.RemoteURL == "" {
		opt.RemoteURL = "http://localhost:2002"
	}

	if opt.HTTPTransportMaxIddleConns == 0 {
		opt.HTTPTransportMaxIddleConns = 100
	}

	if opt.HTTPTransportMaxIddleConnsPerHost == 0 {
		opt.HTTPTransportMaxIddleConnsPerHost = 100
	}
}

// New returns a new user manager that connects to the cboxgroupdaemon.
func New(opt *Options) user.Manager {
	if opt == nil {
		opt = &Options{}
	}

	opt.init()
	tr := &http.Transport{
		DisableKeepAlives:   opt.HTTPTransportDisableKeepAlives,
		IdleConnTimeout:     time.Duration(opt.HTTPTransportIddleConnTimeout) * time.Second,
		MaxIdleConns:        opt.HTTPTransportMaxIddleConns,
		MaxIdleConnsPerHost: opt.HTTPTransportMaxIddleConnsPerHost,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: opt.HTTPTransportInsecureSkipVerify},
	}

	return &userManager{
		secret:    opt.Secret,
		remoteURL: opt.RemoteURL,
		tr:        tr,
		logger:    logger.New(opt.LoggerOut, "remote", opt.LoggerKey),
	}
}

type userManager struct {
	remoteURL string
	secret    string
	tr        *http.Transport
	logger    *logger.Logger
}

func (um *userManager) IsInGroup(ctx context.Context, username, group string) (bool, error) {
	groups, err := um.GetUserGroups(ctx, username)
	if err != nil {
		return false, err
	}
	for _, g := range groups {
		if g == group {
			return true, nil
		}
	}
	return false, nil
}

func (um *userManager) GetUserGroups(ctx context.Context, username string) ([]string, error) {
	groups := []string{}
	client := &http.Client{Transport: um.tr}
	url := fmt.Sprintf("%s/user/v1/membership/usergroups/%s", um.remoteURL, username)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return groups, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", um.secret))
	res, err := client.Do(req)
	if err != nil {
		return groups, err
	}

	if res.StatusCode != http.StatusOK {
		err := errors.New("error calling cboxgroupd membership")
		return groups, err
	}

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return groups, err
	}

	err = json.Unmarshal(body, &groups)
	if err != nil {
		return groups, err
	}
	return groups, nil
}

type groupResponse []string

/*
// search calls the cboxgroupd daemon for finding entries.
func (p *proxy) search(w http.ResponseWriter, r *http.Request) {
	search := r.URL.Query().Get("search")

	//itemType := r.URL.Query().Get("itemType")
	//perPage := r.URL.Query().Get("perPage")

	if search == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	searchTarget := p.getSearchTarget(search)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}
	url := fmt.Sprintf("%s/user/v1/search/%s", p.remoteURL, search)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		p.logger.Error("", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", p.secret))
	res, err := client.Do(req)
	if err != nil {
		p.logger.Error("", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if res.StatusCode != http.StatusOK {
		p.logger.Error("error calling cboxgroupd search", zap.Int("status", res.StatusCode))
		w.WriteHeader(res.StatusCode)
		return

	}

	searchEntries := []*searchEntry{}
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		p.logger.Error("", zap.Error(err))
		w.WriteHeader(res.StatusCode)
		return
	}

}
*/
