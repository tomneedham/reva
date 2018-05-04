package main

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/cernbox/gohub/goconfig"
	"github.com/cernbox/reva/api"
	"gopkg.in/ldap.v2"

	"go.uber.org/zap"
)

type authManager struct {
	hostname     string
	port         int
	baseDN       string
	filter       string
	bindUsername string
	bindPassword string
	logger       *zap.Logger
}

func RegisterConfig(gc *goconfig.GoConfig) {
	gc.Add("authmanager-plugin-ldap-hostname", "localhost", "the ldap hostname.")
	gc.Add("authmanager-plugin-ldap-port", "636", "the ldap port.")
	gc.Add("authmanager-plugin-ldap-base-dn", "DC=example,DC=org", "the ldap base dn.")
	gc.Add("authmanager-plugin-ldap-base-filter", "(samaccountname=%s)", "the ldap filter.")
	gc.Add("authmanager-plugin-ldap-bind-username", "foo", "the ldap username to bind the connection.")
	gc.Add("authmanager-plugin-ldap-bind-password", "bar", "the ldap password to bind the connection.")
}

func New(gc *goconfig.GoConfig, logger *zap.Logger) (api.AuthManager, error) {
	return &authManager{
		hostname:     gc.GetString("authmanager-plugin-ldap-hostname"),
		port:         gc.GetInt("authmanager-plugin-ldap-port"),
		baseDN:       gc.GetString("authmanager-plugin-ldap-base-dn"),
		filter:       gc.GetString("authmanager-plugin-ldap-base-filter"),
		bindUsername: gc.GetString("authmanager-plugin-ldap-bind-username"),
		bindPassword: gc.GetString("authmanager-plugin-ldap-bind-password"),
		logger:       logger}, nil
}

func (am *authManager) Authenticate(ctx context.Context, clientID, clientSecret string) (*api.User, error) {
	l, err := ldap.DialTLS("tcp", fmt.Sprintf("%s:%d", am.hostname, am.port), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return nil, err
	}
	defer l.Close()

	// First bind with a read only user
	err = l.Bind(am.bindUsername, am.bindPassword)
	if err != nil {
		fmt.Println("bind failed", err)
		return nil, err
	}

	// Search for the given clientID
	searchRequest := ldap.NewSearchRequest(
		am.baseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf(am.filter, clientID),
		[]string{"dn"},
		nil,
	)

	sr, err := l.Search(searchRequest)
	if err != nil {
		fmt.Println("search failed", fmt.Sprintf(am.filter, clientID))
		return nil, err
	}

	if len(sr.Entries) != 1 {
		return nil, api.NewError(api.UserNotFoundErrorCode)
	}

	userdn := sr.Entries[0].DN

	// Bind as the user to verify their password
	err = l.Bind(userdn, clientSecret)
	if err != nil {
		return nil, err
	}

	return &api.User{AccountId: clientID, Groups: []string{}}, nil
}
