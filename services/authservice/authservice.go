package authservice

import (
	"context"
	"io"

	"github.com/cernbox/cs3apis/gen/proto/go/cs3/auth/v1"
	"github.com/cernbox/cs3apis/gen/proto/go/cs3/rpc"
	"github.com/cernbox/reva/pkg/auth"
	"github.com/cernbox/reva/pkg/logger"
	"github.com/cernbox/reva/pkg/token"
	"github.com/cernbox/reva/pkg/user"

	"github.com/pkg/errors"
)

func New(authmgr auth.Manager, tokenmgr token.Manager, usermgr user.Manager, logOut io.Writer, logKey interface{}) authv1pb.AuthServiceServer {
	logger := logger.New(logOut, "authservice", logKey)
	return &service{
		authmgr:  authmgr,
		tokenmgr: tokenmgr,
		usermgr:  usermgr,
		logger:   logger,
	}
}

type service struct {
	authmgr  auth.Manager
	tokenmgr token.Manager
	usermgr  user.Manager
	logger   *logger.Logger
}

func (s *service) GenerateAccessToken(ctx context.Context, req *authv1pb.GenerateAccessTokenRequest) (*authv1pb.GenerateAccessTokenResponse, error) {
	username := req.GetUsername()
	password := req.GetPassword()

	err := s.authmgr.Authenticate(ctx, username, password)
	if err != nil {
		err = errors.Wrap(err, "authservice: error authenticating user")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_UNAUTHENTICATED}
		res := &authv1pb.GenerateAccessTokenResponse{Status: status}
		return res, nil
	}

	user, err := s.usermgr.GetUser(ctx, username)
	if err != nil {
		err = errors.Wrap(err, "authservice: error getting user information")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_UNAUTHENTICATED}
		res := &authv1pb.GenerateAccessTokenResponse{Status: status}
		return res, nil
	}

	claims := token.Claims{
		"username":     user.Username,
		"groups":       user.Groups,
		"mail":         user.Mail,
		"display_name": user.DisplayName,
	}

	accessToken, err := s.tokenmgr.ForgeToken(ctx, claims)
	if err != nil {
		err = errors.Wrap(err, "authservice: error creating access token")
		s.logger.Error(ctx, err)
		status := &rpcpb.Status{Code: rpcpb.Code_CODE_UNAUTHENTICATED}
		res := &authv1pb.GenerateAccessTokenResponse{Status: status}
		return res, nil
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &authv1pb.GenerateAccessTokenResponse{Status: status, AccessToken: accessToken}
	return res, nil

}

func (s *service) WhoAmI(ctx context.Context, req *authv1pb.WhoAmIRequest) (*authv1pb.WhoAmIResponse, error) {
	u := user.ContextMustGetUser(ctx)
	user := &authv1pb.User{
		Username:    u.Username,
		DisplayName: u.DisplayName,
		Groups:      u.Groups,
		Mail:        u.Mail,
	}

	status := &rpcpb.Status{Code: rpcpb.Code_CODE_OK}
	res := &authv1pb.WhoAmIResponse{Status: status, User: user}
	return res, nil
}

/*
func (s *service) ForgeUserToken(ctx context.Context, req *api.ForgeUserTokenReq) (*api.TokenResponse, error) {
	l := ctx_zap.Extract(ctx)
	user, err := s.authmgr.Authenticate(ctx, req.ClientId, req.ClientSecret)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}

	token, err := s.tokenmgr.ForgeUserToken(ctx, user)
	if err != nil {
		l.Error("", zap.Error(err))
		return nil, err
	}
	tokenResponse := &api.TokenResponse{Token: token}
	return tokenResponse, nil
}

func (s *service) DismantleUserToken(ctx context.Context, req *api.TokenReq) (*api.UserResponse, error) {
	l := ctx_zap.Extract(ctx)
	token := req.Token
	u, err := s.tokenmgr.DismantleUserToken(ctx, token)
	if err != nil {
		l.Warn("token invalid", zap.Error(err))
		res := &api.UserResponse{Status: api.StatusCode_TOKEN_INVALID}
		return res, nil
		//return nil, api.NewError(api.TokenInvalidErrorCode).WithMessage(err.Error())
	}
	userRes := &api.UserResponse{User: u}
	return userRes, nil
}

func (s *service) ForgePublicLinkToken(ctx context.Context, req *api.ForgePublicLinkTokenReq) (*api.TokenResponse, error) {
	l := ctx_zap.Extract(ctx)
	pl, err := s.lm.AuthenticatePublicLink(ctx, req.Token, req.Password)
	if err != nil {
		if api.IsErrorCode(err, api.PublicLinkInvalidPasswordErrorCode) {
			return &api.TokenResponse{Status: api.StatusCode_PUBLIC_LINK_INVALID_PASSWORD}, nil
		}
		l.Error("", zap.Error(err))
		return nil, err
	}

	token, err := s.tokenmgr.ForgePublicLinkToken(ctx, pl)
	if err != nil {
		l.Warn("", zap.Error(err))
		return nil, err
	}
	tokenResponse := &api.TokenResponse{Token: token}
	return tokenResponse, nil
}

func (s *service) DismantlePublicLinkToken(ctx context.Context, req *api.TokenReq) (*api.PublicLinkResponse, error) {
	l := ctx_zap.Extract(ctx)
	token := req.Token
	u, err := s.tokenmgr.DismantlePublicLinkToken(ctx, token)
	if err != nil {
		l.Error("token invalid", zap.Error(err))
		return nil, api.NewError(api.TokenInvalidErrorCode).WithMessage(err.Error())
	}
	userRes := &api.PublicLinkResponse{PublicLink: u}
	return userRes, nil
}

// Override the Auth function to avoid checking the bearer token for this service
// https://github.com/grpc-ecosystem/go-grpc-middleware/tree/master/auth#type-serviceauthfuncoverride
func (s *service) AuthFuncOverride(ctx context.Context, fullMethodNauthmgre string) (context.Context, error) {
	return ctx, nil
}
*/
