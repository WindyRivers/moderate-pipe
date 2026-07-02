package user

import (
	"context"
	"errors"

	userpb "github.com/WindyRivers/moderate-pipe/proto/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the generated userpb.UserServiceServer.
type Server struct {
	userpb.UnimplementedUserServiceServer
	repo *Repo
}

func NewServer(repo *Repo) *Server { return &Server{repo: repo} }

// GetUserReputation returns the poster's reputation and violation count.
func (s *Server) GetUserReputation(ctx context.Context, req *userpb.GetUserReputationRequest) (*userpb.GetUserReputationResponse, error) {
	u, err := s.repo.Get(ctx, uint(req.GetUserId()))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "user %d not found", req.GetUserId())
		}
		return nil, status.Errorf(codes.Internal, "lookup failed: %v", err)
	}
	return &userpb.GetUserReputationResponse{
		UserId:         uint64(u.ID),
		Reputation:     int32(u.Reputation),
		ViolationCount: int32(u.ViolationCount),
	}, nil
}

// GetUserProfile returns basic profile fields.
func (s *Server) GetUserProfile(ctx context.Context, req *userpb.GetUserProfileRequest) (*userpb.GetUserProfileResponse, error) {
	u, err := s.repo.Get(ctx, uint(req.GetUserId()))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "user %d not found", req.GetUserId())
		}
		return nil, status.Errorf(codes.Internal, "lookup failed: %v", err)
	}
	return &userpb.GetUserProfileResponse{
		UserId:     uint64(u.ID),
		Username:   u.Username,
		Email:      u.Email,
		Reputation: int32(u.Reputation),
	}, nil
}
