package objectgrpc

import (
	"context"
	"errors"
	"fluxforge-control/internal/domain"
	"fluxforge-control/internal/objects"
	pb "fluxforge-control/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

type Server struct {
	pb.UnimplementedObjectServiceServer
	Service  *objects.Service
	Internal bool
}

func objectPB(r domain.ObjectRef) *pb.ObjectRef {
	return &pb.ObjectRef{ObjectId: r.ID, SizeBytes: r.Size, Sha256: r.SHA256, ContentType: r.ContentType}
}
func objectError(e error) error {
	if e == nil {
		return nil
	}
	code := codes.Unavailable
	var d *domain.Error
	if errors.As(e, &d) {
		switch d.Code {
		case domain.Invalid:
			code = codes.InvalidArgument
		case domain.NotFound:
			code = codes.NotFound
		case domain.Conflict:
			code = codes.FailedPrecondition
		}
	}
	return status.Error(code, e.Error())
}
func (s *Server) BeginUpload(ctx context.Context, r *pb.BeginUploadRequest) (*pb.BeginUploadResponse, error) {
	u, grant, e := s.Service.Begin(ctx, r.Purpose, r.SizeBytes, r.Sha256, r.ContentType, s.Internal)
	if e != nil {
		return nil, objectError(e)
	}
	return &pb.BeginUploadResponse{UploadId: u.ID, Url: grant, ExpiresUnixMs: time.Now().Add(15 * time.Minute).UnixMilli(), Headers: map[string]string{"Content-Type": u.Ref.ContentType}}, nil
}
func (s *Server) CommitUpload(ctx context.Context, r *pb.CommitUploadRequest) (*pb.CommitUploadResponse, error) {
	o, e := s.Service.Commit(ctx, r.UploadId)
	if e != nil {
		return nil, objectError(e)
	}
	return &pb.CommitUploadResponse{Object: objectPB(o)}, nil
}
func (s *Server) GetDownloadURL(ctx context.Context, r *pb.GetDownloadURLRequest) (*pb.GetDownloadURLResponse, error) {
	o, u, e := s.Service.Download(ctx, r.ObjectId, s.Internal)
	if e != nil {
		return nil, objectError(e)
	}
	return &pb.GetDownloadURLResponse{Object: objectPB(o), Url: u, ExpiresUnixMs: time.Now().Add(15 * time.Minute).UnixMilli()}, nil
}
