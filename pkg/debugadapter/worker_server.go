package debugadapter

import (
	"context"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/debugadapter"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// WorkerServer implements the DebugAdapterWorker gRPC service. It
// handles session registration and bidirectional message relay from the
// worker's perspective.
type WorkerServer struct {
	debugadapter.UnimplementedDebugAdapterWorkerServer

	store             *SessionStore
	heartbeatInterval time.Duration
}

// NewWorkerServer creates a new worker-facing gRPC server.
func NewWorkerServer(store *SessionStore, heartbeatInterval time.Duration) *WorkerServer {
	return &WorkerServer{
		store:             store,
		heartbeatInterval: heartbeatInterval,
	}
}

func (s *WorkerServer) RegisterSession(ctx context.Context, req *debugadapter.RegisterSessionRequest) (*emptypb.Empty, error) {
	if req.InvocationId == "" {
		return nil, status.Error(codes.InvalidArgument, "Invocation ID is required")
	}
	if !s.store.Register(req.InvocationId) {
		return nil, status.Errorf(codes.AlreadyExists, "Session already exists for invocation %q", req.InvocationId)
	}
	return &emptypb.Empty{}, nil
}

func (s *WorkerServer) SendMessages(ctx context.Context, req *debugadapter.SendMessagesRequest) (*emptypb.Empty, error) {
	sess := s.store.Get(req.InvocationId)
	if sess == nil {
		return nil, status.Errorf(codes.NotFound, "No session for invocation %q", req.InvocationId)
	}
	sess.appendWorkerToClient(req.Messages)
	return &emptypb.Empty{}, nil
}

func (s *WorkerServer) ReceiveMessages(req *debugadapter.ReceiveMessagesRequest, stream debugadapter.DebugAdapterWorker_ReceiveMessagesServer) error {
	sess := s.store.Get(req.InvocationId)
	if sess == nil {
		return status.Errorf(codes.NotFound, "No session for invocation %q", req.InvocationId)
	}

	lastSeq := req.LastReceivedSequence
	heartbeat := time.NewTicker(s.heartbeatInterval)
	defer heartbeat.Stop()

	// First, replay any buffered messages.
	if msgs := sess.getClientToWorkerAfter(lastSeq); len(msgs) > 0 {
		for _, msg := range msgs {
			if err := stream.Send(&debugadapter.ReceiveMessagesResponse{
				Payload:  msg.payload,
				Sequence: msg.sequence,
			}); err != nil {
				return err
			}
			lastSeq = msg.sequence
		}
	}

	for {
		waitCh := sess.workerWaitChan()

		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-heartbeat.C:
			if err := stream.Send(&debugadapter.ReceiveMessagesResponse{}); err != nil {
				return err
			}
		case <-waitCh:
			if sess.isEnded() {
				return nil
			}
			msgs := sess.getClientToWorkerAfter(lastSeq)
			for _, msg := range msgs {
				if err := stream.Send(&debugadapter.ReceiveMessagesResponse{
					Payload:  msg.payload,
					Sequence: msg.sequence,
				}); err != nil {
					return err
				}
				lastSeq = msg.sequence
			}
		}
	}
}
