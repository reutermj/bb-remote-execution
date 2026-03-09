package debugadapter

import (
	"context"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/debugadapter"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ClientServer implements the DebugAdapterClient gRPC service. It
// handles bidirectional message relay from the DAP client's perspective
// (e.g., VS Code extension).
type ClientServer struct {
	debugadapter.UnimplementedDebugAdapterClientServer

	store             *SessionStore
	heartbeatInterval time.Duration
}

// NewClientServer creates a new client-facing gRPC server.
func NewClientServer(store *SessionStore, heartbeatInterval time.Duration) *ClientServer {
	return &ClientServer{
		store:             store,
		heartbeatInterval: heartbeatInterval,
	}
}

func (s *ClientServer) SendMessages(ctx context.Context, req *debugadapter.SendMessagesRequest) (*emptypb.Empty, error) {
	sess := s.store.Get(req.InvocationId)
	if sess == nil {
		return nil, status.Errorf(codes.NotFound, "No session for invocation %q", req.InvocationId)
	}
	sess.appendClientToWorker(req.Messages)
	return &emptypb.Empty{}, nil
}

func (s *ClientServer) ReceiveMessages(req *debugadapter.ReceiveMessagesRequest, stream debugadapter.DebugAdapterClient_ReceiveMessagesServer) error {
	sess := s.store.Get(req.InvocationId)
	if sess == nil {
		return status.Errorf(codes.NotFound, "No session for invocation %q", req.InvocationId)
	}

	lastSeq := req.LastReceivedSequence
	heartbeat := time.NewTicker(s.heartbeatInterval)
	defer heartbeat.Stop()

	// First, replay any buffered messages.
	if msgs := sess.getWorkerToClientAfter(lastSeq); len(msgs) > 0 {
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
		waitCh := sess.clientWaitChan()

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
			msgs := sess.getWorkerToClientAfter(lastSeq)
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

func (s *ClientServer) EndSession(ctx context.Context, req *debugadapter.EndSessionRequest) (*emptypb.Empty, error) {
	sess := s.store.Get(req.InvocationId)
	if sess == nil {
		return nil, status.Errorf(codes.NotFound, "No session for invocation %q", req.InvocationId)
	}
	s.store.Remove(req.InvocationId)
	return &emptypb.Empty{}, nil
}
