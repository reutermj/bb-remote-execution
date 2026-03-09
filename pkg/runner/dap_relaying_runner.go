//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package runner

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/debugadapter"
	runner_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type dapRelayingRunner struct {
	base                         runner_pb.RunnerServer
	debugAdapter                 debugadapter.DebugAdapterWorkerClient
	buildDirectory               filesystem.Directory
	buildDirectoryPath           *path.Builder
	commandCreator               CommandCreator
	setTmpdirEnvironmentVariable bool
}

// NewDAPRelayingRunner creates a Runner decorator that intercepts debug
// sessions. When a RunRequest includes a debug_invocation_id, the
// runner launches the process with stdin/stdout pipes and relays DAP
// messages bidirectionally through bb_debug_adapter. For non-debug
// requests, it delegates to the base runner unchanged.
func NewDAPRelayingRunner(
	base runner_pb.RunnerServer,
	debugAdapter debugadapter.DebugAdapterWorkerClient,
	buildDirectory filesystem.Directory,
	buildDirectoryPath *path.Builder,
	commandCreator CommandCreator,
	setTmpdirEnvironmentVariable bool,
) runner_pb.RunnerServer {
	return &dapRelayingRunner{
		base:                         base,
		debugAdapter:                 debugAdapter,
		buildDirectory:               buildDirectory,
		buildDirectoryPath:           buildDirectoryPath,
		commandCreator:               commandCreator,
		setTmpdirEnvironmentVariable: setTmpdirEnvironmentVariable,
	}
}

func (r *dapRelayingRunner) Run(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if request.DebugInvocationId == "" {
		return r.base.Run(ctx, request)
	}
	return r.runDebugSession(ctx, request)
}

func (r *dapRelayingRunner) runDebugSession(ctx context.Context, request *runner_pb.RunRequest) (*runner_pb.RunResponse, error) {
	if len(request.Arguments) < 1 {
		return nil, status.Error(codes.InvalidArgument, "Insufficient number of command arguments")
	}

	// Register the debug session with bb_debug_adapter.
	if _, err := r.debugAdapter.RegisterSession(ctx, &debugadapter.RegisterSessionRequest{
		InvocationId: request.DebugInvocationId,
	}); err != nil {
		return nil, util.StatusWrap(err, "Failed to register debug session")
	}

	// Resolve input root and create command.
	inputRootDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
	if err := path.Resolve(path.UNIXFormat.NewParser(request.InputRootDirectory), scopeWalker); err != nil {
		return nil, util.StatusWrap(err, "Failed to resolve input root directory")
	}

	cmd, err := r.commandCreator(ctx, request.Arguments, inputRootDirectory, path.UNIXFormat.NewParser(request.WorkingDirectory), request.EnvironmentVariables["PATH"])
	if err != nil {
		return nil, err
	}

	// Set environment variables.
	cmd.Env = make([]string, 0, len(request.EnvironmentVariables)+1)
	if r.setTmpdirEnvironmentVariable && request.TemporaryDirectory != "" {
		temporaryDirectory, scopeWalker := r.buildDirectoryPath.Join(path.VoidScopeWalker)
		if err := path.Resolve(path.UNIXFormat.NewParser(request.TemporaryDirectory), scopeWalker); err != nil {
			return nil, util.StatusWrap(err, "Failed to resolve temporary directory")
		}
		temporaryDirectoryStr, err := path.LocalFormat.GetString(temporaryDirectory)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create local representation of temporary directory")
		}
		for _, prefix := range temporaryDirectoryEnvironmentVariablePrefixes {
			cmd.Env = append(cmd.Env, prefix+temporaryDirectoryStr)
		}
	}
	for name, value := range request.EnvironmentVariables {
		cmd.Env = append(cmd.Env, name+"="+value)
	}

	// Set up pipes for DAP communication.
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create stdin pipe")
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create stdout pipe")
	}

	// Still write stderr to the log file for diagnostics.
	stderrResolver := buildDirectoryPathResolver{
		stack: util.NewNonEmptyStack(filesystem.NopDirectoryCloser(r.buildDirectory)),
	}
	defer stderrResolver.closeAll()
	if request.StderrPath != "" {
		if err := path.Resolve(path.UNIXFormat.NewParser(request.StderrPath), path.NewRelativeScopeWalker(&stderrResolver)); err == nil {
			if stderrResolver.TerminalName != nil {
				if stderr, err := stderrResolver.stack.Peek().OpenAppend(*stderrResolver.TerminalName, filesystem.CreateExcl(0o666)); err == nil {
					cmd.Stderr = stderr
					defer stderr.Close()
				}
			}
		}
	}

	// Start the process.
	if err := cmd.Start(); err != nil {
		code := codes.Internal
		for _, invalidArgumentErr := range invalidArgumentErrs {
			if errors.Is(err, invalidArgumentErr) {
				code = codes.InvalidArgument
				break
			}
		}
		return nil, util.StatusWrapWithCode(err, code, "Failed to start process")
	}

	// Relay DAP messages bidirectionally.
	var wg sync.WaitGroup
	relayCtx, relayCancel := context.WithCancel(ctx)
	defer relayCancel()

	// stdout -> bb_debug_adapter (SendMessages).
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.relayProcessToDebugAdapter(relayCtx, stdoutPipe, request.DebugInvocationId)
	}()

	// bb_debug_adapter -> stdin (ReceiveMessages).
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.relayDebugAdapterToProcess(relayCtx, stdinPipe, request.DebugInvocationId)
	}()

	// Wait for the process to exit.
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			relayCancel()
			wg.Wait()
			return nil, err
		}
	}

	// Process exited; cancel relay goroutines and wait.
	relayCancel()
	wg.Wait()

	posixResourceUsage, err := anypb.New(getPOSIXResourceUsage(cmd))
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to marshal POSIX resource usage")
	}
	return &runner_pb.RunResponse{
		ExitCode:      int64(cmd.ProcessState.ExitCode()),
		ResourceUsage: []*anypb.Any{posixResourceUsage},
	}, nil
}

// relayProcessToDebugAdapter reads DAP messages from the process's
// stdout and sends them to bb_debug_adapter via SendMessages.
//
// DAP uses a simple framing protocol on stdio:
//
//	Content-Length: <N>\r\n
//	\r\n
//	<N bytes of JSON>
func (r *dapRelayingRunner) relayProcessToDebugAdapter(ctx context.Context, stdout io.Reader, invocationID string) {
	reader := bufio.NewReader(stdout)
	for {
		if ctx.Err() != nil {
			return
		}

		// Read Content-Length header.
		contentLength, err := readDAPContentLength(reader)
		if err != nil {
			return
		}

		// Read the JSON payload.
		payload := make([]byte, contentLength)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return
		}

		// Send to bb_debug_adapter.
		if _, err := r.debugAdapter.SendMessages(ctx, &debugadapter.SendMessagesRequest{
			InvocationId: invocationID,
			Messages:     [][]byte{payload},
		}); err != nil {
			return
		}
	}
}

// relayDebugAdapterToProcess receives DAP messages from bb_debug_adapter
// and writes them to the process's stdin with Content-Length framing.
func (r *dapRelayingRunner) relayDebugAdapterToProcess(ctx context.Context, stdin io.WriteCloser, invocationID string) {
	defer stdin.Close()

	stream, err := r.debugAdapter.ReceiveMessages(ctx, &debugadapter.ReceiveMessagesRequest{
		InvocationId:         invocationID,
		LastReceivedSequence: 0,
	})
	if err != nil {
		return
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return
		}

		// Skip heartbeats (empty payload).
		if len(resp.Payload) == 0 {
			continue
		}

		// Write with DAP Content-Length framing.
		frame := fmt.Sprintf("Content-Length: %d\r\n\r\n", len(resp.Payload))
		if _, err := io.WriteString(stdin, frame); err != nil {
			return
		}
		if _, err := stdin.Write(resp.Payload); err != nil {
			return
		}
	}
}

func (r *dapRelayingRunner) CheckReadiness(ctx context.Context, request *runner_pb.CheckReadinessRequest) (*emptypb.Empty, error) {
	return r.base.CheckReadiness(ctx, request)
}

// readDAPContentLength reads the Content-Length header from a DAP
// message stream. The format is:
//
//	Content-Length: <N>\r\n
//	\r\n
func readDAPContentLength(reader *bufio.Reader) (int, error) {
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimRight(line, "\r\n")

		// Empty line marks end of headers.
		if line == "" {
			return 0, fmt.Errorf("missing Content-Length header")
		}

		if strings.HasPrefix(line, "Content-Length: ") {
			n, err := strconv.Atoi(strings.TrimPrefix(line, "Content-Length: "))
			if err != nil {
				return 0, fmt.Errorf("invalid Content-Length: %w", err)
			}

			// Consume the blank line after headers.
			separator, err := reader.ReadString('\n')
			if err != nil {
				return 0, err
			}
			if strings.TrimRight(separator, "\r\n") != "" {
				return 0, fmt.Errorf("expected blank line after headers")
			}

			return n, nil
		}
	}
}
