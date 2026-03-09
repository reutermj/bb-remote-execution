package main

import (
	"context"
	"os"
	"time"

	"github.com/buildbarn/bb-remote-execution/pkg/debugadapter"
	debugadapter_pb "github.com/buildbarn/bb-remote-execution/pkg/proto/debugadapter"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_debug_adapter"
	"github.com/buildbarn/bb-storage/pkg/global"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_debug_adapter bb_debug_adapter.jsonnet")
		}
		var configuration bb_debug_adapter.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}
		lifecycleState, grpcClientFactory, err := global.ApplyConfiguration(configuration.Global, dependenciesGroup)
		if err != nil {
			return util.StatusWrap(err, "Failed to apply global configuration options")
		}

		// Parse duration configurations.
		inactivityTimeout := 10 * time.Minute
		if configuration.InactivityTimeout != nil {
			if err := configuration.InactivityTimeout.CheckValid(); err != nil {
				return util.StatusWrap(err, "Invalid inactivity timeout")
			}
			inactivityTimeout = configuration.InactivityTimeout.AsDuration()
		}

		heartbeatInterval := 10 * time.Second
		if configuration.HeartbeatInterval != nil {
			if err := configuration.HeartbeatInterval.CheckValid(); err != nil {
				return util.StatusWrap(err, "Invalid heartbeat interval")
			}
			heartbeatInterval = configuration.HeartbeatInterval.AsDuration()
		}

		// Create the shared session store.
		store := debugadapter.NewSessionStore(inactivityTimeout)

		// Start a background goroutine to reap inactive sessions.
		go func() {
			ticker := time.NewTicker(inactivityTimeout / 2)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					store.ReapInactive()
				}
			}
		}()

		// Spawn client-facing gRPC servers.
		clientServer := debugadapter.NewClientServer(store, heartbeatInterval)
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.ClientGrpcServers,
			func(s grpc.ServiceRegistrar) {
				debugadapter_pb.RegisterDebugAdapterClientServer(s, clientServer)
			},
			siblingsGroup,
			grpcClientFactory,
		); err != nil {
			return util.StatusWrap(err, "Client gRPC server failure")
		}

		// Spawn worker-facing gRPC servers.
		workerServer := debugadapter.NewWorkerServer(store, heartbeatInterval)
		if err := bb_grpc.NewServersFromConfigurationAndServe(
			configuration.WorkerGrpcServers,
			func(s grpc.ServiceRegistrar) {
				debugadapter_pb.RegisterDebugAdapterWorkerServer(s, workerServer)
			},
			siblingsGroup,
			grpcClientFactory,
		); err != nil {
			return util.StatusWrap(err, "Worker gRPC server failure")
		}

		lifecycleState.MarkReadyAndWait(siblingsGroup)
		return nil
	})
}
