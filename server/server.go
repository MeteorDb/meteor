package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"meteor/internal/commands"
	"meteor/internal/common"
	"meteor/internal/config"
	"meteor/internal/dbmanager"
	"meteor/internal/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func Init() {
	slog.SetDefault(logger.New())

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	dm, err := dbmanager.NewDBManager()
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		cancel()
		return
	}

	go handleShutdown(cancel)

	go runServer(dm, ctx, wg)

	wg.Wait()
	slog.Info("Server stopped")
}

func runServer(dm *dbmanager.DBManager, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ln, err := net.Listen("tcp", config.Config.Host+":"+config.Config.Port)

	if err != nil {
		slog.Error("Failed to listen", "error", err)
		return
	}
	defer ln.Close()
	slog.Info("Server started", "host", config.Config.Host, "port", config.Config.Port)
	listenForConnections(dm, ctx, ln)
}

func listenForConnections(dm *dbmanager.DBManager, ctx context.Context, listener net.Listener) {
	// When the context is cancelled, Close() the listener to unblock Accept()
	go func() {
		<-ctx.Done()
		slog.Info("Context cancelled, closing listener")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// If the context was cancelled, we expect an error from Close()
			if ctx.Err() != nil {
				slog.Info("Listener closed, stopping accept loop")
				return
			}
			// Otherwise, log and continue accepting
			slog.Error("Failed to accept connection", "error", err)
			continue
		}

		slog.Info("Accepted connection", "remoteAddr", conn.RemoteAddr().String())
		go handleConnection(dm, ctx, conn)
	}
}

func handleConnection(dm *dbmanager.DBManager, ctx context.Context, conn net.Conn) {
	defer conn.Close()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Closing connection", "remoteAddr", conn.RemoteAddr().String())
			time.Sleep(3 * time.Second)
			slog.Info("Connection closed", "remoteAddr", conn.RemoteAddr().String())
			return
		default:
			buffer := make([]byte, 4096)
			n, err := conn.Read(buffer)
			if err != nil {
				if err != io.EOF {
					slog.Error("Failed to read from connection", "error", err)
				}
				slog.Info("Connection closed", "remoteAddr", conn.RemoteAddr().String())
				return
			}

			cmd, err := dm.Parser.Parse(buffer[:n], &conn)
			if err != nil {
				slog.Error("Failed to parse command", "error", err)
				return
			}

			res, err := performOperation(dm, cmd)
			if err != nil {
				res = []byte(fmt.Sprintf("error: %s\n", err))
			}

			fmt.Println("Command parsed", "command", cmd)

			_, err = conn.Write(res)
			if err != nil {
				slog.Error("Failed to write to connection", "error", err)
				return
			}
		}
	}
}

func handleShutdown(contextCancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	slog.Info("Received shutdown signal")
	contextCancel()
}

func performOperation(dm *dbmanager.DBManager, cmd *common.Command) ([]byte, error) {
	spec, ok := commands.Get(cmd.Operation)
    if !ok {
        return nil, fmt.Errorf("unknown operation %q", cmd.Operation)
    }

    res, err := spec.Handler(dm, cmd)
	if err != nil {
        return nil, err
    }

    return append(res, '\n'), nil
}
