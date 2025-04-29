package server

import (
	"context"
	"log/slog"
	"meteor/internal/config"
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

	go handleShutdown(cancel)

	go runServer(ctx, wg)

	wg.Wait()
	slog.Info("Server stopped")
}

func runServer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ln, err := net.Listen("tcp", config.Config.Host+":"+config.Config.Port)
	if err != nil {
		slog.Error("Failed to listen", "error", err)
		return
	}
	defer ln.Close()
	slog.Info("Server started", "host", config.Config.Host, "port", config.Config.Port)
	listenForConnections(ctx, ln, wg)
}

func listenForConnections(ctx context.Context, listener net.Listener, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			slog.Info("No longer accepting connections")
			return
		default:
			slog.Info("Waiting for connection")
			conn, err := listener.Accept()
			if err != nil {
				slog.Error("Failed to accept connection", "error", err)
			}
			slog.Info("Accepted connection", "remoteAddr", conn.RemoteAddr().String())
			go handleConnection(ctx, conn, wg)
		}
	}
}

func handleConnection(ctx context.Context, conn net.Conn, wg *sync.WaitGroup) {
	wg.Add(1)
	defer conn.Close()
	defer wg.Done()

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
				slog.Error("Failed to read from connection", "error", err)
				return
			}

			response := append([]byte("response from server: "), buffer[:n]...)

			_, err = conn.Write(response)
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
