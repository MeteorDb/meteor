package server

import (
	"log/slog"
	"meteor/internal/logger"
)

func Init() {
	slog.SetDefault(logger.New())
}
