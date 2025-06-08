package main

import (
	"meteor/internal/config"
	"meteor/server"
)

func main() {
	config.LoadConfig()
	server.Init()
}
