package main

import (
	"os"

	"github.com/aleybovich/carrot-mq/internal"
)

func main() {
	server := internal.NewServer()
	logger := server.Logger()
	logger.Info("Starting AMQP server")
	if err := server.Start(":5672"); err != nil {
		logger.Err("Failed to start server: %v", err)
		os.Exit(1)
	}
}
