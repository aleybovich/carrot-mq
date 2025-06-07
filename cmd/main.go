package main

import (
	"carrot-mq/internal"
	"os"
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
