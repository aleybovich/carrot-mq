package main

import "errors"

// ANSI color codes for terminal output
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[37m"
	colorWhite  = "\033[97m"

	colorBoldRed    = "\033[1;31m"
	colorBoldGreen  = "\033[1;32m"
	colorBoldYellow = "\033[1;33m"
	colorBoldBlue   = "\033[1;34m"
)

const (
	FrameMethod    = 1
	FrameHeader    = 2
	FrameBody      = 3
	FrameHeartbeat = 8
	FrameEnd       = 206
)

const (
	ClassConnection = 10
	ClassChannel    = 20
	ClassExchange   = 40
	ClassQueue      = 50
	ClassBasic      = 60
	ClassConfirm    = 85
)

const (
	MethodConfirmSelect   = 10
	MethodConfirmSelectOk = 11

	MethodConnectionStart   = 10
	MethodConnectionStartOk = 11
	MethodConnectionTune    = 30
	MethodConnectionTuneOk  = 31
	MethodConnectionOpen    = 40
	MethodConnectionOpenOk  = 41
	MethodConnectionClose   = 50
	MethodConnectionCloseOk = 51

	MethodChannelOpen    = 10
	MethodChannelOpenOk  = 11
	MethodChannelClose   = 40
	MethodChannelCloseOk = 41

	MethodExchangeDeclare   = 10
	MethodExchangeDeclareOk = 11

	MethodQueueDeclare   = 10
	MethodQueueDeclareOk = 11
	MethodQueueBind      = 20
	MethodQueueBindOk    = 21
	MethodQueuePurge     = 30
	MethodQueuePurgeOk   = 31
	MethodQueueDelete    = 40
	MethodQueueDeleteOk  = 41

	MethodBasicConsume   = 20
	MethodBasicConsumeOk = 21
	MethodBasicCancel    = 30
	MethodBasicCancelOk  = 31
	MethodBasicPublish   = 40
	MethodBasicReturn    = 50
	MethodBasicDeliver   = 60
	MethodBasicGet       = 70
	MethodBasicGetOk     = 71
	MethodBasicGetEmpty  = 72
	MethodBasicAck       = 80
	MethodBasicReject    = 90
	MethodBasicRecover   = 110
	MethodBasicRecoverOk = 111
	MethodBasicNack      = 120
)

var (
	errConnectionClosedGracefully = errors.New("connection closed gracefully by AMQP protocol")
	// NEW: Indicates the server initiated a Channel.Close and it was sent successfully.
	// The connection itself might still be viable for other channels.
	errChannelClosedByServer = errors.New("channel closed by server via AMQP Channel.Close")
	// NEW: Indicates the server initiated a Connection.Close and it was sent successfully.
	// The server is now waiting for Connection.Close-Ok from the client.
	errConnectionCloseSentByServer = errors.New("connection.close sent by server, awaiting close-ok")
)
