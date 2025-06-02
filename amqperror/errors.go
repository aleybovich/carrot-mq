package amqpError

// AmqpError represents AMQP protocol error codes
type AmqpError uint16

// AMQP error code constants
const (
	// NoRoute - Used when mandatory messages cannot be routed
	NoRoute AmqpError = 312

	// ConnectionForced - Used when VHost is deleted
	ConnectionForced AmqpError = 320

	// AccessRefused - Used for vhost access denied, passive declare of server-named queue
	AccessRefused AmqpError = 403

	// NotFound - Used for missing exchanges, queues, bindings, consumers
	NotFound AmqpError = 404

	// ResourceLocked - Used when exclusive queue is accessed by another consumer
	ResourceLocked AmqpError = 405

	// PreconditionFailed - Used for queue property mismatches, unknown delivery tags, if-unused/if-empty conditions, tx operations on non-tx channels
	PreconditionFailed AmqpError = 406

	// InternalError - Used for internal server errors
	InternalError AmqpError = 500

	// FrameError - Used for frame parsing errors
	FrameError AmqpError = 501

	// SyntaxError - Used for malformed method arguments
	SyntaxError AmqpError = 502

	// CommandInvalid - Used for invalid commands (e.g., non-Connection class on channel 0)
	CommandInvalid AmqpError = 503

	// ChannelError - Used for channel-related errors
	ChannelError AmqpError = 504

	// UnexpectedFrame - Used for frames received in wrong order
	UnexpectedFrame AmqpError = 505

	// ResourceError - Used for resource limits (tx memory/operation limits)
	ResourceError AmqpError = 506

	// NotAllowed - Used for duplicate consumer tags
	NotAllowed AmqpError = 530

	// NotImplemented - Used for unimplemented methods
	NotImplemented AmqpError = 540
)

func (e AmqpError) Code() uint16 {
	// Return the error code as a uint16
	return uint16(e)
}

// String returns the error string representation of the AmqpError
func (e AmqpError) String() string {
	switch e {
	case NoRoute:
		return "NO_ROUTE"
	case ConnectionForced:
		return "CONNECTION_FORCED"
	case AccessRefused:
		return "ACCESS_REFUSED"
	case NotFound:
		return "NOT_FOUND"
	case ResourceLocked:
		return "RESOURCE_LOCKED"
	case PreconditionFailed:
		return "PRECONDITION_FAILED"
	case InternalError:
		return "INTERNAL_ERROR"
	case FrameError:
		return "FRAME_ERROR"
	case SyntaxError:
		return "SYNTAX_ERROR"
	case CommandInvalid:
		return "COMMAND_INVALID"
	case ChannelError:
		return "CHANNEL_ERROR"
	case UnexpectedFrame:
		return "UNEXPECTED_FRAME"
	case ResourceError:
		return "RESOURCE_ERROR"
	case NotAllowed:
		return "NOT_ALLOWED"
	case NotImplemented:
		return "NOT_IMPLEMENTED"
	default:
		return "UNKNOWN_ERROR"
	}
}
