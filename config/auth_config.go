package config

// AuthMode defines the authentication mode for the server
type AuthMode int

const (
	// AuthModeNone disables authentication
	AuthModeNone AuthMode = iota
	// AuthModePlain enables PLAIN SASL authentication
	AuthModePlain
)
