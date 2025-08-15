package polardbx

import (
	"errors"
)

// Various errors the driver might return. Can change between driver versions.
var (
	ErrNoNodeFound     = errors.New("communications link failure: no available nodes meet the conditions")
	ErrInvalidPort     = errors.New("invalid port number")
	ErrClusterMismatch = errors.New("cluster id mismatch")
)

var (
	ErrInvalidDSNUnescaped = errors.New("invalid DSN: did you forget to escape a param value?")
	ErrInvalidDSNAddr      = errors.New("invalid DSN: network address not terminated (missing closing brace)")
	ErrInvalidDSNNoSlash   = errors.New("invalid DSN: missing the slash separating the database name")
)
