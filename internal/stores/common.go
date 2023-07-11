package stores

import "context"

var (
	ctx = context.Background()
)

const (
	LoggerTag = "stores"

	INTERNAL = "INTERNAL"
	CLIENT   = "CLIENT"
	SERVER   = "SERVER"
)
