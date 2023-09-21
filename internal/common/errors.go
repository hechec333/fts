package common

import "errors"

var (
	ErrDuplicateload = errors.New("file has already load")
)
