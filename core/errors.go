package core

import "errors"

var (
	ErrNotFound      = errors.New("job not found")
	ErrNotRunning    = errors.New("job is not running")
	ErrManagerClosed = errors.New("manager is shut down")
	ErrEmptyCommand  = errors.New("command must not be empty")
	ErrEmptyOwner    = errors.New("owner must not be empty")
)
