package entities

import "errors"

var ErrStoreEntityNotFound = errors.New("store resource not found")
var ErrEmptyTick = errors.New("empty tick")
