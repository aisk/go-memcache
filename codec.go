package memcache

import (
	"encoding/json"
	"fmt"
)

// Codec converts application values to and from cached bytes. Flags is an
// application-owned uint32 stored by memcached and returned with the value.
type Codec interface {
	Marshal(value any) (data []byte, flags uint32, err error)
	Unmarshal(data []byte, flags uint32, destination any) error
}

// JSONCodec encodes values as JSON. Its Flag is written as the client flags
// token and checked while decoding when non-zero.
type JSONCodec struct{ Flag uint32 }

func (c JSONCodec) Marshal(value any) ([]byte, uint32, error) {
	b, err := json.Marshal(value)
	return b, c.Flag, err
}

func (c JSONCodec) Unmarshal(data []byte, flags uint32, destination any) error {
	if c.Flag != 0 && flags != c.Flag {
		return fmt.Errorf("memcache: codec flag mismatch: got %d, want %d", flags, c.Flag)
	}
	return json.Unmarshal(data, destination)
}

// GetAs is the temporary generic counterpart to Client.GetInto.
//
// TODO: make this a generic Client method if Go adds generic methods.
func GetAs[T any](ctx Context, client *Client, key string) (T, error) {
	var value T
	err := client.GetInto(ctx, key, &value)
	return value, err
}
