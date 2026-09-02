package memcache

import (
	"encoding/json"
	"fmt"
)

// Codec converts values to and from the bytes stored in memcached. It is
// consulted by the object verbs (Get, GetMany, Set, SetMany, Add, Replace,
// Fetch, Update) whenever the value type is anything other than []byte. A
// []byte value always passes through untouched, so raw bytes remain reachable
// regardless of the codec. Implementations must be safe for concurrent use
// and must never produce an empty encoding, since memcached reserves zero
// byte items for lease placeholders.
type Codec interface {
	Marshal(value any) ([]byte, error)
	Unmarshal(data []byte, value any) error
}

// JSONCodec encodes values with encoding/json. It is the default codec.
type JSONCodec struct{}

func (JSONCodec) Marshal(value any) ([]byte, error)      { return json.Marshal(value) }
func (JSONCodec) Unmarshal(data []byte, value any) error { return json.Unmarshal(data, value) }

type codecOption struct{ codec Codec }

func (o codecOption) applyOption(c *config) error {
	if o.codec == nil {
		return fmt.Errorf("memcache: nil codec")
	}
	c.codec = o.codec
	return nil
}
func (o codecOption) policyOption() {}

// WithCodec replaces the codec used by the object verbs. The default is
// JSONCodec. The counter and raw bytes verbs (Incr, Decr, Append, Prepend,
// Take) never consult it.
func WithCodec(codec Codec) PolicyOption { return codecOption{codec: codec} }

// encode turns a value into its stored bytes. []byte is the identity case.
func (c *Client) encode(value any) ([]byte, error) {
	if raw, ok := value.([]byte); ok {
		return raw, nil
	}
	data, err := c.config.codec.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("memcache: encoding value: %w", err)
	}
	return data, nil
}

// decode turns stored bytes back into a value. []byte is the identity case.
func decode[T any](c *Client, data []byte) (T, error) {
	var value T
	if raw, ok := any(&value).(*[]byte); ok {
		*raw = data
		return value, nil
	}
	if err := c.config.codec.Unmarshal(data, &value); err != nil {
		var zero T
		return zero, fmt.Errorf("memcache: decoding value: %w", err)
	}
	return value, nil
}
