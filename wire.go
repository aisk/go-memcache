package memcache

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	maxWireKey  = 250
	maxLineSize = 64 * 1024
)

// MetaCommand is the raw escape hatch for protocol extensions. Command must
// be a supported two-byte meta command. Flags omit their separating spaces.
type MetaCommand struct {
	Command  string
	Key      string
	Flags    []string
	Value    []byte
	HasValue bool
}

func encodeKey(key string) (string, bool, error) {
	if key == "" {
		return "", false, fmt.Errorf("memcache: key must not be empty")
	}
	binary := false
	for i := 0; i < len(key); i++ {
		if key[i] <= 0x20 || key[i] == 0x7f {
			binary = true
			break
		}
	}
	wire := key
	if binary {
		wire = base64.StdEncoding.EncodeToString([]byte(key))
	}
	if len(wire) > maxWireKey {
		return "", false, fmt.Errorf("memcache: encoded key is %d bytes; maximum is %d", len(wire), maxWireKey)
	}
	return wire, binary, nil
}

func validateOpaque(token string) error {
	if token == "" {
		return nil
	}
	if len(token) > 32 {
		return fmt.Errorf("memcache: opaque token exceeds 32 bytes")
	}
	for i := range len(token) {
		if token[i] <= 0x20 || token[i] == 0x7f {
			return fmt.Errorf("memcache: opaque token contains whitespace or control bytes")
		}
	}
	return nil
}

func (m MetaCommand) marshal() ([]byte, error) {
	switch m.Command {
	case "mg", "ms", "md", "ma", "me":
	default:
		return nil, fmt.Errorf("memcache: unsupported meta command %q", m.Command)
	}
	key, binary, err := encodeKey(m.Key)
	if err != nil {
		return nil, err
	}
	flags := append([]string(nil), m.Flags...)
	if binary {
		if m.Command == "me" {
			return nil, fmt.Errorf("memcache: meta debug does not reliably support binary keys")
		}
		flags = append(flags, "b")
	}
	var b strings.Builder
	b.Grow(len(key) + len(m.Value) + 64)
	b.WriteString(m.Command)
	b.WriteByte(' ')
	b.WriteString(key)
	if m.HasValue {
		b.WriteByte(' ')
		b.WriteString(strconv.Itoa(len(m.Value)))
	}
	for _, flag := range flags {
		if flag == "" || strings.IndexFunc(flag, func(r rune) bool { return r <= ' ' || r == 0x7f }) >= 0 {
			return nil, fmt.Errorf("memcache: invalid meta flag %q", flag)
		}
		b.WriteByte(' ')
		b.WriteString(flag)
	}
	b.WriteString("\r\n")
	if m.HasValue {
		b.Write(m.Value)
		b.WriteString("\r\n")
	}
	return []byte(b.String()), nil
}

func readLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > maxLineSize {
			return nil, &ProtocolError{Message: "response line exceeds 64 KiB"}
		}
		if err == nil {
			break
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, &ProtocolError{Message: "response line is not CRLF terminated"}
	}
	return line[:len(line)-2], nil
}

func readRawResponse(reader *bufio.Reader, maxItemSize int) (RawResponse, error) {
	line, err := readLine(reader)
	if err != nil {
		return RawResponse{}, err
	}
	text := string(line)
	if text == "ERROR" {
		return RawResponse{}, &ServerError{Kind: "ERROR"}
	}
	if strings.HasPrefix(text, "CLIENT_ERROR") {
		return RawResponse{}, &ServerError{Kind: "CLIENT_ERROR", Message: strings.TrimSpace(strings.TrimPrefix(text, "CLIENT_ERROR"))}
	}
	if strings.HasPrefix(text, "SERVER_ERROR") {
		return RawResponse{}, &ServerError{Kind: "SERVER_ERROR", Message: strings.TrimSpace(strings.TrimPrefix(text, "SERVER_ERROR"))}
	}
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return RawResponse{}, &ProtocolError{Message: "empty response"}
	}
	code := ResponseCode(parts[0])
	result := RawResponse{Code: code}
	flagStart := 1
	switch code {
	case ResponseValue:
		if len(parts) < 2 {
			return RawResponse{}, &ProtocolError{Message: "VA response omitted value length"}
		}
		size, parseErr := strconv.ParseUint(parts[1], 10, 63)
		if parseErr != nil {
			return RawResponse{}, &ProtocolError{Message: "invalid VA value length"}
		}
		if maxItemSize > 0 && size > uint64(maxItemSize) {
			return RawResponse{}, &ProtocolError{Message: "response value exceeds configured maximum"}
		}
		result.Value = make([]byte, int(size))
		if _, err = io.ReadFull(reader, result.Value); err != nil {
			return RawResponse{}, err
		}
		var crlf [2]byte
		if _, err = io.ReadFull(reader, crlf[:]); err != nil {
			return RawResponse{}, err
		}
		if crlf != [2]byte{'\r', '\n'} {
			return RawResponse{}, &ProtocolError{Message: "value is not CRLF terminated"}
		}
		flagStart = 2
	case ResponseHeader, ResponseMiss, ResponseNotStore, ResponseExists, ResponseNotFound, ResponseNoop:
	case ResponseDebug:
		if len(parts) < 2 {
			return RawResponse{}, &ProtocolError{Message: "ME response omitted key"}
		}
		result.Debug = make(map[string]string)
		for _, field := range parts[2:] {
			name, value, ok := strings.Cut(field, "=")
			if ok {
				result.Debug[name] = value
			}
		}
		return result, nil
	default:
		return RawResponse{}, &ProtocolError{Message: fmt.Sprintf("unexpected response code %q", code)}
	}
	result.Flags = append([]string(nil), parts[flagStart:]...)
	if err := parseResponseFlags(&result); err != nil {
		return RawResponse{}, err
	}
	return result, nil
}

func parseResponseFlags(result *RawResponse) error {
	base64Key := false
	for _, flag := range result.Flags {
		if flag == "" {
			continue
		}
		code, token := flag[0], flag[1:]
		parseUint := func(bits int) (uint64, error) {
			v, err := strconv.ParseUint(token, 10, bits)
			if err != nil {
				return 0, &ProtocolError{Message: fmt.Sprintf("invalid %c response flag", code)}
			}
			return v, nil
		}
		switch code {
		case 'c':
			v, e := parseUint(64)
			if e != nil {
				return e
			}
			result.Metadata.CAS = ptr(v)
		case 't':
			v, e := strconv.ParseInt(token, 10, 64)
			if e != nil {
				return &ProtocolError{Message: "invalid t response flag"}
			}
			result.Metadata.TTL = ptr(v)
		case 's':
			v, e := parseUint(64)
			if e != nil {
				return e
			}
			result.Metadata.Size = ptr(v)
		case 'f':
			v, e := parseUint(32)
			if e != nil {
				return e
			}
			n := uint32(v)
			result.Metadata.ClientFlags = &n
		case 'l':
			v, e := parseUint(64)
			if e != nil {
				return e
			}
			result.Metadata.LastAccess = ptr(v)
		case 'h':
			if token != "0" && token != "1" {
				return &ProtocolError{Message: "invalid h response flag"}
			}
			v := token == "1"
			result.Metadata.HitBefore = &v
		case 'k':
			result.Key = []byte(token)
		case 'O':
			result.Opaque = token
		case 'W':
			result.Won = true
		case 'Z':
			result.Busy = true
		case 'X':
			result.Stale = true
		case 'b':
			base64Key = true
		}
	}
	if base64Key && result.Key != nil {
		decoded, err := base64.StdEncoding.DecodeString(string(result.Key))
		if err != nil {
			return &ProtocolError{Message: "invalid base64 response key"}
		}
		result.Key = decoded
	}
	return nil
}
