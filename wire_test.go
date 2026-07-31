package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestMetaCommandMarshalBinaryKey(t *testing.T) {
	command := MetaCommand{Command: "ms", Key: "a b", Flags: []string{"T30"}, Value: []byte("x\r\ny"), HasValue: true}
	got, err := command.marshal()
	if err != nil {
		t.Fatal(err)
	}
	want := "ms YSBi 4 T30 b\r\nx\r\ny\r\n"
	if string(got) != want {
		t.Fatalf("wire:\n got %q\nwant %q", got, want)
	}
}

func TestReadRawResponse(t *testing.T) {
	reader := bufio.NewReader(bytes.NewBufferString("VA 4 c42 t-1 f7 h0 kYSBi b Oreq W X\r\na\r\nb\r\n"))
	response, err := readRawResponse(reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != ResponseValue || string(response.Value) != "a\r\nb" {
		t.Fatalf("unexpected response: %#v", response)
	}
	if response.Metadata.CAS == nil || *response.Metadata.CAS != 42 {
		t.Fatalf("CAS not parsed: %#v", response.Metadata)
	}
	if response.Metadata.TTL == nil || *response.Metadata.TTL != -1 {
		t.Fatalf("TTL not parsed: %#v", response.Metadata)
	}
	if response.Metadata.ClientFlags == nil || *response.Metadata.ClientFlags != 7 {
		t.Fatalf("flags not parsed: %#v", response.Metadata)
	}
	if !reflect.DeepEqual(response.Key, []byte("a b")) || response.Opaque != "req" || !response.Won || !response.Stale {
		t.Fatalf("flags not parsed: %#v", response)
	}
}

func TestReadRawResponseRejectsFraming(t *testing.T) {
	_, err := readRawResponse(bufio.NewReader(bytes.NewBufferString("HD\n")), 0)
	var protocol *ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("got %v, want ProtocolError", err)
	}
	_, err = readRawResponse(bufio.NewReader(bytes.NewBufferString("VA 3\r\nabcXX")), 0)
	if !errors.As(err, &protocol) {
		t.Fatalf("got %v, want ProtocolError", err)
	}
}

func TestBuildersValidateBeforeIO(t *testing.T) {
	ttl := Expiration(30)
	cas := uint64(1)
	tests := []struct {
		name string
		err  error
	}{
		{"refresh without lease", func() error { _, e := buildGet("k", GetOptions{RefreshBefore: &ttl}); return e }()},
		{"add with CAS", func() error { _, e := buildSet("k", nil, SetOptions{Mode: ModeAdd, CompareCAS: &cas}); return e }()},
		{"append TTL", func() error { _, e := buildSet("k", nil, SetOptions{Mode: ModeAppend, TTL: 3}); return e }()},
		{"stale TTL", func() error { _, e := buildDelete("k", DeleteOptions{StaleFor: &ttl}); return e }()},
		{"initial pair", func() error { _, e := buildArithmetic("k", ArithmeticOptions{Initial: &cas}); return e }()},
	}
	for _, test := range tests {
		if test.err == nil {
			t.Errorf("%s: expected error", test.name)
		}
	}
}

func TestExpirationHelpers(t *testing.T) {
	if got := ExpiresIn(time.Nanosecond); got != 1 {
		t.Fatalf("ExpiresIn: got %d", got)
	}
	if got := ExpiresIn(1500 * time.Millisecond); got != 2 {
		t.Fatalf("ExpiresIn rounds up: got %d", got)
	}
}
