package memcache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// Client is a concurrent meta-protocol memcached client.
type Client struct {
	config  config
	servers []*serverPool
	close   sync.Once
}

// New creates a lazy client. No connection is opened until the first command.
func New(server string, options ...Option) (*Client, error) {
	cfg := defaultConfig(server)
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("memcache: nil option")
		}
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.network == "" {
		return nil, fmt.Errorf("memcache: network must not be empty")
	}
	if len(cfg.servers) == 0 {
		return nil, fmt.Errorf("memcache: at least one server is required")
	}
	client := &Client{config: cfg, servers: make([]*serverPool, len(cfg.servers))}
	for i, address := range cfg.servers {
		if address == "" {
			return nil, fmt.Errorf("memcache: server address %d is empty", i)
		}
		client.servers[i] = &serverPool{address: address, config: &client.config}
	}
	return client, nil
}

func (c *Client) server(key string) (*serverPool, error) {
	index := c.config.router.Pick(key, c.config.servers)
	if index < 0 || index >= len(c.servers) {
		return nil, fmt.Errorf("memcache: router returned invalid server index %d", index)
	}
	return c.servers[index], nil
}

// Close releases idle connections and prevents new work. In-flight requests
// are allowed to finish. Close is idempotent.
func (c *Client) Close() error {
	c.close.Do(func() {
		for _, server := range c.servers {
			server.close()
		}
	})
	return nil
}

// ExecuteMeta runs a raw command against the server selected by its key.
func (c *Client) ExecuteMeta(ctx context.Context, command MetaCommand) (RawResponse, error) {
	data, err := command.marshal()
	if err != nil {
		return RawResponse{}, err
	}
	if command.HasValue && c.config.maxItemSize > 0 && len(command.Value) > c.config.maxItemSize {
		return RawResponse{}, fmt.Errorf("memcache: value exceeds configured maximum of %d bytes", c.config.maxItemSize)
	}
	server, err := c.server(command.Key)
	if err != nil {
		return RawResponse{}, err
	}
	responses, written, err := server.exchange(ctx, data, func(RawResponse) bool { return true })
	if err != nil {
		var serverErr *ServerError
		if errors.As(err, &serverErr) {
			return RawResponse{}, err
		}
		if written > 0 && command.Command != "mg" && command.Command != "me" {
			return RawResponse{}, &AmbiguousWriteError{Operation: command.Command, Key: command.Key, Cause: err}
		}
		return RawResponse{}, err
	}
	return responses[0], nil
}

func appendOpaque(flags []string, opaque string) ([]string, error) {
	if err := validateOpaque(opaque); err != nil {
		return nil, err
	}
	if opaque != "" {
		flags = append(flags, "O"+opaque)
	}
	return flags, nil
}

func expirationFlag(prefix string, expiration *Expiration) string {
	return prefix + strconv.FormatInt(int64(*expiration), 10)
}

func buildGet(key string, options GetOptions) (MetaCommand, error) {
	if options.RefreshBefore != nil && options.VivifyTTL == nil {
		return MetaCommand{}, fmt.Errorf("memcache: RefreshBefore requires VivifyTTL")
	}
	if options.UnlessCAS != nil && options.MetadataOnly {
		return MetaCommand{}, fmt.Errorf("memcache: UnlessCAS requires a value read")
	}
	for name, ttl := range map[string]*Expiration{"VivifyTTL": options.VivifyTTL, "RefreshBefore": options.RefreshBefore} {
		if ttl != nil && *ttl <= 0 {
			return MetaCommand{}, fmt.Errorf("memcache: %s must be positive", name)
		}
	}
	flags := make([]string, 0, 14)
	if !options.MetadataOnly {
		flags = append(flags, "v")
	}
	if options.ReturnCAS || options.VivifyTTL != nil || options.UnlessCAS != nil {
		flags = append(flags, "c")
	}
	if options.ReturnClientFlags {
		flags = append(flags, "f")
	}
	if options.ReturnTTL {
		flags = append(flags, "t")
	}
	if options.ReturnSize {
		flags = append(flags, "s")
	}
	if options.ReturnLastAccess {
		flags = append(flags, "l")
	}
	if options.ReturnHitBefore {
		flags = append(flags, "h")
	}
	if options.ReturnKey {
		flags = append(flags, "k")
	}
	if options.NoLRUBump {
		flags = append(flags, "u")
	}
	if options.Touch != nil {
		flags = append(flags, expirationFlag("T", options.Touch))
	}
	if options.VivifyTTL != nil {
		flags = append(flags, expirationFlag("N", options.VivifyTTL))
	}
	if options.RefreshBefore != nil {
		flags = append(flags, expirationFlag("R", options.RefreshBefore))
	}
	if options.UnlessCAS != nil {
		flags = append(flags, "C"+strconv.FormatUint(*options.UnlessCAS, 10))
	}
	if options.SetCAS != nil {
		flags = append(flags, "E"+strconv.FormatUint(*options.SetCAS, 10))
	}
	var err error
	flags, err = appendOpaque(flags, options.Opaque)
	return MetaCommand{Command: "mg", Key: key, Flags: flags}, err
}

// Get reads a value and maps a miss to ErrCacheMiss.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	result, err := c.GetWithOptions(ctx, key, GetOptions{ReturnClientFlags: true})
	if err != nil {
		return nil, err
	}
	if result.Status != GetHit {
		return nil, ErrCacheMiss
	}
	return result.Value, nil
}

// GetWithOptions performs a semantic meta get.
func (c *Client) GetWithOptions(ctx context.Context, key string, options GetOptions) (GetResult, error) {
	command, err := buildGet(key, options)
	if err != nil {
		return GetResult{}, err
	}
	wire, err := c.ExecuteMeta(ctx, command)
	if err != nil {
		return GetResult{}, err
	}
	return semanticGet(key, options, wire)
}

func semanticGet(key string, options GetOptions, wire RawResponse) (GetResult, error) {
	result := GetResult{Key: key, Metadata: wire.Metadata, ValueState: ValueFresh, Lease: LeaseNone}
	switch wire.Code {
	case ResponseMiss:
		result.Status, result.ValueState = GetMiss, ValueMissing
	case ResponseValue, ResponseHeader:
		result.Status = GetHit
		if wire.Code == ResponseValue {
			result.Value = wire.Value
		}
		if wire.Code == ResponseHeader && options.UnlessCAS != nil {
			result.Status = GetUnchanged
		}
		if wire.Stale {
			result.ValueState = ValueStale
		}
		if wire.Won {
			result.Lease = LeaseGranted
		}
		if wire.Busy {
			result.Lease = LeaseBusy
		}
		if options.VivifyTTL != nil && wire.Code == ResponseValue && len(wire.Value) == 0 && (wire.Won || wire.Busy) && !wire.Stale {
			result.ValueState = ValueMissing
			if wire.Won {
				result.Status = GetMiss
			} else {
				result.Status = GetPending
			}
		}
	default:
		return GetResult{}, &ProtocolError{Message: fmt.Sprintf("unexpected get response %q", wire.Code)}
	}
	return result, nil
}

// Inspect returns metadata without fetching the value.
func (c *Client) Inspect(ctx context.Context, key string) (GetResult, error) {
	return c.GetWithOptions(ctx, key, GetOptions{MetadataOnly: true, ReturnCAS: true, ReturnTTL: true, ReturnSize: true, ReturnLastAccess: true, ReturnHitBefore: true})
}

func modeFlag(mode StoreMode) (string, error) {
	switch mode {
	case ModeSet:
		return "", nil
	case ModeAdd:
		return "ME", nil
	case ModeReplace:
		return "MR", nil
	case ModeAppend:
		return "MA", nil
	case ModePrepend:
		return "MP", nil
	default:
		return "", fmt.Errorf("memcache: invalid store mode %d", mode)
	}
}

func buildSet(key string, value []byte, options SetOptions) (MetaCommand, error) {
	mode, err := modeFlag(options.Mode)
	if err != nil {
		return MetaCommand{}, err
	}
	concatenate := options.Mode == ModeAppend || options.Mode == ModePrepend
	if concatenate && options.TTL != 0 {
		return MetaCommand{}, fmt.Errorf("memcache: TTL is ignored for append/prepend; use VivifyTTL")
	}
	if !concatenate && options.VivifyTTL != nil {
		return MetaCommand{}, fmt.Errorf("memcache: VivifyTTL is only valid for append/prepend")
	}
	if options.Mode == ModeAdd && options.CompareCAS != nil {
		return MetaCommand{}, fmt.Errorf("memcache: add cannot compare CAS")
	}
	if options.VivifyTTL != nil && *options.VivifyTTL <= 0 {
		return MetaCommand{}, fmt.Errorf("memcache: VivifyTTL must be positive")
	}
	flags := []string{"F" + strconv.FormatUint(uint64(options.ClientFlags), 10)}
	if options.TTL != 0 {
		flags = append(flags, "T"+strconv.FormatInt(int64(options.TTL), 10))
	}
	if mode != "" {
		flags = append(flags, mode)
	}
	if options.CompareCAS != nil {
		flags = append(flags, "C"+strconv.FormatUint(*options.CompareCAS, 10))
	}
	if options.SetCAS != nil {
		flags = append(flags, "E"+strconv.FormatUint(*options.SetCAS, 10))
	}
	if options.Invalidate {
		flags = append(flags, "I")
	}
	if options.VivifyTTL != nil {
		flags = append(flags, expirationFlag("N", options.VivifyTTL))
	}
	if options.ReturnCAS {
		flags = append(flags, "c")
	}
	if options.ReturnSize {
		flags = append(flags, "s")
	}
	if options.ReturnKey {
		flags = append(flags, "k")
	}
	flags, err = appendOpaque(flags, options.Opaque)
	return MetaCommand{Command: "ms", Key: key, Flags: flags, Value: value, HasValue: true}, err
}

// Set unconditionally stores bytes with the supplied expiration.
func (c *Client) Set(ctx context.Context, key string, value []byte, expiration Expiration) error {
	result, err := c.Store(ctx, key, value, SetOptions{TTL: expiration})
	if err != nil {
		return err
	}
	if !result.Applied() {
		return mutationError(result.Status)
	}
	return nil
}

// Store performs a configurable meta set.
func (c *Client) Store(ctx context.Context, key string, value []byte, options SetOptions) (MutationResult, error) {
	command, err := buildSet(key, value, options)
	if err != nil {
		return MutationResult{}, err
	}
	wire, err := c.ExecuteMeta(ctx, command)
	if err != nil {
		return MutationResult{}, err
	}
	status, err := mutationStatus(wire.Code, options.Mode)
	return MutationResult{Key: key, Status: status, CAS: wire.Metadata.CAS}, err
}

func mutationStatus(code ResponseCode, mode StoreMode) (MutationStatus, error) {
	switch code {
	case ResponseHeader, ResponseValue:
		return MutationApplied, nil
	case ResponseExists:
		return MutationCASMismatch, nil
	case ResponseNotFound:
		return MutationNotFound, nil
	case ResponseNotStore:
		if mode == ModeAdd {
			return MutationAlreadyExists, nil
		}
		return MutationNotFound, nil
	default:
		return MutationNotFound, &ProtocolError{Message: fmt.Sprintf("unexpected mutation response %q", code)}
	}
}

func mutationError(status MutationStatus) error {
	switch status {
	case MutationNotFound:
		return ErrCacheMiss
	case MutationCASMismatch:
		return ErrCASMismatch
	default:
		return ErrNotStored
	}
}

// Add stores only when key is absent.
func (c *Client) Add(ctx context.Context, key string, value []byte, expiration Expiration) (bool, error) {
	r, err := c.Store(ctx, key, value, SetOptions{TTL: expiration, Mode: ModeAdd})
	return err == nil && r.Applied(), err
}

// Replace stores only when key exists.
func (c *Client) Replace(ctx context.Context, key string, value []byte, expiration Expiration) (bool, error) {
	r, err := c.Store(ctx, key, value, SetOptions{TTL: expiration, Mode: ModeReplace})
	return err == nil && r.Applied(), err
}

// CompareAndSwap stores only if cas matches.
func (c *Client) CompareAndSwap(ctx context.Context, key string, value []byte, expiration Expiration, cas uint64) (bool, error) {
	r, err := c.Store(ctx, key, value, SetOptions{TTL: expiration, CompareCAS: &cas})
	return err == nil && r.Applied(), err
}

func buildDelete(key string, options DeleteOptions) (MetaCommand, error) {
	if options.StaleFor != nil && !options.Invalidate {
		return MetaCommand{}, fmt.Errorf("memcache: StaleFor requires Invalidate")
	}
	flags := make([]string, 0, 8)
	if options.CompareCAS != nil {
		flags = append(flags, "C"+strconv.FormatUint(*options.CompareCAS, 10))
	}
	if options.SetCAS != nil {
		flags = append(flags, "E"+strconv.FormatUint(*options.SetCAS, 10))
	}
	if options.Invalidate {
		flags = append(flags, "I")
	}
	if options.StaleFor != nil {
		flags = append(flags, expirationFlag("T", options.StaleFor))
	}
	if options.DropValue {
		flags = append(flags, "x")
	}
	if options.ReturnKey {
		flags = append(flags, "k")
	}
	var err error
	flags, err = appendOpaque(flags, options.Opaque)
	return MetaCommand{Command: "md", Key: key, Flags: flags}, err
}

// Delete removes a key. Missing keys are reported as (false, nil).
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	r, err := c.DeleteWithOptions(ctx, key, DeleteOptions{})
	return err == nil && r.Applied(), err
}

func (c *Client) DeleteWithOptions(ctx context.Context, key string, options DeleteOptions) (MutationResult, error) {
	command, err := buildDelete(key, options)
	if err != nil {
		return MutationResult{}, err
	}
	wire, err := c.ExecuteMeta(ctx, command)
	if err != nil {
		return MutationResult{}, err
	}
	status, err := mutationStatus(wire.Code, ModeSet)
	return MutationResult{Key: key, Status: status, CAS: wire.Metadata.CAS}, err
}

func buildArithmetic(key string, options ArithmeticOptions) (MetaCommand, error) {
	if (options.Initial == nil) != (options.InitialTTL == nil) {
		return MetaCommand{}, fmt.Errorf("memcache: Initial and InitialTTL must be supplied together")
	}
	if options.InitialTTL != nil && *options.InitialTTL <= 0 {
		return MetaCommand{}, fmt.Errorf("memcache: InitialTTL must be positive")
	}
	delta := options.Delta
	if delta == 0 {
		delta = 1
	}
	flags := []string{"D" + strconv.FormatUint(delta, 10)}
	if options.Decrement {
		flags = append(flags, "MD")
	}
	if options.Initial != nil {
		flags = append(flags, "J"+strconv.FormatUint(*options.Initial, 10), expirationFlag("N", options.InitialTTL))
	}
	if options.Touch != nil {
		flags = append(flags, expirationFlag("T", options.Touch))
	}
	if options.CompareCAS != nil {
		flags = append(flags, "C"+strconv.FormatUint(*options.CompareCAS, 10))
	}
	if options.SetCAS != nil {
		flags = append(flags, "E"+strconv.FormatUint(*options.SetCAS, 10))
	}
	if !options.MetadataOnly {
		flags = append(flags, "v")
	}
	if options.ReturnTTL {
		flags = append(flags, "t")
	}
	if options.ReturnCAS {
		flags = append(flags, "c")
	}
	if options.ReturnKey {
		flags = append(flags, "k")
	}
	var err error
	flags, err = appendOpaque(flags, options.Opaque)
	return MetaCommand{Command: "ma", Key: key, Flags: flags}, err
}

func (c *Client) Arithmetic(ctx context.Context, key string, options ArithmeticOptions) (ArithmeticResult, error) {
	command, err := buildArithmetic(key, options)
	if err != nil {
		return ArithmeticResult{}, err
	}
	wire, err := c.ExecuteMeta(ctx, command)
	if err != nil {
		return ArithmeticResult{}, err
	}
	return semanticArithmetic(key, wire)
}

func semanticArithmetic(key string, wire RawResponse) (ArithmeticResult, error) {
	status, err := mutationStatus(wire.Code, ModeSet)
	result := ArithmeticResult{Key: key, Status: status, Metadata: wire.Metadata}
	if wire.Code == ResponseValue {
		value, parseErr := strconv.ParseUint(string(wire.Value), 10, 64)
		if parseErr != nil {
			return ArithmeticResult{}, &ProtocolError{Message: "arithmetic value is not uint64"}
		}
		result.Value, result.HasValue = value, true
	}
	return result, err
}

func (c *Client) Increment(ctx context.Context, key string, delta uint64) (uint64, error) {
	r, err := c.Arithmetic(ctx, key, ArithmeticOptions{Delta: delta})
	if err != nil {
		return 0, err
	}
	if !r.HasValue {
		return 0, mutationError(r.Status)
	}
	return r.Value, nil
}

func (c *Client) Decrement(ctx context.Context, key string, delta uint64) (uint64, error) {
	r, err := c.Arithmetic(ctx, key, ArithmeticOptions{Delta: delta, Decrement: true})
	if err != nil {
		return 0, err
	}
	if !r.HasValue {
		return 0, mutationError(r.Status)
	}
	return r.Value, nil
}

// Touch updates a key's expiration without fetching its value.
func (c *Client) Touch(ctx context.Context, key string, expiration Expiration) (bool, error) {
	r, err := c.GetWithOptions(ctx, key, GetOptions{MetadataOnly: true, Touch: &expiration})
	return err == nil && r.Status == GetHit, err
}

// Debug returns the me command's internal key/value metadata.
func (c *Client) Debug(ctx context.Context, key string) (map[string]string, error) {
	wire, err := c.ExecuteMeta(ctx, MetaCommand{Command: "me", Key: key})
	if err != nil {
		return nil, err
	}
	if wire.Code == ResponseMiss {
		return nil, ErrCacheMiss
	}
	if wire.Code != ResponseDebug {
		return nil, &ProtocolError{Message: "unexpected debug response"}
	}
	return wire.Debug, nil
}

// GetInto decodes a cached value using the configured Codec.
func (c *Client) GetInto(ctx context.Context, key string, destination any) error {
	result, err := c.GetWithOptions(ctx, key, GetOptions{})
	if err != nil {
		return err
	}
	if result.Status != GetHit {
		return ErrCacheMiss
	}
	flags := uint32(0)
	if result.Metadata.ClientFlags != nil {
		flags = *result.Metadata.ClientFlags
	}
	return c.config.codec.Unmarshal(result.Value, flags, destination)
}

// SetValue encodes and stores an application value using the configured Codec.
func (c *Client) SetValue(ctx context.Context, key string, value any, expiration Expiration) error {
	data, flags, err := c.config.codec.Marshal(value)
	if err != nil {
		return err
	}
	result, err := c.Store(ctx, key, data, SetOptions{TTL: expiration, ClientFlags: flags})
	if err != nil {
		return err
	}
	if !result.Applied() {
		return mutationError(result.Status)
	}
	return nil
}

// Noop checks every configured backend with the meta no-op command.
func (c *Client) Noop(ctx context.Context) error {
	for _, server := range c.servers {
		responses, _, err := server.exchange(ctx, []byte("mn\r\n"), func(r RawResponse) bool { return r.Code == ResponseNoop })
		if err != nil {
			return err
		}
		if len(responses) != 1 || responses[0].Code != ResponseNoop {
			return &ProtocolError{Message: "unexpected no-op response"}
		}
	}
	return nil
}
