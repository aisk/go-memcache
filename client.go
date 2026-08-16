package memcache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// Client is a concurrent meta-protocol memcached client. Its methods are the
// scenario layer: one verb per user scenario, returning business values. The
// 1:1 protocol layer stays available behind Meta.
type Client struct {
	config        config
	servers       []*serverPool
	serverIndexes map[string]int
	close         sync.Once

	// rootCtx owns every background goroutine the client spawns; Close
	// cancels it.
	rootCtx    context.Context
	cancelRoot context.CancelFunc

	// fetchMu guards the in-process Fetch coordination state.
	fetchMu      sync.Mutex
	fetchFlights map[string]*fetchFlight
	refreshing   map[string]struct{}
}

// New creates a lazy client. No connection is opened until the first command.
func New(server string, options ...Option) (*Client, error) {
	cfg := defaultConfig(server)
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("memcache: nil option")
		}
		if err := option.applyOption(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.network == "" {
		return nil, fmt.Errorf("memcache: network must not be empty")
	}
	if len(cfg.servers) == 0 {
		return nil, fmt.Errorf("memcache: at least one server is required")
	}
	seen := make(map[string]struct{}, len(cfg.servers))
	client := &Client{
		config:       cfg,
		servers:      make([]*serverPool, len(cfg.servers)),
		fetchFlights: make(map[string]*fetchFlight),
		refreshing:   make(map[string]struct{}),
	}
	if cfg.copyServersForRouter {
		client.serverIndexes = make(map[string]int, len(cfg.servers))
	}
	for i, address := range cfg.servers {
		if address == "" {
			return nil, fmt.Errorf("memcache: server address %d is empty", i)
		}
		if _, exists := seen[address]; exists {
			return nil, fmt.Errorf("memcache: duplicate server address %q", address)
		}
		seen[address] = struct{}{}
		client.servers[i] = &serverPool{address: address, config: &client.config}
		if client.serverIndexes != nil {
			client.serverIndexes[address] = i
		}
	}
	// Created after validation so no error path leaks the cancelable context.
	client.rootCtx, client.cancelRoot = context.WithCancel(context.Background())
	return client, nil
}

// NewServers creates a client for one or more backends. It is the preferred
// multi-server constructor; keys are routed with RendezvousRouter by default.
func NewServers(servers []string, options ...Option) (*Client, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("memcache: at least one server is required")
	}
	all := make([]Option, 0, len(options)+1)
	all = append(all, WithServers(servers...))
	all = append(all, options...)
	return New(servers[0], all...)
}

func (c *Client) server(key string) (*serverPool, error) {
	if !c.config.copyServersForRouter {
		index := c.config.router.Pick(key, c.config.servers)
		if index < 0 || index >= len(c.servers) {
			return nil, fmt.Errorf("memcache: router returned invalid server index %d", index)
		}
		return c.servers[index], nil
	}

	// A Router may reorder the slice it receives. Give it a private copy, then
	// resolve the selected address back to the corresponding pool so its index
	// is interpreted relative to the slice it actually saw.
	servers := append([]string(nil), c.config.servers...)
	index := c.config.router.Pick(key, servers)
	if index < 0 || index >= len(servers) {
		return nil, fmt.Errorf("memcache: router returned invalid server index %d", index)
	}
	poolIndex, ok := c.serverIndexes[servers[index]]
	if !ok {
		return nil, fmt.Errorf("memcache: router selected unknown server address %q", servers[index])
	}
	return c.servers[poolIndex], nil
}

// Close cancels background refreshes and any in-flight Fetch loaders,
// releases idle connections, and prevents new work. Requests already on the
// wire finish their exchange. Close is idempotent.
func (c *Client) Close() error {
	c.close.Do(func() {
		c.cancelRoot()
		for _, server := range c.servers {
			server.close()
		}
	})
	return nil
}

// reportError delivers an error that never reaches a caller to the OnError
// hook, when one is installed.
func (c *Client) reportError(err error) {
	if err != nil && c.config.onError != nil {
		c.config.onError(err)
	}
}

// absorb decides whether the Degrade policy swallows err. Only
// infrastructure failures qualify. Ambiguous writes always surface: degrading
// covers "the cache is down", never "the write may or may not have landed".
// Usage errors are permanent client-side mistakes that would otherwise become
// silent fake results on every call. The caller's own context cancellation is
// the caller's answer, not the cache's, so it surfaces too. Absorbed errors
// are reported to OnError.
func (c *Client) absorb(ctx context.Context, err error) bool {
	if err == nil || !c.config.degrade {
		return false
	}
	var ambiguous *AmbiguousWriteError
	if errors.As(err, &ambiguous) {
		return false
	}
	var usage *usageError
	if errors.As(err, &usage) {
		return false
	}
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return false
	}
	c.reportError(err)
	return true
}

// executeMeta runs a raw command against the server selected by its key.
func (c *Client) executeMeta(ctx context.Context, command MetaCommand) (RawResponse, error) {
	for _, flag := range command.Flags {
		if flag == "q" {
			return RawResponse{}, &usageError{fmt.Errorf("memcache: quiet commands require Batch or an explicit mn barrier")}
		}
	}
	if command.HasValue && c.config.maxItemSize > 0 && len(command.Value) > c.config.maxItemSize {
		return RawResponse{}, &usageError{fmt.Errorf("memcache: value exceeds configured maximum of %d bytes", c.config.maxItemSize)}
	}
	data, err := command.marshal()
	if err != nil {
		return RawResponse{}, &usageError{err}
	}
	server, err := c.server(command.Key)
	if err != nil {
		return RawResponse{}, &usageError{err}
	}
	responses, written, err := server.exchange(ctx, data, func(RawResponse) bool { return true })
	if err != nil {
		var serverErr *ServerError
		if errors.As(err, &serverErr) {
			return RawResponse{}, err
		}
		var framed *framedResponseError
		if errors.As(err, &framed) {
			return RawResponse{}, framed.err
		}
		if written == len(data) && commandHasSideEffect(command) {
			return RawResponse{}, &AmbiguousWriteError{Operation: command.Command, Key: command.Key, Cause: err}
		}
		return RawResponse{}, err
	}
	return responses[0], nil
}

func commandHasSideEffect(command MetaCommand) bool {
	if command.Command != "mg" {
		return command.Command != "me"
	}
	for _, flag := range command.Flags {
		if flag != "" {
			switch flag[0] {
			case 'T', 'N', 'R', 'E':
				return true
			}
		}
	}
	return false
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

func buildGet(key string, options MetaGetOptions) (MetaCommand, error) {
	if options.UnlessCAS != nil && options.MetadataOnly {
		return MetaCommand{}, fmt.Errorf("memcache: UnlessCAS requires a value read")
	}
	if options.MetadataOnly && options.VivifyTTL != nil && options.RefreshBefore != nil {
		return MetaCommand{}, fmt.Errorf("memcache: metadata-only refresh cannot distinguish a miss from an early recache hit")
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
	if options.ReturnCAS || options.VivifyTTL != nil || options.RefreshBefore != nil || options.UnlessCAS != nil {
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

func semanticGet(key string, options MetaGetOptions, wire RawResponse) (GetResult, error) {
	result := GetResult{Key: key, Metadata: wire.Metadata, ValueState: ValueFresh, Lease: LeaseNone, ReturnedKey: wire.Key, Opaque: wire.Opaque}
	switch wire.Code {
	case ResponseMiss:
		result.Status, result.ValueState = GetMiss, ValueMissing
	case ResponseValue, ResponseHeader:
		if wire.Code == ResponseHeader && !options.MetadataOnly && options.UnlessCAS == nil {
			return GetResult{}, &ProtocolError{Message: "value-bearing get returned HD"}
		}
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
		placeholder := options.VivifyTTL != nil && (wire.Won || wire.Busy) && !wire.Stale &&
			(options.RefreshBefore == nil || (wire.Code == ResponseValue && len(wire.Value) == 0))
		if placeholder {
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

func buildSet(key string, value []byte, options MetaSetOptions) (MetaCommand, error) {
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
	if options.Invalidate && options.CompareCAS == nil {
		return MetaCommand{}, fmt.Errorf("memcache: Invalidate requires CompareCAS")
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

func storeStatus(code ResponseCode, mode StoreMode) (MutationStatus, error) {
	switch code {
	case ResponseHeader:
		return MutationApplied, nil
	case ResponseExists:
		return MutationCASMismatch, nil
	case ResponseNotFound:
		return MutationNotFound, nil
	case ResponseNotStored:
		if mode == ModeAdd {
			return MutationAlreadyExists, nil
		}
		return MutationNotFound, nil
	default:
		return MutationUnknown, &ProtocolError{Message: fmt.Sprintf("unexpected mutation response %q", code)}
	}
}

func deleteStatus(code ResponseCode) (MutationStatus, error) {
	switch code {
	case ResponseHeader:
		return MutationApplied, nil
	case ResponseExists:
		return MutationCASMismatch, nil
	case ResponseNotFound:
		return MutationNotFound, nil
	default:
		return MutationUnknown, &ProtocolError{Message: fmt.Sprintf("unexpected delete response %q", code)}
	}
}

func arithmeticStatus(code ResponseCode) (MutationStatus, error) {
	switch code {
	case ResponseHeader, ResponseValue:
		return MutationApplied, nil
	case ResponseExists:
		return MutationCASMismatch, nil
	case ResponseNotFound, ResponseNotStored:
		return MutationNotFound, nil
	default:
		return MutationUnknown, &ProtocolError{Message: fmt.Sprintf("unexpected arithmetic response %q", code)}
	}
}

func buildDelete(key string, options MetaDeleteOptions) (MetaCommand, error) {
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

func buildArithmetic(key string, options MetaArithmeticOptions) (MetaCommand, error) {
	if (options.Initial == nil) != (options.InitialTTL == nil) {
		return MetaCommand{}, fmt.Errorf("memcache: Initial and InitialTTL must be supplied together")
	}
	if options.InitialTTL != nil && *options.InitialTTL <= 0 {
		return MetaCommand{}, fmt.Errorf("memcache: InitialTTL must be positive")
	}
	flags := []string{"D" + strconv.FormatUint(options.Delta, 10)}
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

func semanticArithmetic(key string, options MetaArithmeticOptions, wire RawResponse) (ArithmeticResult, error) {
	status, err := arithmeticStatus(wire.Code)
	result := ArithmeticResult{Key: key, Status: status, Metadata: wire.Metadata, ReturnedKey: wire.Key, Opaque: wire.Opaque}
	if err == nil && status == MutationApplied {
		if options.MetadataOnly && wire.Code != ResponseHeader {
			return ArithmeticResult{}, &ProtocolError{Message: "metadata-only arithmetic returned a value"}
		}
		if !options.MetadataOnly && wire.Code != ResponseValue {
			return ArithmeticResult{}, &ProtocolError{Message: "value-bearing arithmetic returned HD"}
		}
	}
	if wire.Code == ResponseValue {
		value, parseErr := strconv.ParseUint(string(wire.Value), 10, 64)
		if parseErr != nil {
			return ArithmeticResult{}, &ProtocolError{Message: "arithmetic value is not uint64"}
		}
		result.Value, result.HasValue = value, true
	}
	return result, err
}
