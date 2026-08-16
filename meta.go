package memcache

import "context"

// MetaClient is the 1:1 protocol layer behind Client.Meta. Its methods map
// directly onto the meta commands (mg, ms, md, ma, me, mn) and return typed
// results without collapsing protocol states into errors. Anything the
// scenario verbs do not cover is expressible here.
type MetaClient struct{ client *Client }

// Meta returns the protocol-layer escape hatch. The returned client shares
// this client's connection pools and configuration.
func (c *Client) Meta() *MetaClient { return &MetaClient{client: c} }

// Get performs a configurable meta get.
func (m *MetaClient) Get(ctx context.Context, key string, options MetaGetOptions) (GetResult, error) {
	command, err := buildGet(key, options)
	if err != nil {
		return GetResult{}, err
	}
	wire, err := m.client.executeMeta(ctx, command)
	if err != nil {
		return GetResult{}, err
	}
	return semanticGet(key, options, wire)
}

// Set performs a configurable meta set in any of its five modes.
func (m *MetaClient) Set(ctx context.Context, key string, value []byte, options MetaSetOptions) (MutationResult, error) {
	command, err := buildSet(key, value, options)
	if err != nil {
		return MutationResult{}, err
	}
	wire, err := m.client.executeMeta(ctx, command)
	if err != nil {
		return MutationResult{}, err
	}
	status, err := storeStatus(wire.Code, options.Mode)
	return MutationResult{Key: key, Status: status, CAS: wire.Metadata.CAS, Size: wire.Metadata.Size, ReturnedKey: wire.Key, Opaque: wire.Opaque}, err
}

// Delete performs a configurable meta delete or stale invalidation.
func (m *MetaClient) Delete(ctx context.Context, key string, options MetaDeleteOptions) (MutationResult, error) {
	command, err := buildDelete(key, options)
	if err != nil {
		return MutationResult{}, err
	}
	wire, err := m.client.executeMeta(ctx, command)
	if err != nil {
		return MutationResult{}, err
	}
	status, err := deleteStatus(wire.Code)
	return MutationResult{Key: key, Status: status, CAS: wire.Metadata.CAS, Size: wire.Metadata.Size, ReturnedKey: wire.Key, Opaque: wire.Opaque}, err
}

// Arithmetic performs configurable unsigned 64-bit meta arithmetic.
func (m *MetaClient) Arithmetic(ctx context.Context, key string, options MetaArithmeticOptions) (ArithmeticResult, error) {
	command, err := buildArithmetic(key, options)
	if err != nil {
		return ArithmeticResult{}, err
	}
	wire, err := m.client.executeMeta(ctx, command)
	if err != nil {
		return ArithmeticResult{}, err
	}
	return semanticArithmetic(key, options, wire)
}

// Execute runs a raw command against the server selected by its key.
func (m *MetaClient) Execute(ctx context.Context, command MetaCommand) (RawResponse, error) {
	return m.client.executeMeta(ctx, command)
}

// Batch executes operations in pipelines grouped by backend and restores
// input order. All operations are validated before any network write.
// Per-operation transport failures are returned in OperationResult.Err so a
// failure on one backend does not erase successful results from another.
func (m *MetaClient) Batch(ctx context.Context, operations []Operation) ([]OperationResult, error) {
	return m.client.batch(ctx, operations)
}

// Debug returns the me command's internal key/value metadata. A miss is
// reported as ErrCacheMiss.
func (m *MetaClient) Debug(ctx context.Context, key string) (map[string]string, error) {
	wire, err := m.client.executeMeta(ctx, MetaCommand{Command: "me", Key: key})
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

// Noop checks every configured backend with the meta no-op command.
func (m *MetaClient) Noop(ctx context.Context) error {
	for _, server := range m.client.servers {
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
