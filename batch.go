package memcache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
)

// Operation is one item accepted by Client.Batch. The concrete operation
// structs below are the only implementations.
type Operation interface {
	operationKey() string
	prepare() (MetaCommand, operationKind, error)
}

type operationKind uint8

const (
	kindGet operationKind = iota
	kindSet
	kindDelete
	kindArithmetic
)

type GetOperation struct {
	Key     string
	Options GetOptions
}

func (o GetOperation) operationKey() string { return o.Key }
func (o GetOperation) prepare() (MetaCommand, operationKind, error) {
	c, e := buildGet(o.Key, o.Options)
	return c, kindGet, e
}

type SetOperation struct {
	Key     string
	Value   []byte
	Options SetOptions
}

func (o SetOperation) operationKey() string { return o.Key }
func (o SetOperation) prepare() (MetaCommand, operationKind, error) {
	c, e := buildSet(o.Key, o.Value, o.Options)
	return c, kindSet, e
}

type DeleteOperation struct {
	Key     string
	Options DeleteOptions
}

func (o DeleteOperation) operationKey() string { return o.Key }
func (o DeleteOperation) prepare() (MetaCommand, operationKind, error) {
	c, e := buildDelete(o.Key, o.Options)
	return c, kindDelete, e
}

type ArithmeticOperation struct {
	Key     string
	Options ArithmeticOptions
}

func (o ArithmeticOperation) operationKey() string { return o.Key }
func (o ArithmeticOperation) prepare() (MetaCommand, operationKind, error) {
	c, e := buildArithmetic(o.Key, o.Options)
	return c, kindArithmetic, e
}

// OperationResult contains exactly one typed result when Err is nil.
// Ambiguous is true when a side effect may have happened but no response or
// barrier proved its outcome.
type OperationResult struct {
	Get        *GetResult
	Mutation   *MutationResult
	Arithmetic *ArithmeticResult
	Err        error
	Ambiguous  bool
}

type preparedOperation struct {
	index   int
	kind    operationKind
	op      Operation
	command MetaCommand
	start   int
}

type batchGroup struct {
	server  int
	items   []preparedOperation
	payload []byte
}
type groupOutcome struct {
	group     batchGroup
	responses []RawResponse
	written   int
	err       error
}

func quietCommand(item preparedOperation) MetaCommand {
	command := item.command
	command.Flags = make([]string, 0, len(item.command.Flags)+2)
	for _, flag := range item.command.Flags {
		if len(flag) == 0 || flag[0] != 'O' {
			command.Flags = append(command.Flags, flag)
		}
	}
	command.Flags = append(command.Flags, "O"+strconv.Itoa(item.index))
	needsSuccess := item.kind == kindArithmetic
	if item.kind == kindSet {
		op := item.op.(SetOperation)
		needsSuccess = op.Options.ReturnCAS || op.Options.ReturnKey || op.Options.ReturnSize
	}
	if item.kind == kindDelete {
		op := item.op.(DeleteOperation)
		needsSuccess = op.Options.ReturnKey
	}
	// For mg, q suppresses only misses; hits still carry the requested value.
	if item.kind == kindGet || !needsSuccess {
		command.Flags = append(command.Flags, "q")
	}
	return command
}

// Batch executes operations in pipelines grouped by backend and restores
// input order. All operations are validated before any network write.
// Per-operation transport failures are returned in OperationResult.Err so a
// failure on one backend does not erase successful results from another.
func (c *Client) Batch(ctx context.Context, operations []Operation) ([]OperationResult, error) {
	results := make([]OperationResult, len(operations))
	if len(operations) == 0 {
		return results, nil
	}
	groups := make(map[int]*batchGroup)
	for index, operation := range operations {
		if operation == nil {
			return nil, fmt.Errorf("memcache: batch operation %d is nil", index)
		}
		command, kind, err := operation.prepare()
		if err != nil {
			return nil, fmt.Errorf("memcache: batch operation %d: %w", index, err)
		}
		if command.HasValue && c.config.maxItemSize > 0 && len(command.Value) > c.config.maxItemSize {
			return nil, fmt.Errorf("memcache: batch operation %d value exceeds configured maximum", index)
		}
		server, err := c.server(operation.operationKey())
		if err != nil {
			return nil, err
		}
		serverIndex := -1
		for i, candidate := range c.servers {
			if candidate == server {
				serverIndex = i
				break
			}
		}
		group := groups[serverIndex]
		if group == nil {
			group = &batchGroup{server: serverIndex}
			groups[serverIndex] = group
		}
		group.items = append(group.items, preparedOperation{index: index, kind: kind, op: operation, command: command})
	}
	ordered := make([]batchGroup, 0, len(groups))
	for serverIndex := range c.servers {
		group := groups[serverIndex]
		if group == nil {
			continue
		}
		for i := range group.items {
			group.items[i].start = len(group.payload)
			wire, err := quietCommand(group.items[i]).marshal()
			if err != nil {
				return nil, fmt.Errorf("memcache: batch operation %d: %w", group.items[i].index, err)
			}
			group.payload = append(group.payload, wire...)
		}
		group.payload = append(group.payload, "mn\r\n"...)
		ordered = append(ordered, *group)
	}
	outcomes := make(chan groupOutcome, len(ordered))
	var wg sync.WaitGroup
	for _, group := range ordered {
		wg.Add(1)
		go func(group batchGroup) {
			defer wg.Done()
			responses, written, err := c.servers[group.server].exchange(ctx, group.payload, func(response RawResponse) bool { return response.Code == ResponseNoop })
			outcomes <- groupOutcome{group: group, responses: responses, written: written, err: err}
		}(group)
	}
	wg.Wait()
	close(outcomes)
	for outcome := range outcomes {
		c.resolveGroup(results, outcome)
	}
	return results, nil
}

func (c *Client) resolveGroup(output []OperationResult, outcome groupOutcome) {
	byIndex := make(map[int]RawResponse)
	barrier := false
	parseFailure := error(nil)
	for _, response := range outcome.responses {
		if response.Code == ResponseNoop {
			barrier = true
			continue
		}
		if response.Opaque == "" {
			parseFailure = &ProtocolError{Message: "pipeline response omitted opaque token"}
			continue
		}
		index, err := strconv.Atoi(response.Opaque)
		if err != nil {
			parseFailure = &ProtocolError{Message: "pipeline response has invalid opaque token"}
			continue
		}
		byIndex[index] = response
	}
	if outcome.err == nil && !barrier {
		parseFailure = &ProtocolError{Message: "pipeline omitted no-op barrier"}
	}
	for _, item := range outcome.group.items {
		if response, ok := byIndex[item.index]; ok {
			output[item.index] = parseBatchResponse(item, response)
			continue
		}
		if barrier && parseFailure == nil {
			output[item.index] = inferQuietResult(item)
			continue
		}
		cause := outcome.err
		if cause == nil {
			cause = parseFailure
		}
		if cause == nil {
			cause = &ProtocolError{Message: "pipeline failed"}
		}
		sideEffect := item.kind != kindGet
		ambiguous := sideEffect && outcome.written > item.start
		if ambiguous {
			cause = &AmbiguousWriteError{Operation: item.command.Command, Key: item.op.operationKey(), Cause: cause}
		}
		output[item.index] = OperationResult{Err: cause, Ambiguous: ambiguous}
	}
}

func inferQuietResult(item preparedOperation) OperationResult {
	switch item.kind {
	case kindGet:
		r := GetResult{Key: item.op.operationKey(), Status: GetMiss, ValueState: ValueMissing}
		return OperationResult{Get: &r}
	case kindSet, kindDelete:
		r := MutationResult{Key: item.op.operationKey(), Status: MutationApplied}
		return OperationResult{Mutation: &r}
	default:
		return OperationResult{Err: &ProtocolError{Message: "arithmetic response was suppressed"}}
	}
}

func parseBatchResponse(item preparedOperation, wire RawResponse) OperationResult {
	switch item.kind {
	case kindGet:
		op := item.op.(GetOperation)
		r, err := semanticGet(op.Key, op.Options, wire)
		return OperationResult{Get: &r, Err: err}
	case kindSet:
		op := item.op.(SetOperation)
		status, err := mutationStatus(wire.Code, op.Options.Mode)
		r := MutationResult{Key: op.Key, Status: status, CAS: wire.Metadata.CAS}
		return OperationResult{Mutation: &r, Err: err}
	case kindDelete:
		op := item.op.(DeleteOperation)
		status, err := mutationStatus(wire.Code, ModeSet)
		r := MutationResult{Key: op.Key, Status: status, CAS: wire.Metadata.CAS}
		return OperationResult{Mutation: &r, Err: err}
	case kindArithmetic:
		op := item.op.(ArithmeticOperation)
		r, err := semanticArithmetic(op.Key, wire)
		return OperationResult{Arithmetic: &r, Err: err}
	default:
		return OperationResult{Err: &ProtocolError{Message: "unknown batch operation"}}
	}
}
