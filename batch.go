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

// GetOperation is a batch meta get.
type GetOperation struct {
	Key     string
	Options MetaGetOptions
}

func (o GetOperation) operationKey() string { return o.Key }
func (o GetOperation) prepare() (MetaCommand, operationKind, error) {
	c, e := buildGet(o.Key, o.Options)
	return c, kindGet, e
}

// SetOperation is a batch meta set.
type SetOperation struct {
	Key     string
	Value   []byte
	Options MetaSetOptions
}

func (o SetOperation) operationKey() string { return o.Key }
func (o SetOperation) prepare() (MetaCommand, operationKind, error) {
	c, e := buildSet(o.Key, o.Value, o.Options)
	return c, kindSet, e
}

// DeleteOperation is a batch meta delete.
type DeleteOperation struct {
	Key     string
	Options MetaDeleteOptions
}

func (o DeleteOperation) operationKey() string { return o.Key }
func (o DeleteOperation) prepare() (MetaCommand, operationKind, error) {
	c, e := buildDelete(o.Key, o.Options)
	return c, kindDelete, e
}

// ArithmeticOperation is a batch meta arithmetic command.
type ArithmeticOperation struct {
	Key     string
	Options MetaArithmeticOptions
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
	end     int
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

func normalizeOperation(operation Operation) (Operation, error) {
	switch op := operation.(type) {
	case GetOperation, SetOperation, DeleteOperation, ArithmeticOperation:
		return operation, nil
	case *GetOperation:
		if op == nil {
			return nil, fmt.Errorf("nil *GetOperation")
		}
		return *op, nil
	case *SetOperation:
		if op == nil {
			return nil, fmt.Errorf("nil *SetOperation")
		}
		return *op, nil
	case *DeleteOperation:
		if op == nil {
			return nil, fmt.Errorf("nil *DeleteOperation")
		}
		return *op, nil
	case *ArithmeticOperation:
		if op == nil {
			return nil, fmt.Errorf("nil *ArithmeticOperation")
		}
		return *op, nil
	default:
		return nil, fmt.Errorf("unsupported operation %T", operation)
	}
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
	needsSuccess := successResponseRequired(item)
	// For mg, q suppresses only misses; hits still carry the requested value.
	if item.kind == kindGet || !needsSuccess {
		command.Flags = append(command.Flags, "q")
	}
	return command
}

func successResponseRequired(item preparedOperation) bool {
	switch item.kind {
	case kindGet:
		return item.op.(GetOperation).Options.VivifyTTL != nil
	case kindArithmetic:
		return true
	case kindSet:
		op := item.op.(SetOperation)
		return op.Options.ReturnCAS || op.Options.ReturnKey || op.Options.ReturnSize
	case kindDelete:
		return item.op.(DeleteOperation).Options.ReturnKey
	default:
		return false
	}
}

// batch executes operations in pipelines grouped by backend and restores
// input order. It backs MetaClient.Batch and the scenario-layer collection
// verbs. All operations are validated before any network write.
// Per-operation transport failures are returned in OperationResult.Err so a
// failure on one backend does not erase successful results from another.
func (c *Client) batch(ctx context.Context, operations []Operation) ([]OperationResult, error) {
	results := make([]OperationResult, len(operations))
	if len(operations) == 0 {
		return results, nil
	}
	groups := make(map[int]*batchGroup)
	for index, operation := range operations {
		if operation == nil {
			return nil, fmt.Errorf("memcache: batch operation %d is nil", index)
		}
		operation, err := normalizeOperation(operation)
		if err != nil {
			return nil, fmt.Errorf("memcache: batch operation %d: %w", index, err)
		}
		command, kind, err := operation.prepare()
		if err != nil {
			return nil, fmt.Errorf("memcache: batch operation %d: %w", index, err)
		}
		if command.HasValue && c.config.maxItemSize > 0 && len(command.Value) > c.config.maxItemSize {
			return nil, fmt.Errorf("memcache: batch operation %d value exceeds configured maximum", index)
		}
		for _, flag := range command.Flags {
			if len(flag) > 0 && flag[0] == 'O' {
				return nil, fmt.Errorf("memcache: batch operation %d: Opaque is reserved by Batch", index)
			}
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
			group.items[i].end = len(group.payload)
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
			responses, written, err := c.servers[group.server].pipeline(ctx, group.payload)
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
	badIndex := make(map[int]error)
	expected := make(map[int]struct{}, len(outcome.group.items))
	for _, item := range outcome.group.items {
		expected[item.index] = struct{}{}
	}
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
		if _, ok := expected[index]; !ok {
			parseFailure = &ProtocolError{Message: "pipeline response has unexpected opaque token"}
			continue
		}
		if _, duplicate := byIndex[index]; duplicate {
			delete(byIndex, index)
			badIndex[index] = &ProtocolError{Message: "pipeline response repeated opaque token"}
			continue
		}
		if _, duplicate := badIndex[index]; duplicate {
			continue
		}
		byIndex[index] = response
	}
	if outcome.err == nil && !barrier {
		parseFailure = &ProtocolError{Message: "pipeline omitted no-op barrier"}
	}
	for _, item := range outcome.group.items {
		if itemError := badIndex[item.index]; itemError != nil {
			sideEffect := item.kind != kindGet || commandHasSideEffect(item.command)
			ambiguous := sideEffect && outcome.written >= item.end
			if ambiguous {
				itemError = &AmbiguousWriteError{Operation: item.command.Command, Key: item.op.operationKey(), Cause: itemError}
			}
			output[item.index] = OperationResult{Err: itemError, Ambiguous: ambiguous}
			continue
		}
		if response, ok := byIndex[item.index]; ok {
			output[item.index] = parseBatchResponse(item, response)
			continue
		}
		if barrier && parseFailure == nil {
			if !successResponseRequired(item) {
				output[item.index] = inferQuietResult(item)
				continue
			}
			cause := &ProtocolError{Message: "pipeline omitted required response"}
			ambiguous := (item.kind != kindGet || commandHasSideEffect(item.command)) && outcome.written >= item.end
			if ambiguous {
				output[item.index] = OperationResult{Err: &AmbiguousWriteError{Operation: item.command.Command, Key: item.op.operationKey(), Cause: cause}, Ambiguous: true}
			} else {
				output[item.index] = OperationResult{Err: cause}
			}
			continue
		}
		cause := outcome.err
		if cause == nil {
			cause = parseFailure
		}
		if cause == nil {
			cause = &ProtocolError{Message: "pipeline failed"}
		}
		sideEffect := item.kind != kindGet || commandHasSideEffect(item.command)
		ambiguous := sideEffect && outcome.written >= item.end
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
	if wire.parseErr != nil {
		return OperationResult{Err: wire.parseErr}
	}
	wire.Opaque = "" // Batch's opaque token is internal correlation state.
	switch item.kind {
	case kindGet:
		op := item.op.(GetOperation)
		r, err := semanticGet(op.Key, op.Options, wire)
		return OperationResult{Get: &r, Err: err}
	case kindSet:
		op := item.op.(SetOperation)
		status, err := storeStatus(wire.Code, op.Options.Mode)
		r := MutationResult{Key: op.Key, Status: status, CAS: wire.Metadata.CAS, Size: wire.Metadata.Size, ReturnedKey: wire.Key}
		return OperationResult{Mutation: &r, Err: err}
	case kindDelete:
		op := item.op.(DeleteOperation)
		status, err := deleteStatus(wire.Code)
		r := MutationResult{Key: op.Key, Status: status, CAS: wire.Metadata.CAS, Size: wire.Metadata.Size, ReturnedKey: wire.Key}
		return OperationResult{Mutation: &r, Err: err}
	case kindArithmetic:
		op := item.op.(ArithmeticOperation)
		r, err := semanticArithmetic(op.Key, op.Options, wire)
		return OperationResult{Arithmetic: &r, Err: err}
	default:
		return OperationResult{Err: &ProtocolError{Message: "unknown batch operation"}}
	}
}
