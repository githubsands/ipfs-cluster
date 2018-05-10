package optracker

import (
	"context"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
)

var logger = logging.Logger("operationtracker")

//go:generate stringer -type=OperationType

// OperationType represents the kinds of operations that the PinTracker
// performs and the operationTracker tracks the status of.
type OperationType int

const (
	// OperationUnknown represents an unknown operation.
	OperationUnknown OperationType = iota
	// OperationPin represents a pin operation.
	OperationPin
	// OperationUnpin represents an unpin operation.
	OperationUnpin
)

//go:generate stringer -type=Phase

// Phase represents the multiple phase that an operation can be in.
type Phase int

const (
	// PhaseError represents an error state.
	PhaseError Phase = iota
	// PhaseQueued represents the queued phase of an operation.
	PhaseQueued
	// PhaseInProgress represents the operation as in progress.
	PhaseInProgress
)

// Operation represents an ongoing operation involving a
// particular Cid. It provides the type and phase of operation
// and a way to mark the operation finished (also used to cancel).
type Operation struct {
	cid    *cid.Cid
	Op     OperationType
	Phase  Phase
	Ctx    context.Context
	cancel func()
}

// NewOperation creates a new Operation.
func NewOperation(ctx context.Context, c *cid.Cid, op OperationType) Operation {
	ctx, cancel := context.WithCancel(ctx)
	return Operation{
		cid:    c,
		Op:     op,
		Phase:  PhaseQueued,
		Ctx:    ctx,
		cancel: cancel, // use *OperationTracker.Finish() instead
	}
}

// OperationTracker tracks and manages all inflight Operations.
type OperationTracker struct {
	ctx context.Context

	mu         sync.RWMutex
	operations map[string]Operation
}

// NewOperationTracker creates a new OperationTracker.
func NewOperationTracker(ctx context.Context) *OperationTracker {
	return &OperationTracker{
		ctx:        ctx,
		operations: make(map[string]Operation),
	}
}

// TrackNewOperation tracks a new operation, adding it to the OperationTracker's
// map of inflight operations.
func (opt *OperationTracker) TrackNewOperation(ctx context.Context, c *cid.Cid, op OperationType) {
	op2 := NewOperation(ctx, c, op)
	logger.Debugf(
		"'%s' on cid '%s' has been created with phase '%s'",
		op.String(),
		c.String(),
		op2.Phase.String(),
	)
	opt.Set(op2)
}

// UpdateOperationPhase updates the phase of the operation associated with
// the provided Cid.
func (opt *OperationTracker) UpdateOperationPhase(c *cid.Cid, p Phase) {
	opc, ok := opt.Get(c)
	if !ok {
		logger.Debugf(
			"attempted to update non-existent operation with cid: %s",
			c.String(),
		)
		return
	}
	opc.Phase = p
	opt.Set(opc)
	logger.Debugf(
		"'%s' on cid '%s' has been updated to phase '%s'",
		opc.Op.String(),
		c.String(),
		p.String(),
	)
}

// Finish cancels the operation context and removes it from the map.
func (opt *OperationTracker) Finish(c *cid.Cid) {
	opt.mu.Lock()
	defer opt.mu.Unlock()

	opc, ok := opt.operations[c.String()]
	if !ok {
		logger.Debugf(
			"attempted to remove non-existent operation with cid: %s",
			c.String(),
		)
		return
	}

	opc.cancel()
	delete(opt.operations, c.String())
	logger.Debugf(
		"'%s' on cid '%s' has been removed",
		opc.Op.String(),
		c.String(),
	)
}

// Set sets the operation in the OperationTrackers map.
func (opt *OperationTracker) Set(oc Operation) {
	opt.mu.Lock()
	opt.operations[oc.cid.String()] = oc
	opt.mu.Unlock()
}

// Get gets the operation associated with the Cid. If the
// there is no associated operation, Get will return Operation{}, false.
func (opt *OperationTracker) Get(c *cid.Cid) (Operation, bool) {
	opt.mu.RLock()
	opc, ok := opt.operations[c.String()]
	opt.mu.RUnlock()
	return opc, ok
}
