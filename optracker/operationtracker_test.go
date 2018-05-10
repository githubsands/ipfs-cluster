package optracker

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/ipfs/ipfs-cluster/test"
)

func testOperationTracker(ctx context.Context, t *testing.T) *OperationTracker {
	return NewOperationTracker(ctx)
}

func TestOperationTracker_trackNewOperationWithCtx(t *testing.T) {
	ctx := context.Background()
	opt := testOperationTracker(ctx, t)

	h, _ := cid.Decode(test.TestCid1)
	opt.TrackNewOperation(ctx, h, OperationPin)

	opc, ok := opt.Get(h)
	if !ok {
		t.Errorf("operation wasn't set in operationTracker")
	}

	testopc1 := Operation{
		cid:   h,
		Op:    OperationPin,
		Phase: PhaseQueued,
	}

	if opc.cid != testopc1.cid {
		t.Fail()
	}
	if opc.Op != testopc1.Op {
		t.Fail()
	}
	if opc.Phase != testopc1.Phase {
		t.Fail()
	}
	if t.Failed() {
		fmt.Printf("got %#v\nwant %#v", opc, testopc1)
	}
}

func TestOperationTracker_finish(t *testing.T) {
	ctx := context.Background()
	opt := testOperationTracker(ctx, t)

	h, _ := cid.Decode(test.TestCid1)
	opt.TrackNewOperation(ctx, h, OperationPin)

	opt.Finish(h)
	_, ok := opt.Get(h)
	if ok {
		t.Error("cancelling operation failed to remove it from the map of ongoing operation")
	}
}

func TestOperationTracker_updateOperationPhase(t *testing.T) {
	ctx := context.Background()
	opt := testOperationTracker(ctx, t)

	h, _ := cid.Decode(test.TestCid1)
	opt.TrackNewOperation(ctx, h, OperationPin)

	opt.UpdateOperationPhase(h, PhaseInProgress)
	opc, ok := opt.Get(h)
	if !ok {
		t.Error("error getting operation context after updating phase")
	}

	if opc.Phase != PhaseInProgress {
		t.Errorf("operation phase failed to be updated to %s, got %s", PhaseInProgress.String(), opc.Phase.String())
	}
}
