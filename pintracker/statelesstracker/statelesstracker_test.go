package statelesstracker

import (
	"reflect"
	"testing"

	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/test"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

func mustDecodeCid(v string) *cid.Cid {
	c, _ := cid.Decode(v)
	return c
}

func testStatelessPinTracker(t *testing.T) *StatelessPinTracker {
	spt := NewStatelessPinTracker(test.TestPeerID1)
	spt.SetClient(test.NewMockRPCClient(t))
	return spt
}

func TestStatelessPinTracker_New(t *testing.T) {
	spt := testStatelessPinTracker(t)
	defer spt.Shutdown()
}

func TestStatelessPinTracker_Shutdown(t *testing.T) {
	spt := testStatelessPinTracker(t)
	err := spt.Shutdown()
	if err != nil {
		t.Fatal(err)
	}
	err = spt.Shutdown()
	if err != nil {
		t.Fatal(err)
	}
}

func TestStatelessPinTracker_Track(t *testing.T) {
	type args struct {
		c api.Pin
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"basic track",
			args{
				api.Pin{
					Cid:                  mustDecodeCid(test.TestCid1),
					Allocations:          []peer.ID{},
					ReplicationFactorMin: -1,
					ReplicationFactorMax: -1,
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := testStatelessPinTracker(t)
			if err := s.Track(tt.args.c); (err != nil) != tt.wantErr {
				t.Errorf("StatelessPinTracker.Track() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatelessPinTracker_Untrack(t *testing.T) {
	type args struct {
		c *cid.Cid
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"basic untrack",
			args{
				mustDecodeCid(test.TestCid1),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatelessPinTracker{}
			if err := s.Untrack(tt.args.c); (err != nil) != tt.wantErr {
				t.Errorf("StatelessPinTracker.Untrack() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStatelessPinTracker_StatusAll(t *testing.T) {
	type fields struct {
	}
	tests := []struct {
		name   string
		fields fields
		want   []api.PinInfo
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatelessPinTracker{}
			if got := s.StatusAll(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatelessPinTracker.StatusAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatelessPinTracker_Status(t *testing.T) {
	type fields struct {
	}
	type args struct {
		c *cid.Cid
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   api.PinInfo
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatelessPinTracker{}
			if got := s.Status(tt.args.c); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatelessPinTracker.Status() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatelessPinTracker_SyncAll(t *testing.T) {
	type fields struct {
	}
	tests := []struct {
		name    string
		fields  fields
		want    []api.PinInfo
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatelessPinTracker{}
			got, err := s.SyncAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("StatelessPinTracker.SyncAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatelessPinTracker.SyncAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatelessPinTracker_Sync(t *testing.T) {
	type fields struct {
	}
	type args struct {
		c *cid.Cid
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    api.PinInfo
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatelessPinTracker{}
			got, err := s.Sync(tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("StatelessPinTracker.Sync() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatelessPinTracker.Sync() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatelessPinTracker_RecoverAll(t *testing.T) {
	type fields struct {
	}
	tests := []struct {
		name    string
		fields  fields
		want    []api.PinInfo
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatelessPinTracker{}
			got, err := s.RecoverAll()
			if (err != nil) != tt.wantErr {
				t.Errorf("StatelessPinTracker.RecoverAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatelessPinTracker.RecoverAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatelessPinTracker_Recover(t *testing.T) {
	type fields struct {
	}
	type args struct {
		c *cid.Cid
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    api.PinInfo
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StatelessPinTracker{}
			got, err := s.Recover(tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("StatelessPinTracker.Recover() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatelessPinTracker.Recover() = %v, want %v", got, tt.want)
			}
		})
	}
}
