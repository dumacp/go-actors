package persistence

import (
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
)

type protoMsg struct {
	state string
}

func (p *protoMsg) Reset()         {}
func (p *protoMsg) String() string { return p.state }
func (p *protoMsg) ProtoMessage()  {}

func TestBoltdb_loadOrInit(t *testing.T) {

	// type fields struct {
	// 	db               *bolt.DB
	// 	snapshotInterval int
	// 	mu               sync.RWMutex
	// 	store            map[string]*entry
	// }
	type args struct {
		actorName string
	}
	tests := []struct {
		name string
		// fields     fields
		args       args
		wantE      *entry
		wantLoaded bool
	}{
		// TODO: Add test cases.
		{
			"test-1",
			args{"actor-test-1"},
			&entry{
				eventIndex:     3,
				eventIndexSnap: 0,
				events:         nil,
				eventsBucket:   []byte("events"),
				snapshot:       []byte("snapshot"),
			},
			false,
		},
	}
	provider, err := NewBoltdbProvider("/tmp/boltdb-test", 2, nil, nil)
	if err != nil {
		t.Errorf("fail NewBoltdbProvider in: %s", "/tmp/boltdb-test")
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			// provider := &Boltdb{
			// 	db:               tt.fields.db,
			// 	snapshotInterval: tt.fields.snapshotInterval,
			// 	mu:               tt.fields.mu,
			// 	store:            tt.fields.store,
			// }
			gotE, gotLoaded := provider.loadOrInit(tt.args.actorName)
			if !reflect.DeepEqual(gotE, tt.wantE) {
				t.Errorf("Boltdb.loadOrInit() gotE = %q, want %q", gotE, tt.wantE)
			}
			if gotLoaded != tt.wantLoaded {
				t.Errorf("Boltdb.loadOrInit() gotLoaded = %v, want %v", gotLoaded, tt.wantLoaded)
			}
		})
	}
}

func TestBoltdb_PersistEvent(t *testing.T) {
	// type fields struct {
	// 	db               *bolt.DB
	// 	snapshotInterval int
	// 	mu               sync.RWMutex
	// 	store            map[string]*entry
	// }

	type args struct {
		actorName  string
		eventIndex int
		event      proto.Message
	}
	tests := []struct {
		name string
		// fields fields
		args args
	}{
		// TODO: Add test cases.
		{
			"test-1",
			args{
				"actor-test-2",
				1,
				&protoMsg{state: "hi"},
			},
		},
		{
			"test-2",
			args{
				"actor-test-2",
				2,
				&protoMsg{state: "bye"},
			},
		},
	}
	provider, err := NewBoltdbProvider("/tmp/boltdb-test", 2, nil, nil)
	if err != nil {
		t.Errorf("fail NewBoltdbProvider in: %s", "/tmp/boltdb-test")
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// provider := &Boltdb{
			// 	db:               tt.fields.db,
			// 	snapshotInterval: tt.fields.snapshotInterval,
			// 	mu:               tt.fields.mu,
			// 	store:            tt.fields.store,
			// }
			provider.PersistEvent(tt.args.actorName, tt.args.eventIndex, tt.args.event)

		})
		t.Errorf("events: %s", provider.store["actor-test-2"].events)
	}
}
