package persistence

import (
	"errors"
	"log"
	"strconv"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"golang.org/x/exp/errors/fmt"
)

const (
	keySnapshot           = "snapshot"
	keyEventsPrefix       = "events"
	keyEventIndexSnapshot = "eventIndexSnap"
	keyEventIndex         = "eventIndex"
	keyLastEvents         = "lastEvents"
)

type entry struct {
	eventIndex       int // the event index
	eventIndexSnap   int // the event index right after snapshot
	snapshot         []byte
	eventsBucket     []byte
	events           map[int][]byte
	lastEventsBucket []byte
}

//Boltdb struct provider
type Boltdb struct {
	db               *bolt.DB
	snapshotInterval int
	mu               sync.RWMutex
	store            map[string]*entry // actorName -> a persistence entry
	eventFunc        ParseMessage
	snapFunc         ParseMessage
}

type ParseMessage func(src []byte) proto.Message

//NewBoltdbProvider create a provider for multiples actors. Only one instance is support in runtime
func NewBoltdbProvider(path string, snapshotInterval int, eventFunc ParseMessage, snapFunc ParseMessage) (*Boltdb, error) {

	db, err := bolt.Open(path, 0664, bolt.DefaultOptions)
	if err != nil {
		return nil, err
	}
	return &Boltdb{
		db,
		snapshotInterval,
		sync.RWMutex{},
		make(map[string]*entry),
		eventFunc,
		snapFunc,
	}, nil
}

func (provider *Boltdb) loadOrInit(actorName string) (e *entry, loaded bool) {
	e = &entry{}

	funcLoad := func() (*entry, bool, error) {
		provider.mu.RLock()
		defer provider.mu.RUnlock()
		entry, ok := provider.store[actorName]
		if ok {
			return entry, true, nil
		}

		//	log.Printf("entry: 1--------%q\n", e)

		if err := provider.db.View(func(tx *bolt.Tx) error {
			bk := tx.Bucket([]byte(actorName))
			if bk == nil {
				return bolt.ErrBucketNotFound
			}

			//events bucket name
			keyEvents := bk.Get([]byte(keyLastEvents))
			if keyEvents == nil {
				return errors.New("empty events")
			}
			e.eventsBucket = keyEvents

			//eventsIndex
			eventIndex := bk.Get([]byte(keyEventIndex))
			if eventIndex == nil {
				return errors.New("empty events")
			}
			num, err := strconv.Atoi(string(eventIndex))
			if err != nil {
				return err
			}
			e.eventIndex = num

			//eventIndexSnap
			eventIndexSnap := bk.Get([]byte(keyEventIndexSnapshot))
			if eventIndexSnap == nil {
				return errors.New("empty events")
			}
			numSnap, err := strconv.Atoi(string(eventIndexSnap))
			if err != nil {
				return err
			}
			e.eventIndexSnap = numSnap

			//snapshot
			e.snapshot = bk.Get([]byte("snapshot"))

			//events

			bkEvents := bk.Bucket([]byte(keyEvents))
			if bkEvents == nil {
				return bolt.ErrBucketNotFound
			}
			log.Printf("bucket event: %s, %d, %d", keyEvents, numSnap, num)
			events := make(map[int][]byte)
			for i := numSnap; i <= num; i++ {
				v := bkEvents.Get([]byte(strconv.Itoa(i)))
				// log.Printf("event: %s", v)
				if v == nil {
					break
				}
				events[i] = v
			}
			e.events = events

			return nil
		}); err != nil {
			log.Printf("funcLoad error: %s", err)
			return nil, false, err
		}
		//	log.Printf("entry: 2--------%q\n", e)
		return e, false, nil
	}

	if entry, mem, err := funcLoad(); err == nil {

		//	log.Printf("entry: 3--------%q\n", entry)

		if mem {
			return entry, true
		}

		provider.mu.Lock()
		defer provider.mu.Unlock()

		if err := provider.db.Update(func(tx *bolt.Tx) error {
			bk, err := tx.CreateBucketIfNotExists([]byte(actorName))
			if err != nil {
				return bolt.ErrBucketNotFound
			}
			keyEvents := []byte(fmt.Sprintf("%s-%s", keyEventsPrefix, uuid.New()))
			_, err = bk.CreateBucketIfNotExists(keyEvents)
			if err != nil {
				return err
			}
			lastEvents := entry.eventsBucket
			entry.eventsBucket = keyEvents

			if lastEvents != nil {
				bk.DeleteBucket(lastEvents)
			}

			err = bk.Put([]byte(keyLastEvents), keyEvents)
			if err != nil {
				return err
			}
			return nil

		}); err != nil {
			return nil, false
		}
		provider.store[actorName] = entry
		//	log.Printf("entry: 4--------%q\n", entry)
		return entry, true
	}

	funcInit := func() error {

		provider.mu.Lock()
		defer provider.mu.Unlock()

		if err := provider.db.Update(func(tx *bolt.Tx) error {
			bkActor, err := tx.CreateBucketIfNotExists([]byte(actorName))
			if err != nil {
				return bolt.ErrBucketNotFound
			}

			lastEvents := []byte(fmt.Sprintf("%s-%s", keyEventsPrefix, uuid.New()))
			if err := bkActor.Put([]byte(keyLastEvents), lastEvents); err != nil {
				return err
			}
			// log.Printf("bucket lastEvent: %s", lastEvents)
			e.eventsBucket = lastEvents
			return nil
		}); err != nil {
			log.Println(err)
			return err
		}
		return nil
	}

	err := funcInit()

	if err != nil {
		fmt.Println("erroroorrrr: %s", err)
		return nil, false
	}
	e.events = make(map[int][]byte)
	e.eventIndex = 0

	provider.store[actorName] = e

	return e, false
}

//Restart provider
func (provider *Boltdb) Restart() {
	log.Println("RESTART persistence provider")
}

//GetSnapshotInterval get snapshot interval in provider
func (provider *Boltdb) GetSnapshotInterval() int {
	return provider.snapshotInterval
}

//GetSnapshot get last snapshot in provider for actor
func (provider *Boltdb) GetSnapshot(actorName string) (snapshot interface{},
	eventIndex int, ok bool) {
	e, load := provider.loadOrInit(actorName)
	if !load || e.snapshot == nil {
		return nil, 0, false
	}
	// log.Printf("entry GetSnapshot: --------%q\n", e)
	snap := provider.snapFunc(e.snapshot)
	return snap, e.eventIndexSnap, true
}

//GetEvents get events for actor from eventIndexStart
func (provider *Boltdb) GetEvents(actorName string,
	eventIndexStart int, callback func(e interface{})) {

	e, _ := provider.loadOrInit(actorName)
	// log.Printf("GetEvents: %d, %d, %d", entry.eventIndex, entry.eventIndexSnap, eventIndexStart)
	for i := eventIndexStart; i <= e.eventIndex; i++ {
		ev, ok := e.events[i]
		if ok {
			// log.Printf("get Events: --------%q\n", e)
			event := provider.eventFunc(ev)
			// log.Printf("get Events proto: --------%q\n", event)
			callback(event)
		}
	}
}

//PersistEvent persiste event
func (provider *Boltdb) PersistEvent(actorName string,
	eventIndex int, event proto.Message) {

	entry, _ := provider.loadOrInit(actorName)

	//log.Printf("entry -: %q", entry)

	var err error
	entry.events[eventIndex], err = proto.Marshal(event)
	if err != nil {
		log.Println(err)
		return
	}
	entry.eventIndex = eventIndex

	provider.mu.Lock()
	defer provider.mu.Unlock()

	if err := provider.db.Update(func(tx *bolt.Tx) error {
		bkActor, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return err
		}
		err = bkActor.Put([]byte(keyEventIndex), []byte(strconv.Itoa(eventIndex)))
		if err != nil {
			return err
		}
		bk, err := bkActor.CreateBucketIfNotExists(entry.eventsBucket)
		if err != nil {
			return err
		}
		if err := bk.Put([]byte(strconv.Itoa(eventIndex)), entry.events[eventIndex]); err != nil {
			return err
		}
		// log.Printf("bucket: %s, key: %d, value: %s",
		// 	entry.eventsBucket,
		// 	eventIndex,
		// 	event,
		// )
		return nil
	}); err != nil {
		log.Println(err)
	}
}

//PersistSnapshot save snapshot, the snapshot is overwrite
func (provider *Boltdb) PersistSnapshot(actorName string,
	eventIndex int, snapshot proto.Message) {

	// log.Printf("index reques Snapshot: %d", eventIndex)

	e, _ := provider.loadOrInit(actorName)

	// log.Printf("len store: %d", len(e.events))

	e.eventIndexSnap = eventIndex
	var err error
	e.snapshot, err = proto.Marshal(snapshot)
	if err != nil {
		log.Println(err)
		return
	}

	provider.mu.Lock()
	defer provider.mu.Unlock()

	if err := provider.db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return err
		}
		if err := bk.Put([]byte(keySnapshot), e.snapshot); err != nil {
			return err
		}
		if err := bk.Put([]byte(keyEventIndexSnapshot), []byte(strconv.Itoa(eventIndex))); err != nil {
			return err
		}
		// keyEvents := []byte(fmt.Sprintf("%s-%s", keyEventsPrefix, uuid.New()))
		// _, err = bk.CreateBucketIfNotExists(keyEvents)
		// if err != nil {
		// 	return err
		// }
		// lastEvents := entry.eventsBucket
		// entry.eventsBucket = keyEvents

		// if lastEvents != nil {
		// 	bk.DeleteBucket(lastEvents)
		// }

		// err = bk.Put([]byte(keyLastEvents), keyEvents)
		// if err != nil {
		// 	return err
		// }

		return nil
	}); err != nil {
		log.Println(err)
	}

	// TODO:
	// safe memory allocation
	if e != nil && len(e.events) > 2048 {
		log.Printf("delete store in-memory")
		provider.store = make(map[string]*entry)
	}
}
