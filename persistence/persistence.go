package persistence

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
)

const (
	keySnapshot           = "snapshot"
	keyEventsPrefix       = "events"
	keyEventIndexSnapshot = "eventIndexSnap"
	keyEventIndex         = "eventIndex"
	keyLastEvents         = "lastEvents"
)

type entry struct {
	eventIndex     int // the event index
	eventIndexSnap int // the event index right after snapshot
	// snapshot       []byte
	eventsBucket []byte

	lastEventsBucket []byte
	// storeEvents      *entryEvents
}

// type entryEvents struct {
// 	events map[int][]byte
// }

//Boltdb struct provider
type Boltdb struct {
	db               *bolt.DB
	snapshotInterval int
	mu               sync.Mutex
	store            map[string]*entry // actorName -> a persistence entry

	eventFunc ParseMessage
	snapFunc  ParseMessage
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
		sync.Mutex{},
		make(map[string]*entry),
		eventFunc,
		snapFunc,
	}, nil
}

func (provider *Boltdb) loadOrInit(actorName string) (*entry, bool) {

	provider.mu.Lock()
	defer provider.mu.Unlock()

	et, ok := provider.store[actorName]
	if ok {
		return et, true
	}
	e := new(entry)
	provider.store[actorName] = e

	keyEvents := make([]byte, 0)
	var num int
	var numSnap int
	err := provider.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(actorName))
		if bk == nil {
			return bolt.ErrBucketNotFound
		}

		//events bucket name
		keyEvts := bk.Get([]byte(keyLastEvents))
		if keyEvts == nil || len(keyEvts) <= 0 {
			return fmt.Errorf("empty events, keyLastEvents -> %s", keyLastEvents)
		}
		keyEvents := append(keyEvents, keyEvts...)

		e.eventsBucket = keyEvents
		e.lastEventsBucket = keyEvents

		//eventsIndex
		eventIndex := bk.Get([]byte(keyEventIndex))
		if eventIndex == nil {
			return fmt.Errorf("empty events, keyEventIndex -> %s", keyEventIndex)
			// return errors.New("empty events")
		}
		var err error
		num, err = strconv.Atoi(string(eventIndex))
		if err != nil {
			return err
		}
		e.eventIndex = num

		numSnap = 0
		//eventIndexSnap
		eventIndexSnap := bk.Get([]byte(keyEventIndexSnapshot))
		if eventIndexSnap != nil {

			numSnap, err = strconv.Atoi(string(eventIndexSnap))
			if err != nil {
				return err
			}
			//log.Printf("loadOrInit, keyEventIndexSnapshot -> %s", eventIndexSnap)
			e.eventIndexSnap = numSnap
		}

		return nil
	})

	//log.Printf("InitOrLoad, eventbucket -> %s", e.eventsBucket)

	if err == nil {
		//provider.store[actorName] = e
		return e, false
	}

	lastEvents := []byte(fmt.Sprintf("%s-%s", keyEventsPrefix, uuid.New()))
	err = provider.db.Update(func(tx *bolt.Tx) error {
		bkActor, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return bolt.ErrBucketNotFound
		}

		if err := bkActor.Put([]byte(keyLastEvents), lastEvents); err != nil {
			return err
		}
		//log.Printf("bucket lastEvent: %s", lastEvents)
		e.eventsBucket = lastEvents
		return nil
	})

	if err != nil {
		log.Printf("erroroorrrr: %s", err)
		return nil, false
	}

	e.eventIndex = 0
	return e, false
}

//Restart provider
func (provider *Boltdb) Restart() {
	// provider.store = make(map[string]*entry)
	log.Println("RESTART persistence provider")
}

//GetSnapshotInterval get snapshot interval in provider
func (provider *Boltdb) GetSnapshotInterval() int {
	interval := provider.snapshotInterval
	return interval
}

//GetSnapshot get last snapshot in provider for actor
func (provider *Boltdb) GetSnapshot(actorName string) (interface{}, int, bool) {
	//log.Println("GetSnapshot")

	e, _ := provider.loadOrInit(actorName)

	//snapshot
	var snapshot []byte
	err := provider.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(actorName))
		if bk == nil {
			return bolt.ErrBucketNotFound
		}

		snapshot = bk.Get([]byte("snapshot"))
		if snapshot == nil {
			return fmt.Errorf("empty snapshot")
		}
		return nil
	})

	if err != nil {
		log.Println(err)
		return nil, 0, false
	}

	dstCopy := make([]byte, len(snapshot))
	copy(dstCopy, snapshot)
	snap := provider.snapFunc(dstCopy)
	//log.Printf("entry GetSnapshot: -------- indexSanp -> %d", e.eventIndexSnap)
	return snap, e.eventIndexSnap, true
}

//GetEvents get events for actor from eventIndexStart
func (provider *Boltdb) GetEvents(actorName string,
	eventIndexStart, eventIndexEnd int, callback func(e interface{})) {

	e, _ := provider.loadOrInit(actorName)
	provider.mu.Lock()
	defer provider.mu.Unlock()

	events := make(map[int][]byte, 0)
	err := provider.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(actorName))
		if bk == nil {
			return bolt.ErrBucketNotFound
		}

		keyEvents := e.eventsBucket

		bkEvents := bk.Bucket([]byte(keyEvents))
		if bkEvents == nil {
			return bolt.ErrBucketNotFound
		}
		//log.Printf("bucket event: %s, %d, %d, %d", keyEvents, eventIndexStart, e.eventIndexSnap, e.eventIndex)

		for i := eventIndexStart; i <= eventIndexEnd; i++ {
			v := bkEvents.Get([]byte(strconv.Itoa(i)))
			// log.Printf("event: %s", v)
			if v == nil || len(v) <= 0 {
				log.Printf("event error: %s", v)
				continue
			}
			//log.Printf("event get: %s\n\n\n\n", v)
			events[i] = make([]byte, 0)
			events[i] = append(events[i], v...)
		}
		return nil
	})

	if err != nil {
		log.Printf("error load event -> %s", err)
		return
	}

	for _, v := range events {
		// log.Printf("get Events: --------%#v\n", ev)
		event := provider.eventFunc(v)
		// log.Printf("get Events proto, %d: --------%#v\n", i, event)
		callback(event)
	}
	// log.Printf("get Events, entry 2: --------%#v\n", e.events)

}

//PersistEvent persiste event
func (provider *Boltdb) PersistEvent(actorName string,
	eventIndex int, event proto.Message) {

	e, _ := provider.loadOrInit(actorName)
	//log.Printf("bucket in event: %s", e.eventsBucket)
	provider.mu.Lock()
	defer provider.mu.Unlock()

	// var err error
	// entry.events[eventIndex], err = proto.Marshal(event)
	evt, err := proto.Marshal(event)
	if err != nil {
		log.Println(err)
		return
	}

	if err := provider.db.Update(func(tx *bolt.Tx) error {
		bkActor, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return err
		}
		if e.eventIndexSnap == 0 {
			log.Printf("persist event snap 0, eventIndexSnap: %d", e.eventIndexSnap)
			if err := bkActor.Put([]byte(keyEventIndexSnapshot), []byte(strconv.Itoa(eventIndex))); err != nil {
				return err
			}
			e.eventIndexSnap = eventIndex
		}
		//log.Printf("persist event, eventIndexSnap: %d", e.eventIndexSnap)

		err = bkActor.Put([]byte(keyEventIndex), []byte(strconv.Itoa(eventIndex)))
		if err != nil {
			return err
		}
		bk, err := bkActor.CreateBucketIfNotExists(e.eventsBucket)
		if err != nil {
			return err
		}
		// if err := bk.Put([]byte(strconv.Itoa(eventIndex)), e.events[eventIndex]); err != nil {
		if err := bk.Put([]byte(strconv.Itoa(eventIndex)), []byte(evt)); err != nil {
			return err
		}
		//log.Printf("bucket: %s, eventIndex: %d", e.eventsBucket, eventIndex)
		return nil
	}); err != nil {
		log.Println(err)
	}
	e.eventIndex = eventIndex
}

//PersistSnapshot save snapshot, the snapshot is overwrite
func (provider *Boltdb) PersistSnapshot(actorName string,
	eventIndex int, snapshot proto.Message) {

	//log.Printf("index request Snapshot: %d", eventIndex)

	e, _ := provider.loadOrInit(actorName)
	provider.mu.Lock()
	defer provider.mu.Unlock()

	// log.Printf("len store: %d", len(e.events))

	// var err error
	snap, err := proto.Marshal(snapshot)
	if err != nil {
		log.Println(err)
		return
	}

	keyEvents := []byte(fmt.Sprintf("%s-%s", keyEventsPrefix, uuid.New()))
	if err := provider.db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return err
		}
		if err := bk.Put([]byte(keySnapshot), snap); err != nil {
			return err
		}
		if err := bk.Put([]byte(keyEventIndexSnapshot), []byte(strconv.Itoa(eventIndex))); err != nil {
			return err
		}

		//log.Printf("update snapshot")
		//log.Printf("update snapshot, eventsbucket: --------%s\n", e.eventsBucket)
		_, err = bk.CreateBucketIfNotExists(keyEvents)
		if err != nil {
			return err
		}
		lastEvents := e.eventsBucket
		// e.eventsBucket = keyEvents

		if lastEvents != nil {
			// log.Printf("update snapshot, delete lastEvents bucket: %s", lastEvents)
			bk.DeleteBucket(lastEvents)
		}
		e.eventsBucket = keyEvents
		e.lastEventsBucket = keyEvents

		err = bk.Put([]byte(keyLastEvents), keyEvents)
		if err != nil {
			return err
		}
		//log.Printf("delete snapshot")
		delete(provider.store, actorName)
		return nil
	}); err != nil {
		log.Println(err)
		return
	}

	e.eventIndexSnap = eventIndex

	// // TODO:
	// // safe memory allocation
	// if e != nil && len(e.events) > 512 {
	// 	log.Printf("delete store in-memory")
	// 	provider.store = make(map[string]*entry)
	// }
}

func (provider *Boltdb) DeleteEvents(actorName string, inclusiveToIndex int) {
	// e, _ := provider.loadOrInit(actorName)
	// //log.Printf("bucket in event: %s", e.eventsBucket)
	// provider.mu.Lock()
	// defer provider.mu.Unlock()

	// if err := provider.db.Update(func(tx *bolt.Tx) error {
	// 	bkActor, err := tx.CreateBucketIfNotExists([]byte(actorName))
	// 	if err != nil {
	// 		return err
	// 	}

	// 	bk := bkActor.Bucket(e.eventsBucket)
	// 	if bk == nil {
	// 		return bolt.ErrBucketNotFound
	// 	}

	// 	eventIndex := e.eventIndex
	// 	// if err := bk.Put([]byte(strconv.Itoa(eventIndex)), e.events[eventIndex]); err != nil {
	// 	if err := bk.Delete([]byte(strconv.Itoa(eventIndex))); err != nil {
	// 		return err
	// 	}
	// 	//log.Printf("bucket: %s, eventIndex: %d", e.eventsBucket, eventIndex)
	// 	return nil
	// }); err != nil {
	// 	log.Println(err)
	// }
}

func (db *Boltdb) DeleteSnapshots(actorName string, inclusiveToIndex int) {}
