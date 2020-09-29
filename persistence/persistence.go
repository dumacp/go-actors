package persistence

import (
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
	log.Println("loadOrInit")
	// var e *entry
	// provider.mu.Lock()
	e := new(entry)
	defer func() {
		// provider.mu.Unlock()
		log.Printf("exit loadOrInit %s", e.eventsBucket)
	}()

	// funcLoad := func() (*entry, bool, error) {
	// provider.mu.RLock()
	// defer provider.mu.RUnlock()
	// var ok bool
	et, ok := provider.store[actorName]
	if ok {
		// if et != nil {
		log.Printf("exit loadOrInit before %s", et.eventsBucket)
		return et, true
		// }
	}

	// e.storeEvents = &entryEvents{}

	//	log.Printf("entry: 1--------%q\n", e)

	err := provider.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket([]byte(actorName))
		if bk == nil {
			return bolt.ErrBucketNotFound
		}

		//events bucket name
		keyEvents := bk.Get([]byte(keyLastEvents))
		if keyEvents == nil || len(keyEvents) <= 0 {
			return fmt.Errorf("empty events, keyLastEvents -> %s", keyLastEvents)
		}
		e.eventsBucket = keyEvents

		//eventsIndex
		eventIndex := bk.Get([]byte(keyEventIndex))
		if eventIndex == nil {
			return fmt.Errorf("empty events, keyEventIndex -> %s", keyEventIndex)
			// return errors.New("empty events")
		}
		num, err := strconv.Atoi(string(eventIndex))
		if err != nil {
			return err
		}
		e.eventIndex = num

		numSnap := 0
		//eventIndexSnap
		eventIndexSnap := bk.Get([]byte(keyEventIndexSnapshot))
		if eventIndexSnap != nil {

			numSnap, err = strconv.Atoi(string(eventIndexSnap))
			if err != nil {
				return err
			}
			log.Printf("loadOrInit, keyEventIndexSnapshot -> %s", eventIndexSnap)
			e.eventIndexSnap = numSnap
		}

		// //snapshot
		// snapshot := bk.Get([]byte("snapshot"))
		// if snapshot == nil {
		// 	log.Printf("empty snapshot")
		// } else {
		// 	e.snapshot = snapshot
		// }

		//events

		// bkEvents := bk.Bucket([]byte(keyEvents))
		// if bkEvents == nil {
		// 	return bolt.ErrBucketNotFound
		// }
		// log.Printf("bucket event: %s, %d, %d", keyEvents, numSnap, num)
		// events := make(map[int][]byte)
		// for i := numSnap; i <= num; i++ {
		// 	v := bkEvents.Get([]byte(strconv.Itoa(i)))
		// 	// log.Printf("event: %s", v)
		// 	if v == nil {
		// 		continue
		// 	}
		// 	events[i] = v
		// }
		// // e.storeEvents = &entryEvents{events}
		// log.Printf("events load")

		return nil
	})

	log.Printf("InitOrLoad, eventbucket -> %s", e.eventsBucket)

	if err == nil {
		// provider.store[actorName] = e
		return e, false
	}

	log.Printf("funcLoad error: %s", err)
	// return nil, false, err

	//	log.Printf("entry: 2--------%q\n", e)
	// return e, false, nil
	// }

	log.Println("loadOrInit 1")

	// if entry, _, err := funcLoad(); err == nil {

	//	log.Printf("entry: 3--------%q\n", entry)

	// if mem {
	// 	return entry, true
	// }

	// // provider.mu.Lock()
	// // defer provider.mu.Unlock()

	// if err := provider.db.Update(func(tx *bolt.Tx) error {
	// 	bk, err := tx.CreateBucketIfNotExists([]byte(actorName))
	// 	if err != nil {
	// 		return bolt.ErrBucketNotFound
	// 	}
	// 	keyEvents := []byte(fmt.Sprintf("%s-%s", keyEventsPrefix, uuid.New()))
	// 	_, err = bk.CreateBucketIfNotExists(keyEvents)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	lastEvents := make([]byte, len(entry.eventsBucket))
	// 	copy(lastEvents, entry.eventsBucket)
	// 	entry.eventsBucket = keyEvents

	// 	if lastEvents != nil {
	// 		bk.DeleteBucket(lastEvents)
	// 	}

	// 	err = bk.Put([]byte(keyLastEvents), keyEvents)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	return nil

	// }); err != nil {
	// 	return nil, false
	// }
	// provider.store[actorName] = *entry
	// log.Printf("load init entry: --------%#v\n", entry)
	// 	return entry, true
	// }

	// log.Println("loadOrInit 2")

	// provider.mu.Lock()
	// defer provider.mu.Unlock()

	err = provider.db.Update(func(tx *bolt.Tx) error {
		bkActor, err := tx.CreateBucketIfNotExists([]byte(actorName))
		if err != nil {
			return bolt.ErrBucketNotFound
		}

		lastEvents := []byte(fmt.Sprintf("%s-%s", keyEventsPrefix, uuid.New()))
		if err := bkActor.Put([]byte(keyLastEvents), lastEvents); err != nil {
			return err
		}
		log.Printf("bucket lastEvent: %s", lastEvents)
		e.eventsBucket = lastEvents
		return nil
	})

	if err != nil {
		log.Printf("erroroorrrr: %s", err)
		return nil, false
	}

	log.Println("loadOrInit 3")

	// e.storeEvents = e.storeEvents
	e.eventIndex = 0

	// provider.store[actorName] = e

	log.Println("loadOrInit 4")

	// log.Printf("load init entry final: --------%#v\n", e)
	return e, false
}

//Restart provider
func (provider *Boltdb) Restart() {
	// provider.store = make(map[string]*entry)
	log.Println("RESTART persistence provider")
}

//GetSnapshotInterval get snapshot interval in provider
func (provider *Boltdb) GetSnapshotInterval() int {
	log.Printf("GetSnapshotInterval -> %d", provider.snapshotInterval)
	interval := provider.snapshotInterval
	return interval
}

//GetSnapshot get last snapshot in provider for actor
func (provider *Boltdb) GetSnapshot(actorName string) (interface{}, int, bool) {
	log.Println("GetSnapshot")

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

	// provider.mu.Lock()
	// defer provider.mu.Unlock()
	// if !load && e.snapshot == nil {
	// 	return nil, 0, false
	// }
	log.Println("entry GetSnapshot: --------")

	dstCopy := make([]byte, len(snapshot))
	copy(dstCopy, snapshot)
	snap := provider.snapFunc(dstCopy)
	log.Printf("entry GetSnapshot: -------- indexSanp -> %d", e.eventIndexSnap)
	return snap, e.eventIndexSnap, true
}

//GetEvents get events for actor from eventIndexStart
func (provider *Boltdb) GetEvents(actorName string,
	eventIndexStart int, callback func(e interface{})) {
	log.Println("GetEvents")

	e, _ := provider.loadOrInit(actorName)
	provider.mu.Lock()
	defer provider.mu.Unlock()

	// events := make([][]byte, 0)
	// log.Printf("get Events, entry: --------%#v\n", e)
	// log.Printf("get Events, entry: --------%#v\n", e.events)
	// log.Printf("GetEvents: %d, %d, %d", e.eventIndex, e.eventIndexSnap, eventIndexStart)

	//events

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
		log.Printf("bucket event: %s, %d, %d, %d", keyEvents, eventIndexStart, e.eventIndexSnap, e.eventIndex)

		for i := eventIndexStart; i <= e.eventIndex; i++ {
			v := bkEvents.Get([]byte(strconv.Itoa(i)))
			// log.Printf("event: %s", v)
			if v == nil || len(v) <= 0 {
				log.Printf("event error: %s", v)
				continue
			}
			log.Printf("event get: %s\n\n\n\n", v)
			events[i] = make([]byte, 0)
			events[i] = append(events[i], v...)
		}
		return nil
	})

	if err != nil {
		log.Printf("error load event -> %s", err)
		return
	}
	// e.storeEvents = &entryEvents{events}
	log.Printf("events load")

	// for i := eventIndexStart; i <= e.eventIndex; i++ {
	// 	if _, ok := events[i]; !ok {
	// 		continue
	// 	}
	// 	ev := make([]byte, len(events[i]))
	// 	copy(ev, events[i])
	// 	events = append(events, ev)
	// }

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

	log.Printf("persist Events init ")
	if provider.store[actorName] != nil && provider.store[actorName].eventsBucket != nil {
		// log.Printf("bucket in event: %s", provider.store[actorName].eventsBucket)
	}
	e, _ := provider.loadOrInit(actorName)
	log.Printf("bucket in event: %s", e.eventsBucket)
	provider.mu.Lock()
	defer provider.mu.Unlock()

	// var err error
	// entry.events[eventIndex], err = proto.Marshal(event)
	evt, err := proto.Marshal(event)
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf("persist Events -> %s\n", evt)
	// e.events[eventIndex] = evt[:]
	// e.eventIndex = eventIndex

	log.Printf("persist Events, eventsbucket: --------%s\n", e.eventsBucket)
	// log.Printf("persist Events, event: --------%#v\n", entry.events)

	// provider.mu.Lock()
	// defer provider.mu.Unlock()

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
		log.Printf("persist event, eventIndexSnap: %d", e.eventIndexSnap)

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
		log.Printf("bucket: %s, eventIndex: %d",
			e.eventsBucket,
			eventIndex,
			// event,
		)
		return nil
	}); err != nil {
		log.Println(err)
	}
	e.eventIndex = eventIndex
}

//PersistSnapshot save snapshot, the snapshot is overwrite
func (provider *Boltdb) PersistSnapshot(actorName string,
	eventIndex int, snapshot proto.Message) {

	log.Printf("index request Snapshot: %d", eventIndex)

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

		log.Printf("update snapshot")
		log.Printf("update snapshot, eventsbucket: --------%s\n", e.eventsBucket)
		keyEvents := []byte(fmt.Sprintf("%s-%s", keyEventsPrefix, uuid.New()))
		_, err = bk.CreateBucketIfNotExists(keyEvents)
		if err != nil {
			return err
		}
		lastEvents := e.eventsBucket
		// e.eventsBucket = keyEvents

		if lastEvents != nil {
			bk.DeleteBucket(lastEvents)
		}

		err = bk.Put([]byte(keyLastEvents), keyEvents)
		if err != nil {
			return err
		}
		log.Printf("delete snapshot")
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
