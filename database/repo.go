package database

import (
	"bytes"
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

func PersistData(id string, data []byte, update bool, buckets ...string) func(*bbolt.Tx) error {
	return func(tx *bbolt.Tx) error {
		if len(buckets) <= 0 {
			return fmt.Errorf("empty buckets")
		}
		bkcll := tx.Bucket([]byte(buckets[0]))
		if len(buckets) > 1 {
			for _, bk := range buckets[1:] {
				var err error
				bkcll, err = bkcll.CreateBucketIfNotExists([]byte(bk))
				if err != nil {
					return err
				}
			}
		}

		// logs.LogBuild.Printf("salida 0 id: %s", id)
		if len(id) <= 0 {
			if uid, err := uuid.NewUUID(); err != nil {
				return err
			} else {
				id = uid.String()
			}
		}
		if !update {
			if v := bkcll.Get([]byte(id)); v != nil {
				return ErrDataUpdateNotAllow
			}
		}
		if err := bkcll.Put([]byte(id), data); err != nil {
			return err
		}
		return nil
	}
}

func GetData(callback func([]byte), id string, buckets ...string) func(*bbolt.Tx) error {
	return func(tx *bbolt.Tx) error {
		if len(buckets) <= 0 {
			return fmt.Errorf("empty buckets")
		}
		bkcll := tx.Bucket([]byte(buckets[0]))
		if len(buckets) > 1 {
			for _, bk := range buckets[1:] {
				bkcll = bkcll.Bucket([]byte(bk))
				if bkcll == nil {
					return bbolt.ErrBucketNotFound
				}
			}
		}

		if len(id) <= 0 {
			return bbolt.ErrKeyRequired
		}
		value := bkcll.Get([]byte(id))
		if value == nil {
			return nil
		}
		data := make([]byte, 0)
		data = append(data, value...)
		callback(data)

		return nil
	}
}

func RemoveData(id string, buckets ...string) func(*bbolt.Tx) error {
	return func(tx *bbolt.Tx) error {
		if len(buckets) <= 0 {
			return fmt.Errorf("empty buckets")
		}
		bkcll := tx.Bucket([]byte(buckets[0]))
		if len(buckets) > 1 {
			for _, bk := range buckets[1:] {
				bkcll = bkcll.Bucket([]byte(bk))
				if bkcll == nil {
					return bbolt.ErrBucketNotFound
				}
			}
		}

		if err := bkcll.Delete([]byte(id)); err != nil {
			return err
		}

		return nil
	}
}

type QueryType struct {
	Data []byte
	ID   string
}

func QueryData(ctx context.Context, callback func(data *QueryType), prefixID []byte, reverse bool, buckets ...string) func(*bbolt.Tx) error {
	return func(tx *bbolt.Tx) error {
		if len(buckets) <= 0 {
			return fmt.Errorf("empty buckets")
		}
		bkcll := tx.Bucket([]byte(buckets[0]))
		if len(buckets) > 1 {
			for _, bk := range buckets[1:] {
				bkcll = bkcll.Bucket([]byte(bk))
				if bkcll == nil {
					return nil
				}
			}
		}

		c := bkcll.Cursor()
		if len(prefixID) > 0 {
			if reverse {
				c.Seek(prefixID)
				for k, v := c.Last(); k != nil && bytes.HasPrefix(k, prefixID); k, v = c.Prev() {
					// fmt.Printf("key=%s, value=%s\n", k, v)
					select {
					case <-ctx.Done():
						fmt.Printf("close query datadb")
						return nil
					default:
						callback(&QueryType{ID: string(k), Data: v})
					}
				}
			} else {
				for k, v := c.Seek(prefixID); k != nil && bytes.HasPrefix(k, prefixID); k, v = c.Next() {
					// fmt.Printf("key=%s, value=%s\n", k, v)
					select {
					case <-ctx.Done():
						fmt.Printf("close query datadb")
						return nil
					default:
						callback(&QueryType{ID: string(k), Data: v})
					}
				}
			}
		} else {
			if reverse {
				for k, v := c.Last(); k != nil; k, v = c.Prev() {
					// fmt.Printf("key=%s, value=%s\n", k, v)
					select {
					case <-ctx.Done():
						fmt.Printf("close query datadb")
						return nil
					default:
						callback(&QueryType{ID: string(k), Data: v})
					}
				}
			} else {
				for k, v := c.First(); k != nil; k, v = c.Next() {
					// fmt.Printf("key=%s, value=%s\n", k, v)
					select {
					case <-ctx.Done():
						fmt.Printf("close query datadb")
						return nil
					default:
						callback(&QueryType{ID: string(k), Data: v})
					}
				}
			}
		}
		return nil
	}
}

func Last(callback func([]byte), prefixID []byte, buckets ...string) func(*bbolt.Tx) error {
	return func(tx *bbolt.Tx) error {
		if len(buckets) <= 0 {
			return fmt.Errorf("empty buckets")
		}
		bkcll := tx.Bucket([]byte(buckets[0]))
		if len(buckets) > 1 {
			for _, bk := range buckets[1:] {
				bkcll = bkcll.Bucket([]byte(bk))
				if bkcll == nil {
					return bbolt.ErrBucketNotFound
				}
			}
		}
		c := bkcll.Cursor()
		var k, v []byte
		if len(prefixID) > 0 {
			for k, v = c.Seek(prefixID); k != nil && bytes.HasPrefix(k, prefixID); k, v = c.Next() {
				// fmt.Printf("key=%s, value=%s\n", k, v)
			}
		} else {
			for k, v = c.First(); k != nil; k, v = c.Next() {
				// fmt.Printf("key=%s, value=%s\n", k, v)
			}
		}
		callback(v)
		return nil
	}
}

func List(callback func(data map[string][]byte)) func(*bbolt.Tx) error {
	return func(tx *bbolt.Tx) error {
		c := tx.Cursor()
		collections := make(map[string][]byte, 0)
		k, _ := c.First()

		for {
			if len(k) <= 0 {
				break
			}
			bk := tx.Bucket(k)
			if bk == nil {
				break
			}
			cbk := bk.Cursor()
			ki, _ := cbk.First()
			for {
				if len(ki) > 0 {
					collections[string(ki)] = k
				} else {
					break
				}
				ki, _ = cbk.Next()
			}
			k, _ = c.Next()
		}

		callback(collections)
		return nil
	}
}

func ListKeys(callback func(list [][]byte), buckets ...string) func(*bbolt.Tx) error {
	return func(tx *bbolt.Tx) error {
		if len(buckets) <= 0 {
			return fmt.Errorf("empty buckets")
		}
		bkcll := tx.Bucket([]byte(buckets[0]))
		if len(buckets) > 1 {
			for _, bk := range buckets[1:] {
				bkcll = bkcll.Bucket([]byte(bk))
				if bkcll == nil {
					return bbolt.ErrBucketNotFound
				}
			}
		}

		collections := make([][]byte, 0)

		cbk := bkcll.Cursor()
		ki, _ := cbk.First()
		for {
			if len(ki) > 0 {
				collections = append(collections, ki)
			} else {
				break
			}
			ki, _ = cbk.Next()
		}

		callback(collections)

		return nil
	}
}
