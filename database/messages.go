package database

type MsgOpenDB struct{}
type MsgOpenedDB struct{}
type MsgCloseDB struct{}
type MsgErrorDB struct{}
type MsgUpdateData struct {
	ID      string
	Data    []byte
	Buckets []string
}
type MsgDeleteData struct {
	ID      string
	Buckets []string
}
type MsgInsertData struct {
	ID      string
	Data    []byte
	Buckets []string
}
type MsgGetData struct {
	Buckets []string
	ID      string
}
type MsgQueryData struct {
	Buckets  []string
	PrefixID string
	Reverse  bool
}
type MsgQueryNext struct{}
type MsgQueryResponse struct {
	ID      string
	Buckets []string
	Data    []byte
}
type MsgAckPersistData struct {
	Bucktes []string
	ID      string
}
type MsgAckDeleteData struct {
	Buckets []string
	ID      string
}
type MsgNoAckPersistData struct {
	Error string
}
type MsgNoAckDeleteData struct {
	Error string
}

type MsgAckGetData struct {
	Data []byte
}
type MsgNoAckGetData struct {
	Error string
}

type MsgList struct {
}

type MsgAckList struct {
	Data map[string][]byte
}
type MsgNoAckList struct {
	Error string
}

type MsgListKeys struct {
	Buckets []string
}

type MsgAckListKeys struct {
	Data [][]byte
}
type MsgNoAckListKyes struct {
	Error string
}

type MsgFlushFilesystem struct{}
