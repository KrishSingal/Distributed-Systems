package pbservice

import "hash/fnv"

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongServer      = "ErrWrongServer"
	ErrDuplicateRequest = "ErrDuplicateRequest"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.

	RequestID int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestID int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type ForwardArgs struct {
	ClaimedPrimary string
	Operation      string
	Key            string
	Value          string
	DoHash         bool // For PutHash
	// You'll have to add definitions here.

	RequestID int64
}

type ForwardReply struct {
	Err Err
}

type InitializationArgs struct {
	Me           string
	KVStore      map[string]string
	RequestCache map[int64]bool
}

type InitializationReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
