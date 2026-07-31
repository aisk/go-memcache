package memcache

import "time"

// Expiration is sent using memcached's TTL rules. Durations at or below 30
// days are relative; later times should be supplied with ExpiresAt.
type Expiration int64

const (
	NoExpiration          Expiration = 0
	maxRelativeExpiration            = 30 * 24 * time.Hour
)

// ExpiresIn converts a relative duration to whole seconds, rounding a
// positive sub-second duration up to one second.
func ExpiresIn(d time.Duration) Expiration {
	if d <= 0 {
		return Expiration(d / time.Second)
	}
	seconds := (d + time.Second - 1) / time.Second
	return Expiration(seconds)
}

// ExpiresAt converts a timestamp to memcached's absolute Unix-time form.
func ExpiresAt(t time.Time) Expiration { return Expiration(t.Unix()) }

// ResponseCode is the two-byte meta protocol result code.
type ResponseCode string

const (
	ResponseHeader   ResponseCode = "HD"
	ResponseValue    ResponseCode = "VA"
	ResponseMiss     ResponseCode = "EN"
	ResponseNotStore ResponseCode = "NS"
	ResponseExists   ResponseCode = "EX"
	ResponseNotFound ResponseCode = "NF"
	ResponseNoop     ResponseCode = "MN"
	ResponseDebug    ResponseCode = "ME"
)

// Metadata contains values requested from a meta get/arithmetic response.
// Pointer fields distinguish a returned zero from a field not requested.
type Metadata struct {
	CAS         *uint64
	TTL         *int64
	Size        *uint64
	ClientFlags *uint32
	LastAccess  *uint64
	HitBefore   *bool
}

type GetStatus uint8

const (
	GetHit GetStatus = iota
	GetMiss
	GetPending
	GetUnchanged
)

type ValueState uint8

const (
	ValueFresh ValueState = iota
	ValueStale
	ValueMissing
)

type LeaseState uint8

const (
	LeaseNone LeaseState = iota
	LeaseGranted
	LeaseBusy
)

// GetResult describes a meta get without collapsing protocol states into an
// error. Value is present only for a value-bearing hit.
type GetResult struct {
	Key        string
	Status     GetStatus
	Value      []byte
	Metadata   Metadata
	ValueState ValueState
	Lease      LeaseState
}

func (r GetResult) Hit() bool { return r.Status == GetHit }

type MutationStatus uint8

const (
	MutationApplied MutationStatus = iota
	MutationNotFound
	MutationAlreadyExists
	MutationCASMismatch
)

// MutationResult describes a set or delete outcome.
type MutationResult struct {
	Key    string
	Status MutationStatus
	CAS    *uint64
}

func (r MutationResult) Applied() bool { return r.Status == MutationApplied }

// ArithmeticResult describes an increment or decrement outcome.
type ArithmeticResult struct {
	Key      string
	Status   MutationStatus
	Value    uint64
	HasValue bool
	Metadata Metadata
}

// StoreMode selects meta set's M flag.
type StoreMode uint8

const (
	ModeSet StoreMode = iota
	ModeAdd
	ModeReplace
	ModeAppend
	ModePrepend
)

// GetOptions exposes meta get behavior. Zero values perform a normal value
// read. Set MetadataOnly for an explicit metadata-only operation.
type GetOptions struct {
	MetadataOnly      bool
	ReturnCAS         bool
	ReturnTTL         bool
	ReturnSize        bool
	ReturnLastAccess  bool
	ReturnHitBefore   bool
	ReturnClientFlags bool
	ReturnKey         bool
	Touch             *Expiration
	VivifyTTL         *Expiration
	RefreshBefore     *Expiration
	UnlessCAS         *uint64
	SetCAS            *uint64
	NoLRUBump         bool
	Opaque            string
}

// SetOptions exposes meta set behavior.
type SetOptions struct {
	TTL         Expiration
	ClientFlags uint32
	Mode        StoreMode
	CompareCAS  *uint64
	SetCAS      *uint64
	Invalidate  bool
	VivifyTTL   *Expiration
	ReturnCAS   bool
	ReturnSize  bool
	ReturnKey   bool
	Opaque      string
}

// DeleteOptions exposes meta delete behavior.
type DeleteOptions struct {
	CompareCAS *uint64
	SetCAS     *uint64
	Invalidate bool
	StaleFor   *Expiration
	DropValue  bool
	ReturnKey  bool
	Opaque     string
}

// ArithmeticOptions exposes meta arithmetic behavior.
type ArithmeticOptions struct {
	Delta        uint64
	Decrement    bool
	Initial      *uint64
	InitialTTL   *Expiration
	Touch        *Expiration
	CompareCAS   *uint64
	SetCAS       *uint64
	MetadataOnly bool
	ReturnTTL    bool
	ReturnCAS    bool
	ReturnKey    bool
	Opaque       string
}

// RawResponse is a lightly parsed response from ExecuteMeta.
type RawResponse struct {
	Code     ResponseCode
	Value    []byte
	Metadata Metadata
	Key      []byte
	Opaque   string
	Won      bool
	Busy     bool
	Stale    bool
	Flags    []string
	Debug    map[string]string
}

func ptr[T any](value T) *T { return &value }
