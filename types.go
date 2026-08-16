package memcache

import "time"

// Expiration is sent using memcached's TTL rules. ExpiresIn uses relative
// seconds through 30 days and automatically converts longer durations to an
// absolute timestamp; ExpiresAt can be used explicitly.
type Expiration int64

const (
	NoExpiration          Expiration = 0
	maxRelativeExpiration            = 30 * 24 * time.Hour
)

// ExpiresIn converts a relative duration to whole seconds, rounding a
// positive sub-second duration up to one second.
func ExpiresIn(d time.Duration) Expiration {
	if d < 0 {
		seconds := d / time.Second
		if seconds == 0 {
			seconds = -1
		}
		return Expiration(seconds)
	}
	if d == 0 {
		return Expiration(d / time.Second)
	}
	if d > maxRelativeExpiration {
		return ExpiresAt(time.Now().Add(d))
	}
	seconds := (d + time.Second - 1) / time.Second
	return Expiration(seconds)
}

// ExpiresAt converts a timestamp to memcached's absolute Unix-time form.
func ExpiresAt(t time.Time) Expiration { return Expiration(t.Unix()) }

// ResponseCode is the two-byte meta protocol result code.
type ResponseCode string

const (
	ResponseHeader    ResponseCode = "HD"
	ResponseValue     ResponseCode = "VA"
	ResponseMiss      ResponseCode = "EN"
	ResponseNotStored ResponseCode = "NS"
	ResponseExists    ResponseCode = "EX"
	ResponseNotFound  ResponseCode = "NF"
	ResponseNoop      ResponseCode = "MN"
	ResponseDebug     ResponseCode = "ME"
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

// GetStatus describes whether and how a meta get resolved.
type GetStatus uint8

const (
	GetUnknown GetStatus = iota
	GetHit
	GetMiss
	GetPending
	GetUnchanged
)

// ValueState distinguishes fresh, stale, and lease-placeholder values.
type ValueState uint8

const (
	ValueUnknown ValueState = iota
	ValueFresh
	ValueStale
	ValueMissing
)

// LeaseState reports ownership of a vivify or early-refresh lease.
type LeaseState uint8

const (
	LeaseNone LeaseState = iota
	LeaseGranted
	LeaseBusy
)

// GetResult describes a meta get without collapsing protocol states into an
// error. Value is present only for a value-bearing hit.
type GetResult struct {
	Key         string
	Status      GetStatus
	Value       []byte
	Metadata    Metadata
	ValueState  ValueState
	Lease       LeaseState
	ReturnedKey []byte
	Opaque      string
}

func (r GetResult) Hit() bool { return r.Status == GetHit }

// MutationStatus describes a conditional set, delete, or arithmetic outcome.
type MutationStatus uint8

const (
	MutationUnknown MutationStatus = iota
	MutationApplied
	MutationNotFound
	MutationAlreadyExists
	MutationCASMismatch
)

// MutationResult describes a set or delete outcome.
type MutationResult struct {
	Key         string
	Status      MutationStatus
	CAS         *uint64
	Size        *uint64
	ReturnedKey []byte
	Opaque      string
}

func (r MutationResult) Applied() bool { return r.Status == MutationApplied }

// ArithmeticResult describes an increment or decrement outcome.
type ArithmeticResult struct {
	Key         string
	Status      MutationStatus
	Value       uint64
	HasValue    bool
	Metadata    Metadata
	ReturnedKey []byte
	Opaque      string
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

// MetaGetOptions exposes meta get behavior. Zero values perform a normal value
// read. Set MetadataOnly for an explicit metadata-only operation. When
// VivifyTTL and RefreshBefore are combined, the wire protocol cannot
// distinguish a real empty value won for early refresh from a newly vivified
// empty placeholder. Value-bearing reads use the reference clients' empty
// value heuristic; metadata-only reads reject that combination.
type MetaGetOptions struct {
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

// MetaSetOptions exposes meta set behavior.
type MetaSetOptions struct {
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

// MetaDeleteOptions exposes meta delete behavior.
type MetaDeleteOptions struct {
	CompareCAS *uint64
	SetCAS     *uint64
	Invalidate bool
	StaleFor   *Expiration
	DropValue  bool
	ReturnKey  bool
	Opaque     string
}

// MetaArithmeticOptions exposes meta arithmetic behavior.
type MetaArithmeticOptions struct {
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
	parseErr error
}

func ptr[T any](value T) *T { return &value }
