package accounting

import (
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
)

// TransferSnapshot represents state of an account at point in time.
type TransferSnapshot struct {
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	Bytes       int64     `json:"bytes"`
	Checked     bool      `json:"checked"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	Error       error     `json:"-"`
}

// MarshalJSON implements json.Marshaler interface.
func (as TransferSnapshot) MarshalJSON() ([]byte, error) {
	err := ""
	if as.Error != nil {
		err = as.Error.Error()
	}
	type Alias TransferSnapshot
	return json.Marshal(&struct {
		Error string `json:"error"`
		Alias
	}{
		Error: err,
		Alias: (Alias)(as),
	})
}

// Transfer keeps track of initiated transfers and provides access to
// accounting functions.
// Transfer needs to be closed on completion.
type Transfer struct {
	// these are initialised at creation and may be accessed without locking
	stats     *StatsInfo
	remote    string
	size      int64
	startedAt time.Time
	checking  bool

	// Protects all below
	mu          sync.RWMutex
	acc         *Account
	err         error
	completedAt time.Time
}

// newCheckingTransfer instantiates new checking of the object.
func newCheckingTransfer(stats *StatsInfo, obj fs.Object) *Transfer {
	return newTransferRemoteSize(stats, obj.Remote(), obj.Size(), true)
}

// newTransfer instantiates new transfer.
func newTransfer(stats *StatsInfo, obj fs.Object) *Transfer {
	return newTransferRemoteSize(stats, obj.Remote(), obj.Size(), false)
}

func newTransferRemoteSize(stats *StatsInfo, remote string, size int64, checking bool) *Transfer {
	tr := &Transfer{
		stats:     stats,
		remote:    remote,
		size:      size,
		startedAt: time.Now(),
		checking:  checking,
	}
	stats.AddTransfer(tr)
	return tr
}

// Done ends the transfer.
// Must be called after transfer is finished to run proper cleanups.
func (tr *Transfer) Done(err error) {
	tr.mu.Lock()

	if err != nil {
		tr.stats.Error(err)
		tr.err = err
	}
	if tr.acc != nil {
		if err := tr.acc.Close(); err != nil {
			fs.LogLevelPrintf(fs.Config.StatsLogLevel, nil, "can't close account: %+v\n", err)
		}
	}

	tr.completedAt = time.Now()

	tr.mu.Unlock()

	if tr.checking {
		tr.stats.DoneChecking(tr.remote)
	} else {
		tr.stats.DoneTransferring(tr.remote, err == nil)
	}
}

// Account returns reader that knows how to keep track of transfer progress.
func (tr *Transfer) Account(in io.ReadCloser) *Account {
	tr.mu.Lock()
	if tr.acc == nil {
		tr.acc = newAccountSizeName(tr.stats, in, tr.size, tr.remote)
	}
	tr.mu.Unlock()
	return tr.acc
}

// TimeRange returns the time transfer started and ended at. If not completed
// it will return zero time for end time.
func (tr *Transfer) TimeRange() (time.Time, time.Time) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return tr.startedAt, tr.completedAt
}

// IsDone returns true if transfer is completed.
func (tr *Transfer) IsDone() bool {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return !tr.completedAt.IsZero()
}

// Snapshot produces stats for this account at point in time.
func (tr *Transfer) Snapshot() TransferSnapshot {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	var s, b int64 = tr.size, 0
	if tr.acc != nil {
		b, s = tr.acc.progress()
	}
	return TransferSnapshot{
		Name:        tr.remote,
		Checked:     tr.checking,
		Size:        s,
		Bytes:       b,
		StartedAt:   tr.startedAt,
		CompletedAt: tr.completedAt,
		Error:       tr.err,
	}
}
