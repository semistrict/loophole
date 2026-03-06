package sqlitevfs

// SyncMode controls how xSync behaves.
type SyncMode int

const (
	// SyncModeSync flushes to S3 on every xSync call.
	SyncModeSync SyncMode = 0
	// SyncModeAsync makes xSync a no-op; data is flushed on a schedule.
	SyncModeAsync SyncMode = 1
)
