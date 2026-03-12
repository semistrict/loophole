package loophole

// Instance identifies a logical loophole store with all connection parameters.
type Instance struct {
	ProfileName       string // profile name from config; used for naming sockets, logs, etc.
	Bucket            string
	Prefix            string
	Endpoint          string // S3 endpoint URL; empty = AWS default
	LocalDir          string // non-empty = local FileStore at this path
	AccessKey         string
	SecretKey         string
	Region            string
	Mode              Mode
	DefaultFSType     FSType // default FS for new volumes; empty = ext4
	NBDSocket         string // if set, auto-start NBD server on this path
	SnapshotterSocket string // if set, start gRPC snapshotter on this path
	LogLevel          string // "debug", "info", "warn", "error"; empty = "info"
	SandboxMode       string // sandbox runtime for /sandbox/exec; empty = DefaultSandboxMode()
	DaemonURL         string // remote daemon base URL (e.g. https://cf-demo.example.com)
}

// URL returns the canonical URL string for this instance.
// For local stores it returns "local:/path", for S3 it returns "s3://bucket/prefix".
func (inst Instance) URL() string {
	if inst.LocalDir != "" {
		return "local:" + inst.LocalDir
	}
	if inst.Prefix == "" {
		return "s3://" + inst.Bucket
	}
	return "s3://" + inst.Bucket + "/" + inst.Prefix
}
