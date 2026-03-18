package env

// ResolvedProfile identifies a logical loophole store with all connection parameters.
type ResolvedProfile struct {
	ProfileName string // profile name from config; used for naming sockets, logs, etc.
	Bucket      string
	Prefix      string
	Endpoint    string // S3 endpoint URL; empty = AWS default
	LocalDir    string // non-empty = local FileStore at this path
	AccessKey   string
	SecretKey   string
	Region      string
	LogLevel    string // "debug", "info", "warn", "error"; empty = "info"
	DaemonURL   string // remote daemon base URL (e.g. https://cf-demo.example.com)
}

// URL returns the canonical URL string for this instance.
// For local stores it returns "local:/path", for S3 it returns "s3://bucket/prefix".
func (inst ResolvedProfile) URL() string {
	if inst.LocalDir != "" {
		return "local:" + inst.LocalDir
	}
	if inst.Prefix == "" {
		return "s3://" + inst.Bucket
	}
	return "s3://" + inst.Bucket + "/" + inst.Prefix
}
