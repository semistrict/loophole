package command

import (
	"os"

	bolt "go.etcd.io/bbolt"
)

// OpenDB is a helper that opens a bbolt database from a file path.
func OpenDB(path string, mode os.FileMode, options *bolt.Options) (*bolt.DB, error) {
	readOnly := false
	if options != nil {
		readOnly = options.ReadOnly
	}
	data, err := bolt.OpenFileData(path, mode, readOnly)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(data, options)
	if err != nil {
		data.Close()
		return nil, err
	}
	return db, nil
}
