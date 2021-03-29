package leaf_key

import "github.com/vulcanize/ipld-eth-indexer/pkg/postgres"

const (
	statePgStr   = `PERFORM add_state_leaf_key($1)`
	storagePgStr = `PERFORM add_storage_leaf_key($1)`
)

// Writer for writing missing leaf keys in the database
type Writer interface {
	WriteState(height uint64) error
	WriteStorage(height uint64) error
}

// LeafKeyWriter struct underpinnng the Writer interface
type LeafKeyWriter struct {
	db      *postgres.DB
	current uint64
}

// NewLeafKeyWriter constructs and returns a new leaf_key.Writer
func NewLeafKeyWriter(db *postgres.DB) Writer {
	return &LeafKeyWriter{
		db:      db,
		current: 0,
	}
}

// WriteState writes any missing leaf keys for `removed node` state_cids entries at the provided height
func (lkw *LeafKeyWriter) WriteState(height uint64) error {
	if _, err := lkw.db.Exec(statePgStr, height); err != nil {
		return err
	}
	return nil
}

// WriteStorage writes any missing leaf keys for `removed node` storage_cids entries at the provided height
func (lkw *LeafKeyWriter) WriteStorage(height uint64) error {
	if _, err := lkw.db.Exec(storagePgStr, height); err != nil {
		return err
	}
	return nil
}
