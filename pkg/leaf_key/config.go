package leaf_key

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
	"github.com/vulcanize/ipld-eth-indexer/pkg/node"
	"github.com/vulcanize/ipld-eth-indexer/pkg/postgres"
	"github.com/vulcanize/ipld-eth-indexer/utils"
)

// Config used for configuring leaf key repair service
type Config struct {
	DB      *postgres.DB
	Start   uint64
	Stop    uint64
	State   bool
	Storage bool
}

const (
	REPAIR_MISSING_STATE_LEAF_KEYS   = "REPAIR_MISSING_STATE_LEAF_KEYS"
	REPAIR_MISSING_STORAGE_LEAF_KEYS = "REPAIR_MISSING_STORAGE_LEAF_KEYS"
	REPAIR_LEAF_KEYS_START_HEIGHT    = "REPAIR_LEAF_KEYS_START_HEIGHT"
	REPAIR_LEAF_KEYS_STOP_HEIGHT     = "REPAIR_LEAF_KEYS_STOP_HEIGHT"
)

// NewConfig is used to initialize a sync config from a .toml file
func NewConfig() (*Config, error) {
	c := new(Config)
	dbConfig := postgres.NewConfig()
	db := utils.LoadPostgres(dbConfig, node.Info{}, false)
	c.DB = &db

	start := viper.GetUint64(REPAIR_LEAF_KEYS_START_HEIGHT)
	stop := viper.GetUint64(REPAIR_LEAF_KEYS_STOP_HEIGHT)
	if stop < start {
		return nil, fmt.Errorf("stop height %d needs to be greater than or equal to the start height %d", stop, start)
	}
	c.Start = start
	c.Start = stop
	c.State = viper.GetBool(REPAIR_MISSING_STATE_LEAF_KEYS)
	c.Storage = viper.GetBool(REPAIR_MISSING_STORAGE_LEAF_KEYS)
	if !(c.State || c.Storage) {
		return nil, errors.New("neither state nor storage reparing is turned on")
	}
	return c, nil
}
