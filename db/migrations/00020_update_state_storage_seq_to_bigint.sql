-- +goose Up
ALTER SEQUENCE eth.state_cids_id_seq as BIGINT NO MAXVALUE;
ALTER SEQUENCE eth.storage_cids_id_seq as BIGINT NO MAXVALUE;

-- +goose Down
ALTER SEQUENCE eth.storage_cids_id_seq as INT NO MAXVALUE;
ALTER SEQUENCE eth.state_cids_id_seq as INT NO MAXVALUE;