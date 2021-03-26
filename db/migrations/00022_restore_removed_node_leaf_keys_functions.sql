-- +goose Up
-- +goose StatementBegin
-- returns all of the state_cid entries for pruned nodes
CREATE OR REPLACE FUNCTION collect_removed_state(height BIGINT) RETURNS setof eth.state_cids
AS $$
  SELECT state_cids.* FROM eth.state_cids
  INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
  WHERE block_number = height
  AND node_type = 3;
$$ LANGUAGE SQL;
-- +goose StatementEnd

-- +goose StatementBegin
-- returns all of the storage_cid entries for pruned nodes
CREATE OR REPLACE FUNCTION collect_removed_storage(height BIGINT) RETURNS setof eth.storage_cids
AS $$
  SELECT storage_cids.* FROM eth.storage_cids
  INNER JOIN eth.state_cids ON (storage_cids.state_id = state_cids.id)
  INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
  WHERE block_number = height
  AND storage_cids.node_type = 3;
$$ LANGUAGE SQL;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION find_state_leaf_key(state_path BYTEA, height BIGINT) RETURNS VARCHAR(66)
AS $$
  SELECT state_leaf_key FROM eth.state_cids
  INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
  WHERE block_number < height
  AND header_cids.id = canonical_header_id(height)
  AND state_cids.state_path = state_path
ORDER BY block_number DESC LIMIT 1;
$$ LANGUAGE SQL;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION find_storage_leaf_key(storage_path BYTEA, state_leaf_key VARCHAR(66), height BIGINT) RETURNS VARCHAR(66)
AS $$
  SELECT storage_leaf_key FROM eth.storage_cids
  INNER JOIN eth.state_cids ON (storage_cids.state_id = state_cids.id)
  INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.id)
  WHERE block_number < height
  AND header_cids.id = canonical_header_id(height)
  AND state_cids.state_leaf_key = state_leaf_key
  AND storage_cids.storage_path = storage_path
ORDER BY block_number DESC LIMIT 1;
$$ LANGUAGE SQL;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION add_state_leaf_key(height BIGINT) RETURNS void
AS $BODY$
DECLARE
  temp_state_row eth.state_cids;
  leaf_key VARCHAR(66);
BEGIN
  FOR temp_state_row IN
  SELECT * FROM collect_removed_state(height)
  LOOP
    leaf_key = find_state_leaf_key(temp_state_row.state_path, height);
    UPDATE eth.state_cids
    SET state_leaf_key = leaf_key
    WHERE id = temp_state_row.id;
  END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION add_storage_leaf_key(height BIGINT) RETURNS void
AS $BODY$
DECLARE
  temp_storage_row eth.storage_cids;
  leaf_key VARCHAR(66);
BEGIN
  FOR temp_storage_row IN
  SELECT * FROM collect_removed_storage(height)
  LOOP
    leaf_key = find_storage_leaf_key(temp_storage_row.storage_path, height);
    UPDATE eth.storage_cids
    SET storage_leaf_key = leaf_key
    WHERE id = temp_storage_row.id;
  END LOOP;
END;
$BODY$
LANGUAGE 'plpgsql';
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION add_storage_leaf_key;
DROP FUNCTION add_state_leaf_key;
DROP FUNCTION collect_removed_storage;
DROP FUNCTION collect_removed_state;
DROP FUNCTION find_storage_leaf_key;
DROP FUNCTION find_state_leaf_key;