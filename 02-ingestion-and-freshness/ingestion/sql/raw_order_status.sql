-- WRITE step for CDC: one Debezium change file (flattened by ExtractNewRecordState).
--   __op           c = create, u = update, d = delete  (r = snapshot read, not used here)
--   __source_ts_ms when the change was committed in the source database (epoch millis, UTC)
--   __lsn          the database log position: the true order of changes
-- Keep only the LATEST change per order, ordered by __lsn, not by time:
-- two changes can share a millisecond, but never a log position.
CREATE OR REPLACE TEMP TABLE stg_raw_order_status AS
SELECT order_id, status, __op, __lsn, epoch_ms(__source_ts_ms) AS __source_ts
FROM read_csv($source_file)
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY __lsn DESC) = 1;
