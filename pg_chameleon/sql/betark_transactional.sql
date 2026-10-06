-- Installed only with a newly created catalogue in transactional migration mode.
CREATE VIEW sch_chameleon.v_betark_transactional_version AS SELECT 1::integer AS version;

CREATE TABLE sch_chameleon.t_transaction_state (
    i_id_source bigint PRIMARY KEY REFERENCES sch_chameleon.t_sources ON DELETE CASCADE,
    source_uuid uuid NOT NULL,
    config_fingerprint text NOT NULL CHECK (length(config_fingerprint) = 64),
    capture_file text NOT NULL,
    capture_position bigint NOT NULL CHECK (capture_position > 0),
    capture_gtid_set text NOT NULL,
    applied_file text NOT NULL,
    applied_position bigint NOT NULL CHECK (applied_position > 0),
    applied_gtid_set text NOT NULL
);

ALTER TABLE sch_chameleon.t_replica_batch ADD COLUMN t_source_uuid uuid;
ALTER TABLE sch_chameleon.t_replica_batch ADD COLUMN t_source_gtid text;
CREATE UNIQUE INDEX idx_replica_batch_source_gtid
    ON sch_chameleon.t_replica_batch(i_id_source,t_source_uuid,t_source_gtid)
    WHERE t_source_gtid IS NOT NULL;

-- The durable order is the source-lock allocation order, not wall-clock time.
DROP INDEX sch_chameleon.idx_t_replica_batch_ts_created;
CREATE INDEX idx_t_replica_batch_ts_created
    ON sch_chameleon.t_replica_batch(i_id_source,ts_created);
