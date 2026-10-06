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

ALTER FUNCTION sch_chameleon.fn_replay_mysql(integer,integer,boolean)
    RENAME TO fn_replay_mysql_upstream;

CREATE FUNCTION sch_chameleon.fn_replay_mysql(p_max_events integer,p_source integer,p_exit boolean)
RETURNS sch_chameleon.ty_replay_status LANGUAGE plpgsql AS $BODY$
DECLARE
    v_batch sch_chameleon.t_replica_batch%ROWTYPE;
    v_status sch_chameleon.ty_replay_status := (false,false,NULL);
    v_complete boolean;
    v_expected bigint;
    v_message text;
BEGIN
    -- Shared source lock serialises publisher/replayers; no SKIP LOCKED/overtaking.
    PERFORM 1 FROM sch_chameleon.t_sources WHERE i_id_source=p_source FOR UPDATE;
    PERFORM 1 FROM sch_chameleon.t_transaction_state WHERE i_id_source=p_source FOR UPDATE;
    IF NOT FOUND THEN
        RETURN sch_chameleon.fn_replay_mysql_upstream(p_max_events,p_source,p_exit);
    END IF;
    IF p_exit IS DISTINCT FROM true OR p_max_events IS NULL OR p_max_events<1 THEN
        RAISE EXCEPTION 'Transactional replay requires exit-on-error and a positive replay budget';
    END IF;
    SELECT * INTO v_batch FROM sch_chameleon.t_replica_batch
        WHERE i_id_source=p_source AND b_started AND b_processed AND NOT b_replayed
        ORDER BY i_id_batch LIMIT 1 FOR UPDATE;
    IF NOT FOUND THEN
        RETURN v_status;
    END IF;
    IF v_batch.t_source_gtid IS NULL THEN
        RAISE EXCEPTION 'Incomplete/non-transactional batch in migration queue';
    END IF;
    BEGIN
        SELECT cardinality(i_id_event) INTO v_expected FROM sch_chameleon.t_batch_events
            WHERE i_id_batch=v_batch.i_id_batch;
        IF v_expected IS NULL THEN
            RAISE EXCEPTION 'Missing complete source transaction event queue';
        END IF;
        IF EXISTS (
            SELECT 1 FROM sch_chameleon.t_log_replica log
            LEFT JOIN sch_chameleon.t_replica_tables tab
                ON tab.i_id_source=p_source AND tab.v_schema_name=log.v_schema_name AND tab.v_table_name=log.v_table_name
            WHERE log.i_id_batch=v_batch.i_id_batch
                AND (log.enm_binlog_event='ddl' OR tab.i_id_table IS NULL OR NOT tab.b_replica_enabled)
        ) THEN
            RAISE EXCEPTION 'Disabled/missing table metadata or DDL in migration queue';
        END IF;
        LOOP
            v_status := sch_chameleon.fn_replay_mysql_upstream(p_max_events,p_source,true);
            IF v_status.b_error THEN
                RAISE EXCEPTION 'Source transaction replay failed';
            END IF;
            SELECT b_replayed INTO STRICT v_complete FROM sch_chameleon.t_replica_batch
                WHERE i_id_batch=v_batch.i_id_batch;
            EXIT WHEN v_complete;
        END LOOP;
        IF (SELECT coalesce(i_replayed,0) FROM sch_chameleon.t_replica_batch
            WHERE i_id_batch=v_batch.i_id_batch) <> v_expected THEN
            RAISE EXCEPTION 'Source transaction events were skipped';
        END IF;
        UPDATE sch_chameleon.t_transaction_state
            SET applied_file=v_batch.t_binlog_name,applied_position=v_batch.i_binlog_position,applied_gtid_set=v_batch.t_gtid_set
            WHERE i_id_source=p_source;
        v_status.b_continue := EXISTS (
            SELECT 1 FROM sch_chameleon.t_replica_batch
            WHERE i_id_source=p_source AND b_started AND b_processed AND NOT b_replayed);
        RETURN v_status;
    EXCEPTION WHEN OTHERS THEN
        -- All DML, upstream queue mutations and frontier updates above roll back.
        GET STACKED DIAGNOSTICS v_message=MESSAGE_TEXT;
        INSERT INTO sch_chameleon.t_error_log
            (i_id_batch,i_id_source,v_table_name,v_schema_name,t_table_pkey,t_binlog_name,
             i_binlog_position,ts_error,t_error_message)
            VALUES(v_batch.i_id_batch,p_source,'transaction','sch_chameleon',v_batch.t_source_gtid,
                   v_batch.t_binlog_name,v_batch.i_binlog_position,clock_timestamp(),
                   coalesce(array_to_string(v_status.v_table_error,'; '),v_message));
        v_status.b_continue := false;
        v_status.b_error := true;
        IF v_status.v_table_error IS NULL THEN
            v_status.v_table_error := ARRAY[v_message];
        END IF;
        RETURN v_status;
    END;
END;
$BODY$;
