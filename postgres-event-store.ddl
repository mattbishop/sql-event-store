-- Postgres event store

CREATE TABLE ledger
(
    entity      TEXT        NOT NULL,
    entity_key  TEXT        NOT NULL,
    event       TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    -- can be anything, like a ULID, nanoid, etc.
    append_key  TEXT        NOT NULL UNIQUE,
    -- previous event id
    -- null for first event in entity instance; null does not trigger UNIQUE constraint
    previous_id UUID        UNIQUE,
    event_id    UUID        NOT NULL UNIQUE,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- sequence for all events in all entities
    sequence   BIGSERIAL    PRIMARY KEY
);

CREATE INDEX entity_index ON ledger (entity, entity_key);


-- immutable events
CREATE RULE ignore_delete_events AS ON DELETE TO ledger
    DO INSTEAD NOTHING;

CREATE RULE ignore_update_events AS ON UPDATE TO ledger
    DO INSTEAD NOTHING;


CREATE FUNCTION append_event(entity_in          TEXT,
                             entity_key_in      TEXT,
                             event_in           TEXT,
                             data_in            JSONB,
                             append_key_in      TEXT,
                             previous_id_in     UUID DEFAULT NULL)
RETURNS UUID AS
$$
    INSERT INTO ledger (entity, entity_key, event, data, append_key, previous_id)
    VALUES (entity_in, entity_key_in, event_in, data_in, append_key_in, previous_id_in)
    RETURNING event_id;
$$
LANGUAGE sql;



CREATE VIEW replay_events AS
SELECT
    entity,
    entity_key,
    event,
    data,
    timestamp,
    event_id
FROM ledger ORDER BY sequence;


CREATE FUNCTION replay_events_after(after_event_id UUID)
    RETURNS SETOF replay_events AS
$$
DECLARE
    after_sequence BIGINT;
BEGIN
    -- Find the sequence number of the specified event_id
    SELECT l.sequence INTO after_sequence
    FROM ledger l
    WHERE l.event_id = after_event_id;

    -- If event_id doesn't exist, raise an error
    IF after_sequence IS NULL THEN
        RAISE EXCEPTION 'Event with ID % does not exist', after_event_id;
    END IF;

    -- Return all events with a higher sequence number
    RETURN QUERY
        SELECT
            l.entity,
            l.entity_key,
            l.event,
            l.data,
            l.timestamp,
            l.event_id
        FROM ledger l
        WHERE l.sequence > after_sequence
        ORDER BY l.sequence;
END
$$
LANGUAGE plpgsql;



-- Generates a UUID for each new event.
CREATE FUNCTION generate_event_id() RETURNS trigger AS
$$
BEGIN
    IF (NEW.event_id IS NOT NULL)
    THEN
        RAISE EXCEPTION 'event_id must not be directly set with INSERT statement, it is generated';
    END IF;
    NEW.event_id = gen_random_uuid();
    RETURN NEW;
END
$$
LANGUAGE plpgsql;


CREATE TRIGGER generate_event_id_on_append
    BEFORE INSERT
    ON ledger
    FOR EACH ROW
    EXECUTE PROCEDURE generate_event_id();



CREATE FUNCTION check_first_event_for_entity() RETURNS trigger AS
$$
BEGIN
    IF EXISTS (SELECT true
               FROM ledger
               WHERE NEW.entity_key = entity_key
                 AND NEW.entity = entity)
    THEN
        RAISE EXCEPTION 'previous_id can only be null for first entity event';
    END IF;
    RETURN NEW;
END
$$
LANGUAGE plpgsql;


CREATE TRIGGER append_first_event_for_entity
    BEFORE INSERT
    ON ledger
    FOR EACH ROW
    WHEN (NEW.previous_id IS NULL)
    EXECUTE PROCEDURE check_first_event_for_entity();




-- check previous_id rules
CREATE FUNCTION check_append_with_previous_id() RETURNS trigger AS
$$
BEGIN
    IF (NOT EXISTS (SELECT true
                    FROM ledger
                    WHERE NEW.previous_id = event_id
                      AND NEW.entity_key = entity_key
                      AND NEW.entity = entity))
    THEN
        RAISE EXCEPTION 'previous_id must be in the same entity';
    END IF;

    IF (EXISTS (SELECT true
                FROM ledger l1
                WHERE NEW.previous_id = l1.event_id
                  AND l1.sequence < (SELECT MAX(l2.sequence)
                                     FROM ledger l2
                                     WHERE NEW.entity = l2.entity
                                       AND NEW.entity_key = l2.entity_key)

    ))
    THEN
        RAISE EXCEPTION 'previous_id must reference the newest event in entity';
    END IF;

    RETURN NEW;
END
$$
LANGUAGE plpgsql;


CREATE TRIGGER append_with_previous_id
    BEFORE INSERT
    ON ledger
    FOR EACH ROW
    WHEN (NEW.previous_id IS NOT NULL)
    EXECUTE FUNCTION check_append_with_previous_id();
