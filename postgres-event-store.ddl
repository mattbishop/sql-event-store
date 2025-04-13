-- Postgres event store

CREATE TABLE ledger
(
    entity      TEXT        NOT NULL,
    entity_key  TEXT        NOT NULL,
    event       TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    event_id    UUID        NOT NULL UNIQUE,
    append_key  TEXT        NOT NULL UNIQUE,
    -- previous event uuid; null for first event; null does not trigger UNIQUE constraint
    previous_id UUID        UNIQUE,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- ordering sequence
    sequence   BIGSERIAL    PRIMARY KEY -- sequence for all events in all entities
);

CREATE INDEX entity_index ON ledger (entity_key, entity);

-- immutable events
CREATE RULE ignore_delete_events AS ON DELETE TO ledger
    DO INSTEAD NOTHING;

CREATE RULE ignore_update_events AS ON UPDATE TO ledger
    DO INSTEAD NOTHING;


CREATE VIEW append_event AS
SELECT entity, entity_key, event, data, event_id, append_key, previous_id FROM ledger;


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
END;
$$
    LANGUAGE plpgsql;


CREATE TRIGGER generate_event_id_on_append
    BEFORE INSERT
    ON ledger
    FOR EACH ROW
    EXECUTE PROCEDURE generate_event_id();


-- Can only use null previousId for first event in an entity
CREATE FUNCTION check_first_event_for_entity() RETURNS trigger AS
$$
BEGIN
    IF (NEW.previous_id IS NULL
        AND EXISTS (SELECT 1
                    FROM ledger
                    WHERE NEW.entity_key = entity_key
                      AND NEW.entity = entity))
    THEN
        RAISE EXCEPTION 'previous_id can only be null for first entity event';
    END IF;
    RETURN NEW;
END;
$$
    LANGUAGE plpgsql;


CREATE TRIGGER first_event_for_entity
    BEFORE INSERT
    ON ledger
    FOR EACH ROW
    EXECUTE PROCEDURE check_first_event_for_entity();



-- previous_id must be in the same entity as the event
CREATE FUNCTION check_previous_id_in_same_entity() RETURNS trigger AS
$$
BEGIN
    IF (NEW.previous_id IS NOT NULL
        AND NOT EXISTS (SELECT 1
                        FROM ledger
                        WHERE NEW.previous_id = event_id
                          AND NEW.entity_key = entity_key
                          AND NEW.entity = entity))
    THEN
        RAISE EXCEPTION 'previous_id must be in the same entity';
    END IF;
    RETURN NEW;
END;
$$
    LANGUAGE plpgsql;


CREATE TRIGGER previous_id_in_same_entity
    BEFORE INSERT
    ON ledger
    FOR EACH ROW
    EXECUTE FUNCTION check_previous_id_in_same_entity();



CREATE VIEW replay_events AS
SELECT
    entity,
    entity_key,
    event,
    data,
    timestamp,
    event_id
FROM ledger ORDER BY sequence;
