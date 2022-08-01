-- PostgreSQL event store

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS entity_events;

CREATE TABLE entity_events
(
    entity TEXT NOT NULL,
    event  TEXT NOT NULL,
    PRIMARY KEY (entity, event)
);

CREATE OR REPLACE RULE ignore_dup_entity_events AS ON INSERT TO entity_events
    WHERE EXISTS(SELECT 1
                 FROM entity_events
                 WHERE (entity, event) = (NEW.entity, NEW.event))
    DO INSTEAD NOTHING;

-- immutable entity_events
CREATE OR REPLACE RULE ignore_delete_entity_events AS ON DELETE TO entity_events
    DO INSTEAD NOTHING;

CREATE OR REPLACE RULE ignore_update_entity_events AS ON UPDATE TO entity_events
    DO INSTEAD NOTHING;



CREATE TABLE events
(
    entity     TEXT        NOT NULL,
    entitykey  TEXT        NOT NULL,
    event      TEXT        NOT NULL,
    data       JSONB       NOT NULL,
    eventid    UUID        NOT NULL UNIQUE,
    commandid  UUID        NOT NULL UNIQUE,
    -- previous event uuid; null for first event; null does not trigger UNIQUE constraint
    previousid UUID        UNIQUE,
    timestamp  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- ordering sequence
    sequence   BIGSERIAL PRIMARY KEY, -- sequence for all events in all entities
    FOREIGN KEY (entity, event) REFERENCES entity_events (entity, event)
);

CREATE INDEX entity_index ON events (entitykey, entity);

-- immutable events
CREATE OR REPLACE RULE ignore_delete_events AS ON DELETE TO events
    DO INSTEAD NOTHING;

CREATE OR REPLACE RULE ignore_update_events AS ON UPDATE TO events
    DO INSTEAD NOTHING;


-- Can only use null previousId for first event in an entity
CREATE OR REPLACE FUNCTION check_first_event_for_entity() RETURNS trigger AS
$$
BEGIN
    IF (NEW.previousid IS NULL
        AND EXISTS (SELECT 1
                    FROM events
                    WHERE NEW.entitykey = entitykey
                      AND NEW.entity = entity))
    THEN
        RAISE EXCEPTION 'previousid can only be null for first entity event';
END IF;
RETURN NEW;
END;
$$
    LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS first_event_for_entity ON events;
CREATE TRIGGER first_event_for_entity
    BEFORE INSERT
    ON events
    FOR EACH ROW
    EXECUTE PROCEDURE check_first_event_for_entity();



-- previousId must be in the same entity as the event
CREATE OR REPLACE FUNCTION check_previousid_in_same_entity() RETURNS trigger AS
$$
BEGIN
    IF (NEW.previousid IS NOT NULL
        AND NOT EXISTS (SELECT 1
                        FROM events
                        WHERE NEW.previousid = eventid
                          AND NEW.entitykey = entitykey
                          AND NEW.entity = entity))
    THEN
        RAISE EXCEPTION 'previousid must be in the same entity';
END IF;
RETURN NEW;
END;
$$
    LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS previousid_in_same_entity ON events;
CREATE TRIGGER previousid_in_same_entity
    BEFORE INSERT
    ON events
    FOR EACH ROW
    EXECUTE FUNCTION check_previousid_in_same_entity();
