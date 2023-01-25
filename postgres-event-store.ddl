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
    commandid  UUID        NOT NULL UNIQUE,
    -- previous event uuid; null for first event; null does not trigger UNIQUE constraint
    previousSequence BIGINT UNIQUE,
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


-- Can only use null previousSequence for first event in an entity
CREATE OR REPLACE FUNCTION check_first_event_for_entity() RETURNS trigger AS
$$
BEGIN
    IF (NEW.previousSequence IS NULL
        AND EXISTS (SELECT 1
                    FROM events
                    WHERE NEW.entitykey = entitykey
                      AND NEW.entity = entity))
    THEN
        RAISE EXCEPTION 'previousSequence can only be null for first entity event';
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



-- previousSequence must be in the same entity as the event
CREATE OR REPLACE FUNCTION check_previousSequence_in_same_entity()
RETURNS trigger AS
$$
BEGIN
    IF (NEW.previousSequence IS NOT NULL
        AND NEW.previousSequence != (
                SELECT sequence FROM events
                    WHERE NEW.entitykey = entitykey
                        AND NEW.entity = entity
                    ORDER BY sequence DESC
                    LIMIT 1
             )
    )
    THEN
        RAISE EXCEPTION 'previousSequence must be the last entry of the event stream for the same entity';
    END IF;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS previousSequence_in_same_entity ON events;
CREATE TRIGGER previousSequence_in_same_entity
    BEFORE INSERT
    ON events
    FOR EACH ROW
    EXECUTE FUNCTION check_previousSequence_in_same_entity();




truncate events;
truncate entity_events cascade;
ALTER SEQUENCE events_sequence_seq RESTART WITH 1;

-- INSERT INTO entity_events (entity, event) VALUES ('thing', 'thing-created');
-- INSERT INTO entity_events (entity, event) VALUES ('thing', 'thing-deleted');

-- INSERT INTO entity_events (entity, event) VALUES ('table-tennis', 'ball-pinged');
-- INSERT INTO entity_events (entity, event) VALUES ('table-tennis', 'ball-ponged');

-- INSERT INTO events (entity, entityKey, event, data, commandId, previousSequence) VALUES ('table-tennis', 'home', 'ball-pinged', '{}', '00000000-0000-0000-0000-000000000000', null);
-- INSERT INTO events (entity, entityKey, event, data, commandId, previousSequence) VALUES ('table-tennis', 'home', 'ball-ponged', '{}', '00000000-0000-0000-0000-000000000001', 1);
-- INSERT INTO events (entity, entityKey, event, data, commandId, previousSequence) VALUES ('table-tennis', 'home', 'ball-ponged', '{}', '00000000-0000-0000-0000-000000000002', 2);

-- INSERT INTO events (entity, entityKey, event, data, commandId, previousSequence) VALUES ('table-tennis', 'work', 'ball-pinged', '{}', '00000000-0000-0000-0000-000000000003', null);
-- INSERT INTO events (entity, entityKey, event, data, commandId, previousSequence) VALUES ('table-tennis', 'work', 'ball-ponged', '{}', '00000000-0000-0000-0000-000000000004', 4);
-- INSERT INTO events (entity, entityKey, event, data, commandId, previousSequence) VALUES ('table-tennis', 'work', 'ball-ponged', '{}', '00000000-0000-0000-0000-000000000005', 5);

