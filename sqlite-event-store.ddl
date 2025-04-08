-- SQLITE event store
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS entity_events;

CREATE TABLE entity_events
(
    entity TEXT NOT NULL,
    event  TEXT NOT NULL,
    PRIMARY KEY (entity, event) ON CONFLICT IGNORE
);


CREATE TABLE events
(
    entity      TEXT NOT NULL,
    entity_key  TEXT NOT NULL,
    event       TEXT NOT NULL,
    data        TEXT NOT NULL,
    event_id    TEXT NOT NULL UNIQUE CHECK (event_id LIKE '________-____-____-____-____________'),
    append_key  TEXT NOT NULL UNIQUE,
    -- previous event uuid; null for first event; null does not trigger UNIQUE constraint
    previous_id TEXT UNIQUE,
    timestamp   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- ordering sequence
    sequence    INTEGER PRIMARY KEY, -- sequence for all events in all entities
    FOREIGN KEY (entity, event) REFERENCES entity_events (entity, event)
);

CREATE INDEX entity_index ON events (entity_key, entity);

-- immutable events
CREATE TRIGGER no_delete_events
    BEFORE DELETE
    ON events
BEGIN
    SELECT RAISE(FAIL, 'Cannot delete events');
END;

CREATE TRIGGER no_update_events
    BEFORE UPDATE
    ON events
BEGIN
    SELECT RAISE(FAIL, 'Cannot update events');
END;


DROP VIEW IF EXISTS uuid7;
CREATE VIEW uuid7 AS
WITH unixtime AS (SELECT CAST((UNIXEPOCH('subsec') * 1000) AS INTEGER) AS time)
SELECT PRINTF('%08x-%04x-%04x-%04x-%012x',
    (SELECT time FROM unixtime) >> 16,
    (SELECT time FROM unixtime) & 0xffff,
    ABS(RANDOM()) % 0x0fff + 0x7000,
    ABS(RANDOM()) % 0x3fff + 0x8000,
    ABS(RANDOM()) >> 16) AS next;


DROP VIEW IF EXISTS append_event;
CREATE VIEW append_event AS
    SELECT entity, entity_key, event, data, append_key, previous_id, event_id FROM events;


CREATE TRIGGER generate_event_id_on_append
    INSTEAD OF INSERT
    ON append_event
    FOR EACH ROW
BEGIN
    INSERT INTO events (entity, entity_key, event, data, append_key, event_id, previous_id)
    VALUES (NEW.entity, NEW.entity_key, NEW.event, NEW.data, NEW.append_key, (SELECT next FROM uuid7), NEW.previous_id);
END;


-- Can only use null previous_id for first event in an entity
CREATE TRIGGER first_event_for_entity
    BEFORE INSERT
    ON events
    FOR EACH ROW
    WHEN NEW.previous_id IS NULL
        AND EXISTS (SELECT 1
                    FROM events
                    WHERE NEW.entity_key = entity_key
                      AND NEW.entity = entity)
BEGIN
    SELECT RAISE(FAIL, 'previous_id can only be null for first entity event');
END;

-- previous_id must be in the same entity as the event
CREATE TRIGGER previous_id_in_same_entity
    BEFORE INSERT
    ON events
    FOR EACH ROW
    WHEN NEW.previous_id IS NOT NULL
        AND NOT EXISTS (SELECT 1
                        FROM events
                        WHERE NEW.previous_id = event_id
                          AND NEW.entity_key = entity_key
                          AND NEW.entity = entity)
BEGIN
    SELECT RAISE(FAIL, 'previous_id must be in same entity');
END;
