-- SQLITE event store

CREATE TABLE events
(
    entity      TEXT NOT NULL,
    entity_key  TEXT NOT NULL,
    event       TEXT NOT NULL,
    data        TEXT NOT NULL,
    -- uuid7, sortable
    event_id    TEXT PRIMARY KEY CHECK (event_id LIKE '________-____-____-____-____________'),
    -- can be anything, like a ULID, nanoid, etc.
    append_key  TEXT NOT NULL UNIQUE,
    -- previous event uuid
    -- null for first event in entity instance; null does not trigger UNIQUE constraint
    previous_id TEXT UNIQUE
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


-- use uuid7 because it's sortable
CREATE VIEW uuid7 AS
WITH unixtime AS (SELECT CAST((UNIXEPOCH('subsec') * 1000) AS INTEGER) AS time),
     current_rowid AS (SELECT IFNULL(MAX(ROWID), 0) AS value FROM events)
SELECT PRINTF('%08x-%04x-%04x-%04x-%012x',
    (SELECT time FROM unixtime) >> 16,
    (SELECT time FROM unixtime) & 0xffff,
    -- Bits 63-52 of ROWID + version
    (((SELECT value FROM current_rowid) >> 52) & 0x0fff) | 0x7000,
    -- Bits 51-38 of ROWID + variant
    (((SELECT value FROM current_rowid) >> 38) & 0x3fff) | 0x8000,
    -- Lower 38 bits of ROWID with last 10 of random
    (((SELECT value FROM current_rowid) & 0x3ffffffffc00) | ABS(RANDOM()) & 0x3ff)
) AS next;



CREATE VIEW append_event AS
    SELECT entity, entity_key, event, data, event_id, append_key, previous_id FROM events;


CREATE TRIGGER generate_event_id_on_append
    INSTEAD OF INSERT
    ON append_event
    FOR EACH ROW
BEGIN
    INSERT INTO events (entity, entity_key, event, data, event_id, append_key, previous_id)
    VALUES (NEW.entity, NEW.entity_key, NEW.event, NEW.data, (SELECT next FROM uuid7), NEW.append_key, NEW.previous_id);
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
