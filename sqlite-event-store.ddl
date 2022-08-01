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
    entity     TEXT NOT NULL,
    entityKey  TEXT NOT NULL,
    event      TEXT NOT NULL,
    data       TEXT NOT NULL,
    eventId    TEXT NOT NULL UNIQUE CHECK (eventId LIKE '________-____-____-____-____________'),
    commandId  TEXT NOT NULL UNIQUE CHECK (commandId LIKE '________-____-____-____-____________'),
    -- previous event uuid; null for first event; null does not trigger UNIQUE constraint
    previousId TEXT UNIQUE,
    timestamp  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- ordering sequence
    sequence   INTEGER PRIMARY KEY, -- sequence for all events in all entities
    FOREIGN KEY (entity, event) REFERENCES entity_events (entity, event)
);

CREATE INDEX entity_index ON events (entityKey, entity);

-- immutable entity_events
CREATE TRIGGER no_delete_entity_events
    BEFORE DELETE
    ON entity_events
BEGIN
    SELECT RAISE(FAIL, 'Cannot delete entity_events');
END;

CREATE TRIGGER no_update_entity_events
    BEFORE UPDATE
    ON entity_events
BEGIN
    SELECT RAISE(FAIL, 'Cannot update entity_events');
END;

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

-- Can only use null previousId for first event in an entity
CREATE TRIGGER first_event_for_entity
    BEFORE INSERT
    ON events
    FOR EACH ROW
    WHEN NEW.previousId IS NULL
        AND EXISTS (SELECT 1
                    FROM events
                    WHERE NEW.entityKey = entityKey
                      AND NEW.entity = entity)
BEGIN
    SELECT RAISE(FAIL, 'previousId can only be null for first entity event');
END;

-- previousId must be in the same entity as the event
CREATE TRIGGER previousId_in_same_entity
    BEFORE INSERT
    ON events
    FOR EACH ROW
    WHEN NEW.previousId IS NOT NULL
        AND NOT EXISTS (SELECT 1
                        FROM events
                        WHERE NEW.previousId = eventId
                          AND NEW.entityKey = entityKey
                          AND NEW.entity = entity)
BEGIN
    SELECT RAISE(FAIL, 'previousId must be in same entity');
END;
