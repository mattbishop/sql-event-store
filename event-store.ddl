-- SQLITE event store
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS entity_events;
DROP TABLE IF EXISTS events;

CREATE TABLE entity_events(
    entity  TEXT NOT NULL,
    event   TEXT NOT NULL,
    PRIMARY KEY (entity, event)
);

CREATE TABLE events (
    entity      TEXT NOT NULL,
    entityKey   TEXT NOT NULL,
    event       TEXT NOT NULL,
    data        TEXT NOT NULL,
    eventId     TEXT NOT NULL UNIQUE,  -- event uuid
    commandId   TEXT NOT NULL UNIQUE,  -- Ensures commands only create one event
    previousId  TEXT UNIQUE,  -- previous event uuid; null for first event; null does not trigger UNIQUE constraint
    ts          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- ordering sequence
    sequence    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,  -- sequence for all events in all entities
    FOREIGN KEY(entity, event) REFERENCES entity_events(entity, event),
    FOREIGN KEY(previousId) REFERENCES events(eventId)
);

CREATE INDEX entity_index ON events(entity, entityKey, event);

-- immutable entity_events
CREATE TRIGGER no_delete_entity_events BEFORE DELETE ON entity_events
    BEGIN
        SELECT RAISE (FAIL, 'Cannot delete entity_events');
    END;

CREATE TRIGGER no_update_entity_events BEFORE UPDATE ON entity_events
    BEGIN
        SELECT RAISE (FAIL, 'Cannot update entity_events');
    END;

-- immutable events
CREATE TRIGGER no_delete_events BEFORE DELETE ON events
    BEGIN
        SELECT RAISE (FAIL, 'Cannot delete events');
    END;

CREATE TRIGGER no_update_events BEFORE UPDATE ON events
    BEGIN
        SELECT RAISE (FAIL, 'Cannot update events');
    END;
