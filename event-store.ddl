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
    FOREIGN KEY(entity, event) REFERENCES entity_events(entity, event)
);

CREATE INDEX entity_index ON events(entity, entityKey, event);
