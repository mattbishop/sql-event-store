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
    entity    TEXT NOT NULL,
    event     TEXT NOT NULL,
    key       TEXT NOT NULL,
    id        TEXT NOT NULL UNIQUE,  -- event uuid
    ts        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data      TEXT NOT NULL,
    command   TEXT NOT NULL UNIQUE,  -- Ensures commands only create one event
    previous  TEXT NOT NULL UNIQUE,  -- previous event uuid; 00000000-0000-0000-0000-000000000000 for first event
    version   TEXT NOT NULL DEFAULT '0.0.0',
    -- ordering sequence
    sequence  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,  -- sequence for all events in all domains
    FOREIGN KEY(entity, event) REFERENCES entity_events(entity, event)
);

CREATE INDEX entity_index ON events(entity, event, key);
