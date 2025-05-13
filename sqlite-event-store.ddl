-- SQLITE event store

CREATE TABLE ledger
(
    entity      TEXT NOT NULL,
    entity_key  TEXT NOT NULL,
    event       TEXT NOT NULL,
    data        TEXT NOT NULL,
    -- uuid
    event_id    TEXT  NOT NULL UNIQUE CHECK (event_id LIKE '________-____-____-____-____________'),
    ts_ms       INTEGER NOT NULL,
    -- can be anything, like a ULID, nanoid, etc.
    append_key  TEXT NOT NULL UNIQUE,
    -- previous event uuid
    -- null for first event in entity instance; null does not trigger UNIQUE constraint
    previous_id TEXT UNIQUE,
    sequence    INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE INDEX entity_index ON ledger (entity, entity_key);


-- immutable ledger
CREATE TRIGGER no_delete_ledger
    BEFORE DELETE
    ON ledger
BEGIN
    SELECT RAISE(FAIL, 'Cannot delete events from the ledger');
END;

CREATE TRIGGER no_update_ledger
    BEFORE UPDATE
    ON ledger
BEGIN
    SELECT RAISE(FAIL, 'Cannot update events in the ledger');
END;


CREATE VIEW append_event AS
SELECT
    entity,
    entity_key,
    event,
    data,
    event_id,
    append_key,
    previous_id
FROM ledger;


CREATE VIEW replay_events AS
SELECT
    entity,
    entity_key,
    event,
    data,
    strftime('%Y-%m-%dT%H:%M:%fZ', ts_ms / 1000.0, 'unixepoch') AS timestamp,
    event_id
FROM ledger ORDER BY sequence;


-- From Claude 3.7, thanks!
DROP VIEW IF EXISTS uuid4;
CREATE VIEW uuid4 AS
WITH random_128 AS (SELECT randomblob(16) AS bytes)
SELECT lower(printf('%s-%s-4%s-%s%s-%s',
           hex(substr(bytes, 1, 4)),
           hex(substr(bytes, 5, 2)),
           substr(hex(substr(bytes, 7, 2)), 2, 3),
           substr('89ab', 1 + (abs(random()) % 4), 1),
           substr(hex(substr(bytes, 10, 2)), 2, 3),
           hex(substr(bytes, 11, 6))
   ))
AS next
FROM random_128;


CREATE TRIGGER generate_event_id_on_append
    INSTEAD OF INSERT
    ON append_event
    FOR EACH ROW
BEGIN
    INSERT INTO ledger (entity, entity_key, event, data, event_id, ts_ms, append_key, previous_id)
    VALUES (NEW.entity,
            NEW.entity_key,
            NEW.event,
            NEW.data,
            (SELECT next FROM uuid4),
            CAST((UNIXEPOCH('subsec') * 1000) AS INTEGER),
            NEW.append_key,
            NEW.previous_id);
END;


-- Can only use null previous_id for first event in an entity
CREATE TRIGGER first_event_for_entity
    BEFORE INSERT
    ON ledger
    FOR EACH ROW
    WHEN NEW.previous_id IS NULL
        AND EXISTS (SELECT true
                    FROM ledger
                    WHERE NEW.entity_key = entity_key
                      AND NEW.entity = entity)
BEGIN
    SELECT RAISE(FAIL, 'previous_id can only be null for first entity event');
END;


-- previous_id must be in the same entity as the event
CREATE TRIGGER previous_id_in_same_entity
    BEFORE INSERT
    ON ledger
    FOR EACH ROW
    WHEN NEW.previous_id IS NOT NULL
        AND NOT EXISTS (SELECT true
                        FROM ledger
                        WHERE NEW.previous_id = event_id
                          AND NEW.entity_key = entity_key
                          AND NEW.entity = entity)
BEGIN
    SELECT RAISE(FAIL, 'previous_id must be in same entity');
END;
