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
-- Approach similar to https://github.com/LiosK/uuidv7/
CREATE VIEW uuid7 AS
WITH unixtime AS (
    SELECT CAST((UNIXEPOCH('subsec') * 1000) AS INTEGER) AS current_time_ms -- Get current Unix timestamp in milliseconds
),
     latest_uuid AS (
         SELECT
             SUBSTR(event_id, 1, 8) AS time_high_hex,        -- Extract the high 32 bits of the timestamp
             SUBSTR(event_id, 10, 4) AS time_mid_hex,         -- Extract the middle 16 bits of the timestamp
             SUBSTR(event_id, 15, 4) AS time_low_version_hex, -- Extract the lower 12 bits of the timestamp and the version
             SUBSTR(event_id, 20, 4) AS clock_seq_variant_hex, -- Extract the clock sequence and variant bits
             SUBSTR(event_id, 25, 12) AS node_hex              -- Extract the node identifier or randomness
         FROM events
         ORDER BY ROWID DESC                           -- Get the most recently inserted event (assuming sequential ROWIDs)
         LIMIT 1                                     -- Limit to the single latest event
     ),
     last_rowid AS (
         SELECT MAX(ROWID) AS max_rowid FROM events      -- Get the maximum ROWID to check if the table is empty
     ),
     last_timestamp AS (
         SELECT
             CASE
                 WHEN (SELECT max_rowid FROM last_rowid) IS NOT NULL
                     THEN (CAST('0x' || (SELECT time_high_hex FROM latest_uuid) AS INTEGER) << 16)
                          + CAST('0x' || (SELECT time_mid_hex FROM latest_uuid) AS INTEGER)
                          + (CAST('0x' || SUBSTR((SELECT time_low_version_hex FROM latest_uuid), 2, 3) AS INTEGER) << 0)
                 ELSE 0                                   -- Default timestamp if no events exist
                 END AS ms
     ),
     last_counter AS (
         SELECT
             CASE
                 WHEN (SELECT max_rowid FROM last_rowid) IS NOT NULL
                     THEN CAST('0x' || (SELECT clock_seq_variant_hex FROM latest_uuid) || SUBSTR((SELECT node_hex FROM latest_uuid), 1, 8) AS INTEGER)
                 ELSE ABS(RANDOM()) & 0x3fffffffffff       -- Initial random counter if no events exist
                 END AS value
     ),
     calculated_counter AS (
         SELECT
             CASE
                 WHEN (SELECT current_time_ms FROM unixtime) = (SELECT ms FROM last_timestamp)
                     THEN ((SELECT value FROM last_counter) + 1) & 0x3fffffffffff -- Increment counter if timestamp is the same
                 ELSE ABS(RANDOM()) & 0x3fffffffffff       -- Reset counter with random if timestamp differs
                 END AS next_value
     )
SELECT
    PRINTF('%08x-%04x-%04x-%04x-%012x',
           ((SELECT current_time_ms FROM unixtime) >> 16) & 0xffffffff, -- Higher 32 bits of timestamp
           ((SELECT current_time_ms FROM unixtime) & 0xffff),           -- Lower 16 bits of timestamp
           (((SELECT next_value FROM calculated_counter) >> 28) & 0x0fff) | 0x7000, -- Higher 12 bits of counter + UUIDv7 version
           (((SELECT next_value FROM calculated_counter) >> 12) & 0x3fff) | 0x8000, -- Middle 16 bits of counter + RFC 4122 variant
           ((SELECT next_value FROM calculated_counter) & 0xfffffffffffff)    -- Lower 14 bits of counter (42 total - 12 - 16 = 14)
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
