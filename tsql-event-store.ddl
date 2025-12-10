-- T-SQL Event Store (SQL Server 2025)
-- Requires: SQL Server 2025 for native JSON type and JSON INDEX

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

CREATE TABLE ledger
(
    entity          NVARCHAR(255)        NOT NULL,
    entity_key      NVARCHAR(255)        NOT NULL,
    event           NVARCHAR(255)        NOT NULL,
    data            JSON                 NOT NULL,
    -- can be anything, like a ULID, nanoid, etc.
    append_key      NVARCHAR(255)        NOT NULL UNIQUE,
    -- previous event id
    -- null for first event in entity instance; null does not trigger UNIQUE constraint
    previous_id     UNIQUEIDENTIFIER     NULL,
    event_id        UNIQUEIDENTIFIER     NOT NULL UNIQUE,
    timestamp       DATETIMEOFFSET       NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    -- sequence for all events in all entities
    sequence        BIGINT IDENTITY(1,1) PRIMARY KEY
);
GO

CREATE INDEX entity_index ON ledger (entity, entity_key);
GO

-- Unique constraint on previous_id, but only for non-NULL values
-- This allows multiple NULL values (for first events in different entities)
CREATE UNIQUE INDEX idx_previous_id_unique ON ledger (previous_id) WHERE previous_id IS NOT NULL;
GO

-- OPTIONAL: Unique constraint to prevent race conditions
-- Trigger validation has a race condition (only sees existing state, not same-INSERT rows).
-- PostgreSQL solves this with a partial unique index: CREATE UNIQUE INDEX ... ON ledger
--   (entity, entity_key) WHERE previous_id IS NULL - this works cleanly in PostgreSQL.
-- T-SQL limitation: Filtered indexes on multiple columns are a mess, needs computed columns/workarounds.
-- Solution here: For event streams, use application-level idempotency (append_key). 
GO

-- JSON index on all paths for efficient querying (SQL Server 2025)
CREATE JSON INDEX idx_data ON ledger (data);
GO

-- Immutable ledger: prevent DELETE
CREATE TRIGGER no_delete_ledger ON ledger
INSTEAD OF DELETE
AS
BEGIN
    THROW 50001, 'Cannot delete events from the ledger', 1;
END;
GO

-- Immutable ledger: prevent UPDATE
CREATE TRIGGER no_update_ledger ON ledger
INSTEAD OF UPDATE
AS
BEGIN
    THROW 50002, 'Cannot update events in the ledger', 1;
END;
GO

-- Append events via stored procedure (preferred API)
-- Provides a single entry point that validates previous_id rules and returns event_id.
CREATE OR ALTER PROCEDURE append_event
    @entity       NVARCHAR(255),
    @entity_key   NVARCHAR(255),
    @event        NVARCHAR(255),
    @data         JSON,
    @append_key   NVARCHAR(255),
    @previous_id  UNIQUEIDENTIFIER = NULL,
    @timestamp    DATETIMEOFFSET   = NULL,
    @event_id     UNIQUEIDENTIFIER OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    -- Validation 1: previous_id IS NULL only for first event of an entity instance
    IF (@previous_id IS NULL)
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM ledger l
            WHERE l.entity = @entity
              AND l.entity_key = @entity_key
        )
        BEGIN
            THROW 50005, 'previous_id can only be null for first entity event', 1;
        END;
    END
    ELSE
    BEGIN
        -- Validation 2: previous_id must be in same entity
        IF NOT EXISTS (
            SELECT 1
            FROM ledger l
            WHERE l.event_id   = @previous_id
              AND l.entity     = @entity
              AND l.entity_key = @entity_key
        )
        BEGIN
            THROW 50006, 'previous_id must be in the same entity', 1;
        END;

        -- Validation 3: previous_id must reference newest event
        IF EXISTS (
            SELECT 1
            FROM ledger l1
            WHERE l1.event_id = @previous_id
              AND l1.sequence < (
                  SELECT MAX(l2.sequence)
                  FROM ledger l2
                  WHERE l2.entity     = @entity
                    AND l2.entity_key = @entity_key
              )
        )
        BEGIN
            THROW 50007, 'previous_id must reference the newest event in entity', 1;
        END;
    END;

    -- Generate event_id and insert
    SET @event_id = NEWID();

    INSERT INTO ledger (entity, entity_key, event, data, append_key, previous_id, event_id, timestamp)
    VALUES (
        @entity,
        @entity_key,
        @event,
        @data,
        @append_key,
        @previous_id,
        @event_id,
        ISNULL(@timestamp, SYSDATETIMEOFFSET())
    );
END;
GO

-- View for replaying events in order
-- Note: T-SQL views cannot have ORDER BY (it's a mess), so ORDER BY must be in the query.
-- 
-- Why view not function: Views can't have parameters, simple replay just needs WHERE filtering.
-- Use case: Initial loading. Example: SELECT * FROM replay_events WHERE entity = 'order' 
-- AND entity_key = 'O001' ORDER BY sequence
CREATE VIEW replay_events AS
SELECT
    entity,
    entity_key,
    event,
    data,
    append_key,
    previous_id,
    event_id,
    timestamp,
    sequence
FROM ledger;
GO

-- Table-valued function to replay events after a specific event
-- Note: ORDER BY must be in the calling query, not in the function (T-SQL limitation).
--
-- Why function not view: Functions can have parameters, catch-up needs @after_event_id.
-- Cannot be a view (views don't support parameters - it's a mess).
--
-- Use case: Catch-up - loading only new events since a known event.
-- Example: SELECT * FROM fn_replay_events_after(@last_known_event_id) WHERE entity = 'order' ORDER BY sequence
--
-- Relationship to replay_events view: Both return same columns. View = simple WHERE filtering.
-- Function = events after specific event_id (sequence comparison). In PostgreSQL, function references
-- the view via "RETURNS SETOF replay_events". In T-SQL, it's "RETURNS TABLE" (inline, can't reference view).
--
-- Typical workflow:
-- 1. Initial load: SELECT * FROM replay_events WHERE ... ORDER BY sequence
-- 2. Catch-up: SELECT * FROM fn_replay_events_after(@last_event_id) WHERE ... ORDER BY sequence
CREATE FUNCTION fn_replay_events_after(@after_event_id UNIQUEIDENTIFIER)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        entity,
        entity_key,
        event,
        data,
        append_key,
        previous_id,
        event_id,
        timestamp,
        sequence
    FROM ledger
    WHERE sequence > (
        SELECT sequence 
        FROM ledger 
        WHERE event_id = @after_event_id
    )
);
GO
