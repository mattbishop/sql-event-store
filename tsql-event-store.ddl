-- T-SQL Event Store (SQL Server 2025)
-- Requires: SQL Server 2025 for native JSON type and JSON INDEX
--
-- Terminology:
--   Entity = The type of aggregate (e.g., "order", "customer")
--   Entity Key = The unique identifier for an instance (e.g., "O001", "C001")
--   Event  = A fact that happened (e.g., "order-placed", "customer-verified")
--
-- Example:
--   entity='order', entity_key='O001', event='order-placed'
--   entity='order', entity_key='O001', event='order-paid'
--   entity='order', entity_key='O001', event='order-shipped'
--   → This is one entity instance with 3 events

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

-- Create the ledger table
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

-- Index for efficient entity queries
CREATE INDEX entity_index ON ledger (entity, entity_key);
GO

-- Unique constraint on previous_id, but only for non-NULL values
-- This allows multiple NULL values (for first events in different entities)
CREATE UNIQUE INDEX idx_previous_id_unique ON ledger (previous_id) WHERE previous_id IS NOT NULL;
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

-- Immutable ledger: enforce validation on all INSERT operations
-- 
-- ARCHITECTURE:
-- This trigger intercepts ALL inserts into the ledger table (both direct and via view).
-- The append_event view trigger forwards inserts to ledger WITHOUT event_id.
-- This trigger then:
--   1. Blocks direct inserts that try to set event_id
--   2. Validates previous_id rules
--   3. Generates event_id automatically
--   4. Performs the actual insert
--
-- This ensures all inserts go through the same validation, regardless of entry point.
CREATE TRIGGER no_direct_insert_ledger ON ledger
INSTEAD OF INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Block direct inserts that try to set event_id
    -- The append_event view inserts WITHOUT event_id, so this distinguishes direct inserts
    IF EXISTS (SELECT 1 FROM inserted WHERE event_id IS NOT NULL)
    BEGIN
        THROW 50003, 'event_id must not be directly set. Use append_event view to insert events.', 1;
    END;
    
    -- ============================================
    -- Validation 1: previous_id IS NULL only for first event
    -- ============================================
    IF EXISTS (
        SELECT 1 
        FROM inserted i
        WHERE i.previous_id IS NULL
          AND EXISTS (
              SELECT 1 
              FROM ledger l
              WHERE l.entity = i.entity
                AND l.entity_key = i.entity_key
          )
    )
    BEGIN
        THROW 50005, 'previous_id can only be null for first entity event', 1;
    END;
    
    -- ============================================
    -- Validation 2 & 3: previous_id rules (only when NOT NULL)
    -- ============================================
    IF EXISTS (
        SELECT 1
        FROM inserted i
        WHERE i.previous_id IS NOT NULL
          AND (
              -- Rule 2: previous_id must be in the same entity
              NOT EXISTS (
                  SELECT 1
                  FROM ledger l
                  WHERE l.event_id = i.previous_id
                    AND l.entity = i.entity
                    AND l.entity_key = i.entity_key
              )
              OR
              -- Rule 3: previous_id must reference the newest event
              EXISTS (
                  SELECT 1
                  FROM ledger l1
                  WHERE l1.event_id = i.previous_id
                    AND l1.sequence < (
                        SELECT MAX(l2.sequence)
                        FROM ledger l2
                        WHERE l2.entity = i.entity
                          AND l2.entity_key = i.entity_key
                    )
              )
          )
    )
    BEGIN
        -- Specific error message for Rule 2
        IF EXISTS (
            SELECT 1
            FROM inserted i
            WHERE i.previous_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM ledger l
                  WHERE l.event_id = i.previous_id
                    AND l.entity = i.entity
                    AND l.entity_key = i.entity_key
              )
        )
        BEGIN
            THROW 50006, 'previous_id must be in the same entity', 1;
        END;
        
        -- Specific error message for Rule 3
        THROW 50007, 'previous_id must reference the newest event in entity', 1;
    END;
    
    -- ============================================
    -- All validations passed - perform insert
    -- ============================================
    INSERT INTO ledger (entity, entity_key, event, data, append_key, previous_id, event_id, timestamp)
    SELECT 
        entity, 
        entity_key, 
        event, 
        data, 
        append_key, 
        previous_id,
        NEWID() AS event_id,  -- Always generate (view trigger doesn't provide it)
        ISNULL(timestamp, SYSDATETIMEOFFSET()) AS timestamp
    FROM inserted;
END;
GO

-- View for appending events (public API)
-- 
-- HOW IT WORKS:
-- T-SQL supports "INSTEAD OF INSERT" triggers on views, creating a "writable view".
-- When applications do: INSERT INTO append_event (...)
--   1. The view trigger (generate_event_id_on_append) intercepts the insert
--   2. It forwards the data to ledger WITHOUT event_id
--   3. The ledger trigger (no_direct_insert_ledger) validates and generates event_id
--
-- This is T-SQL's equivalent to:
--   - PostgreSQL: SELECT append_event(...)  (function-based)
--   - SQLite: INSERT INTO append_event(...) (writable view)
--
-- SECURITY: Direct inserts into ledger are blocked by no_direct_insert_ledger trigger.
-- All inserts MUST go through this view to ensure proper validation.
CREATE VIEW append_event AS
SELECT
    entity,
    entity_key,
    event,
    data,
    append_key,
    previous_id
FROM ledger;
GO

-- Trigger on append_event view: forward inserts to ledger table
-- This trigger simply passes data through to ledger (without event_id).
-- All validation happens in the ledger trigger.
CREATE TRIGGER generate_event_id_on_append ON append_event
INSTEAD OF INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Forward insert to ledger table (without event_id)
    -- The ledger trigger will validate and generate event_id
    INSERT INTO ledger (entity, entity_key, event, data, append_key, previous_id)
    SELECT 
        entity, 
        entity_key, 
        event, 
        data, 
        append_key, 
        previous_id
    FROM inserted;
END;
GO

-- View for replaying events in order
-- Note: T-SQL Views cannot have ORDER BY, so ORDER BY must be in the query
-- 
-- WHY VIEW AND NOT FUNCTION?
-- - Views cannot have parameters (functions can)
-- - Simple replay doesn't need parameters, just WHERE filtering
-- - Standard SQL pattern for simple SELECT queries
-- - Consistent with PostgreSQL/SQLite implementations
--
-- USE CASE: Initial loading of events for an entity
-- Example: SELECT * FROM replay_events WHERE entity = 'order' AND entity_key = 'O001' ORDER BY sequence
CREATE VIEW replay_events AS
SELECT
    entity,
    entity_key,
    event,
    data,
    timestamp,
    event_id
FROM ledger;
GO

-- Table-valued function to replay events after a specific event
-- Note: ORDER BY must be in the query that calls this function, not in the function itself
--
-- WHY FUNCTION AND NOT VIEW?
-- - Functions CAN have parameters (views cannot)
-- - Catch-up needs a parameter: @after_event_id (which event was the last one?)
-- - Complex logic: sequence comparison via subquery
-- - CANNOT be implemented as a view (views don't support parameters)
--
-- USE CASE: "Catch-up" - loading only new events since a known event
-- Example: SELECT * FROM fn_replay_events_after(@last_known_event_id) WHERE entity = 'order' ORDER BY sequence
--
-- RELATIONSHIP TO replay_events VIEW:
-- - Both return the same columns (entity, entity_key, event, data, timestamp, event_id)
-- - Function additionally returns 'sequence' column (needed for ORDER BY)
-- - View: Simple filtering with WHERE
-- - Function: Returns only events after a specific event_id (based on sequence comparison)
-- - In PostgreSQL: Function uses "RETURNS SETOF replay_events" (references the view)
-- - In T-SQL: Function uses "RETURNS TABLE" (inline definition, doesn't reference view)
--
-- TYPICAL WORKFLOW:
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
        timestamp,
        event_id,
        sequence  -- Additional column for ORDER BY
    FROM ledger
    WHERE sequence > (
        SELECT sequence 
        FROM ledger 
        WHERE event_id = @after_event_id
    )
);
GO
