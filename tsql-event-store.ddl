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

-- Unique constraint to prevent race conditions (first event per stream)
-- Ensures only one row with previous_id IS NULL per (entity, entity_key).
CREATE UNIQUE INDEX uq_first_event_per_stream
    ON ledger(entity, entity_key)
    WHERE previous_id IS NULL;
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
    SET XACT_ABORT ON;

    BEGIN TRY
        -- Flag setzen
        EXEC sys.sp_set_session_context 
            @key = N'allow_direct_ledger_insert', 
            @value = 1;

        BEGIN TRAN;

        -- Validation 1
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
            -- Validation 2
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

            -- Validation 3
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

        -- Insert
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

        COMMIT TRAN;

        -- Flag wieder löschen
        EXEC sys.sp_set_session_context 
            @key = N'allow_direct_ledger_insert', 
            @value = NULL;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRAN;

        -- Flag IMMER löschen, auch im Fehlerfall
        EXEC sys.sp_set_session_context 
            @key = N'allow_direct_ledger_insert', 
            @value = NULL;

        THROW;
    END CATCH;
END;
GO


CREATE OR ALTER TRIGGER no_direct_insert_ledger
ON ledger
INSTEAD OF INSERT
AS
BEGIN
    SET NOCOUNT ON;

    IF SESSION_CONTEXT(N'allow_direct_ledger_insert') = 1
    BEGIN
        -- Allow only via procedure; let IDENTITY generate sequence
        INSERT INTO ledger (entity, entity_key, event, data, append_key, previous_id, event_id, timestamp)
        SELECT entity, entity_key, event, data, append_key, previous_id, event_id, timestamp
        FROM inserted;
        RETURN;
    END;

    THROW 50003, 'Use append_event procedure to insert events into the ledger', 1;
END;
GO

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
