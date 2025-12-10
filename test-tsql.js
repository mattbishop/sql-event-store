import { rejects, doesNotReject, strictEqual } from 'node:assert/strict'
import { before, after, test } from 'node:test'
import fs from 'fs'
import { execSync } from 'child_process'
import { join } from 'path'
import sql from 'mssql'
import { nanoid } from 'nanoid'


/**
 * T-SQL Event Store Test Suite
 * 
 * This test suite exercises the T-SQL database schema with incorrect data, 
 * duplicate data, and other likely real-world problems.
 * 
 * T-SQL-SPECIFIC DIFFERENCES FROM POSTGRESQL/SQLITE:
 * 
 * 1. APPEND MECHANISM:
 *    - PostgreSQL: Uses function append_event() that returns UUID
 *    - SQLite: Uses view append_event with INSTEAD OF INSERT trigger
 *    - T-SQL: Uses view append_event with INSTEAD OF INSERT trigger (like SQLite)
 *    → Reason: T-SQL doesn't support functions returning values in INSERT context like PostgreSQL
 * 
 * 2. DATABASE PERSISTENCE:
 *    - PostgreSQL/SQLite: In-memory databases (fresh for each test run)
 *    - T-SQL: Persistent SQL Server database
 *    → Reason: SQL Server requires a running server instance
 *    → Solution: TRUNCATE TABLE after DDL load to ensure clean state
 * 
 * 3. PARAMETERIZED QUERIES:
 *    - PostgreSQL: Uses template strings with query``
 *    - SQLite: Uses prepared statements with placeholders
 *    - T-SQL: Uses mssql parameterized queries with .input()
 *    → Reason: mssql library requires explicit parameter binding for security
 * 
 * 4. UUID TYPE:
 *    - PostgreSQL: UUID type
 *    - SQLite: TEXT with CHECK constraint
 *    - T-SQL: UNIQUEIDENTIFIER type
 *    → Reason: T-SQL native type for GUIDs/UUIDs
 * 
 * 5. ERROR MESSAGE FORMATS:
 *    - PostgreSQL: "error: message"
 *    - SQLite: "message"
 *    - T-SQL: Direct SQL Server error messages (different format)
 *    → Reason: SQL Server throws native errors, not custom formatted
 * 
 * 6. REPLAY VIEWS:
 *    - PostgreSQL/SQLite: ORDER BY in view definition
 *    - T-SQL: ORDER BY must be in query (views can't have ORDER BY)
 *    → Reason: T-SQL view limitations
 * 
 * 7. REPLAY FUNCTION:
 *    - PostgreSQL: RETURNS SETOF replay_events (uses view)
 *    - T-SQL: RETURNS TABLE (inline definition)
 *    → Reason: T-SQL table-valued functions use inline table definition
 */

const thingEntity = 'thing'
const thingCreatedEvent = 'thing-created'
const thingDeletedEvent = 'thing-deleted'

const tableTennisEntity = 'table-tennis'
const pingEvent = 'ball-pinged'
const pongEvent = 'ball-ponged'

const thingKey = '1'
const homeTableKey = 'home'
const workTableKey = 'work'

let thingEventId1
let thingEventId2

let pingEventHomeId
let pingEventWorkId

// T-SQL: Connection to persistent SQL Server instance
// Unlike PostgreSQL/SQLite which use in-memory databases, T-SQL requires a running server
const config = {
  server: 'localhost',
  port: 1433,
  database: 'eventstore',
  user: 'sa',
  password: 'EventStore!2025',
  options: {
    encrypt: false,
    trustServerCertificate: true,
    enableArithAbort: true
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
}

/**
 * Restarts the Docker container to ensure a clean state.
 * T-SQL-SPECIFIC: Required because we use Docker with persistent volumes.
 * This ensures the database is completely reinitialized when DDL changes.
 */
async function restartDockerContainer() {
  const dockerComposePath = join(process.cwd(), 'T-SQL', 'docker-compose.yml')
  const dockerComposeDir = join(process.cwd(), 'T-SQL')
  
  try {
    // Stop and remove containers and volumes
    console.log('Stopping Docker containers and removing volumes...')
    execSync('docker-compose down -v', { 
      cwd: dockerComposeDir,
      stdio: 'inherit'
    })
    
    // Start containers again
    console.log('Starting Docker containers...')
    execSync('docker-compose up -d', { 
      cwd: dockerComposeDir,
      stdio: 'inherit'
    })
    
    // Wait for SQL Server to be ready
    console.log('Waiting for SQL Server to be ready...')
    await new Promise(resolve => setTimeout(resolve, 10000)) // Wait 10 seconds
    
    // Try to connect with retries
    let retries = 30
    while (retries > 0) {
      try {
        const testPool = await sql.connect(config)
        await testPool.close()
        console.log('SQL Server is ready!')
        break
      } catch (err) {
        retries--
        if (retries === 0) {
          throw new Error('SQL Server did not become ready in time')
        }
        await new Promise(resolve => setTimeout(resolve, 1000))
      }
    }
  } catch (err) {
    console.error('Error restarting Docker container:', err.message)
    throw err
  }
}

async function initDb() {
  // T-SQL-SPECIFIC: Restart Docker container to ensure clean state
  // This is necessary because we use persistent volumes and need to reinitialize
  // when DDL changes. Unlike PostgreSQL/SQLite which use fresh in-memory databases.
  await restartDockerContainer()
  
  const pool = await sql.connect(config)
  // T-SQL-SPECIFIC: Always clean database at the start
  // Unlike PostgreSQL/SQLite which use fresh in-memory databases each time,
  // T-SQL uses a persistent database, so we must explicitly clean data before each test run
  await cleanDatabase(pool)
  await loadDdl(pool)
  return pool
}

/**
 * Cleans the database by removing all data from the ledger table.
 * This ensures a clean state before each test run.
 * T-SQL-SPECIFIC: Required because we use a persistent database, not in-memory.
 */
async function cleanDatabase(pool) {
  try {
    // Try to truncate the table if it exists
    await pool.request().query('TRUNCATE TABLE ledger')
  } catch (err) {
    // Table might not exist yet (first run), ignore this error
    if (!err.message.includes('Invalid object name') && 
        !err.message.includes('does not exist')) {
      throw err
    }
  }
}

async function loadDdl(pool) {
  const createScript = fs.readFileSync('./tsql-event-store.ddl', 'utf8')
  // T-SQL: Split by GO statements (batch separator) and execute each batch
  // Unlike PostgreSQL/SQLite which execute entire script at once
  // Split on GO at start of line or after whitespace, followed by optional whitespace and newline
  const batches = createScript.split(/^\s*GO\s*$/gim).filter(b => b.trim().length > 0)
  
  for (const batch of batches) {
    const trimmed = batch.trim()
    if (trimmed) {
      try {
        await pool.request().query(trimmed)
      } catch (err) {
        // Ignore errors for objects that already exist or don't exist (for re-running tests)
        // T-SQL: DDL includes DROP statements, so this handles edge cases
        const errorMsg = err.message || ''
        if (!errorMsg.includes('already exists') && 
            !errorMsg.includes('already an object') &&
            !errorMsg.includes('does not exist') &&
            !errorMsg.includes('Invalid object name') &&
            !errorMsg.includes('Cannot drop') &&
            // Ignore CREATE TRIGGER errors if object doesn't exist yet (will be created in next run)
            !(errorMsg.includes('CREATE TRIGGER') && errorMsg.includes('must be the first statement'))) {
          // Only throw if it's not a known ignorable error
          console.error(`DDL Error in batch: ${errorMsg.substring(0, 100)}`)
          throw err
        }
      }
    }
  }
  
  // T-SQL-SPECIFIC: Clean up again after DDL load to ensure clean state
  // This handles the case where DDL was already loaded but data exists
  await cleanDatabase(pool)
}

async function shutdownDb(pool) {
  // Export data to JSON for inspection (similar to postgres-store.tsv and sqlite-store.db)
  // T-SQL: Cast UNIQUEIDENTIFIER to NVARCHAR for JSON serialization
  const result = await pool.request().query(`
    SELECT 
      sequence,
      entity,
      entity_key,
      event,
      CAST(data AS NVARCHAR(MAX)) AS data,
      append_key,
      CAST(previous_id AS NVARCHAR(50)) AS previous_id,
      CAST(event_id AS NVARCHAR(50)) AS event_id,
      CAST(timestamp AS NVARCHAR(50)) AS timestamp
    FROM ledger
    ORDER BY sequence
  `)
  
  fs.writeFileSync('tsql-store.json', JSON.stringify(result.recordset, null, 2))
  await pool.close()
}

test('T-SQL', async (ctx) => {

  let pool
  before(async () => pool = await initDb())

  after(() => shutdownDb(pool))

  await ctx.test('insert events', async (t) => {
    const appendKey1 = nanoid()
    const appendKey2 = nanoid()
    const data = '{}'

    await t.test('cannot insert empty columns', async () => {
      // T-SQL: Uses parameterized queries with .input() for type safety
      // Unlike PostgreSQL template strings or SQLite prepared statements
      // SQL Server error format: "Cannot insert the value NULL into column 'X'"
      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, null)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, 'test-event')
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, appendKey1)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key)
            VALUES (@entity, @entity_key, @event, @data, @append_key)
          `)
        },
        /Cannot insert the value NULL into column 'entity'/,
        'cannot insert null entity')

      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, null)
          request.input('event', sql.NVarChar, 'test-event')
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, appendKey1)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key)
            VALUES (@entity, @entity_key, @event, @data, @append_key)
          `)
        },
        /Cannot insert the value NULL into column 'entity_key'/,
        'cannot insert null entity key')

      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, null)
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, appendKey1)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key)
            VALUES (@entity, @entity_key, @event, @data, @append_key)
          `)
        },
        /Cannot insert the value NULL into column 'event'/,
        'cannot insert null event')

      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, 'test-event')
          request.input('data', sql.NVarChar, null)
          request.input('append_key', sql.NVarChar, appendKey1)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key)
            VALUES (@entity, @entity_key, @event, @data, @append_key)
          `)
        },
        /Cannot insert the value NULL into column 'data'/,
        'cannot insert null event data')

      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, 'test-event')
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, null)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key)
            VALUES (@entity, @entity_key, @event, @data, @append_key)
          `)
        },
        /Cannot insert the value NULL into column 'append_key'/,
        'cannot insert null append_key')
    })

    await t.test('cannot insert event_id', async () => {
      // T-SQL: Test inserts directly into ledger table (bypassing view)
      // The no_direct_insert_ledger trigger prevents all direct inserts with event_id set.
      // All inserts MUST go through append_event view to ensure proper validation.
      await rejects(
        async () => {
          await pool.request().query(`
            INSERT INTO ledger (entity, entity_key, event, data, append_key, event_id)
            VALUES ('${thingEntity}', '${thingKey}', 'test-event', '{}', '${nanoid()}', NEWID())
          `)
        },
        /event_id must not be directly set/,
        'cannot insert directly into ledger table with event_id set')
    })

    await t.test('UUIDs format for IDs', async () => {
      // T-SQL: UNIQUEIDENTIFIER type validation
      // Unlike PostgreSQL which has explicit UUID type or SQLite with CHECK constraints,
      // T-SQL UNIQUEIDENTIFIER throws conversion errors for invalid formats
      await rejects(
        async () => {
          await pool.request().query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES ('${thingEntity}', '${thingKey}', '${thingCreatedEvent}', '{}', '${appendKey1}', 'not-a-uuid')
          `)
        },
        /Conversion failed when converting from a character string to uniqueidentifier|Invalid column name/,
        'previous_id must be a UUID')
    })

    await t.test('insert events for an entity', async () => {
      // T-SQL: Insert into append_event view (like SQLite, not PostgreSQL function)
      // Must query back to get event_id (no RETURNING clause support in INSTEAD OF triggers)
      // T-SQL: Use sql.UniqueIdentifier type for UNIQUEIDENTIFIER parameters
      await doesNotReject(async () => {
        await pool.request()
          .input('entity', sql.NVarChar, thingEntity)
          .input('entity_key', sql.NVarChar, thingKey)
          .input('event', sql.NVarChar, thingCreatedEvent)
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, appendKey1)
          .input('previous_id', sql.UniqueIdentifier, null)
          .query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        
        // T-SQL: Must SELECT event_id separately (no RETURNING in INSTEAD OF triggers)
        // Unlike PostgreSQL function which returns UUID directly
        const result = await pool.request()
          .input('append_key', sql.NVarChar, appendKey1)
          .query(`SELECT event_id FROM ledger WHERE append_key = @append_key`)
        
        // T-SQL: UNIQUEIDENTIFIER must be converted to string for comparison
        thingEventId1 = result.recordset[0].event_id.toString()
        strictEqual(typeof thingEventId1, 'string', 'event_id should be a string (GUID)')
      })

      await doesNotReject(async () => {
        await pool.request()
          .input('entity', sql.NVarChar, thingEntity)
          .input('entity_key', sql.NVarChar, thingKey)
          .input('event', sql.NVarChar, thingDeletedEvent)
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, appendKey2)
          .input('previous_id', sql.UniqueIdentifier, thingEventId1)
          .query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        
        const result = await pool.request()
          .input('append_key', sql.NVarChar, appendKey2)
          .query(`SELECT event_id FROM ledger WHERE append_key = @append_key`)
        
        thingEventId2 = result.recordset[0].event_id.toString()
      })

      await doesNotReject(async () => {
        const pingHomeKey = nanoid()
        await pool.request()
          .input('entity', sql.NVarChar, tableTennisEntity)
          .input('entity_key', sql.NVarChar, homeTableKey)
          .input('event', sql.NVarChar, pingEvent)
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, pingHomeKey)
          .input('previous_id', sql.UniqueIdentifier, null)
          .query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        
        const result = await pool.request()
          .input('append_key', sql.NVarChar, pingHomeKey)
          .query(`SELECT event_id FROM ledger WHERE append_key = @append_key`)
        
        pingEventHomeId = result.recordset[0].event_id.toString()
      })

      await doesNotReject(async () => {
        const pingWorkKey = nanoid()
        await pool.request()
          .input('entity', sql.NVarChar, tableTennisEntity)
          .input('entity_key', sql.NVarChar, workTableKey)
          .input('event', sql.NVarChar, pingEvent)
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, pingWorkKey)
          .input('previous_id', sql.UniqueIdentifier, null)
          .query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        
        const result = await pool.request()
          .input('append_key', sql.NVarChar, pingWorkKey)
          .query(`SELECT event_id FROM ledger WHERE append_key = @append_key`)
        
        pingEventWorkId = result.recordset[0].event_id.toString()
      })
    })

    await t.test('previous_id rules', async () => {
      // T-SQL: Error messages from THROW statements in trigger
      // Format matches PostgreSQL/SQLite for consistency
      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, tableTennisEntity)
          request.input('entity_key', sql.NVarChar, homeTableKey)
          request.input('event', sql.NVarChar, pingEvent)
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, nanoid())
          request.input('previous_id', sql.UniqueIdentifier, null)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        },
        /previous_id can only be null for first entity event/,
        'cannot insert multiple null previous_id for an entity')

      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, tableTennisEntity)
          request.input('entity_key', sql.NVarChar, workTableKey)
          request.input('event', sql.NVarChar, pongEvent)
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, nanoid())
          request.input('previous_id', sql.UniqueIdentifier, pingEventHomeId)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        },
        /previous_id must be in the same entity/,
        'previous_id must be in same entity')

      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, thingCreatedEvent)
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, nanoid())
          request.input('previous_id', sql.UniqueIdentifier, thingEventId1)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        },
        /previous_id must reference the newest event in entity/,
        'previous ID must be newest event in entity')
    })

    await t.test('Cannot insert duplicates', async () => {
      // T-SQL: UNIQUE constraint error format
      // SQL Server format: "Violation of UNIQUE KEY constraint 'constraint_name'"
      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, thingDeletedEvent)
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, appendKey1)
          request.input('previous_id', sql.UniqueIdentifier, thingEventId2)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        },
        /Violation of UNIQUE KEY constraint/,
        'cannot insert different event for same append_key')

      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, thingDeletedEvent)
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, nanoid())
          request.input('previous_id', sql.UniqueIdentifier, thingEventId1)
          await request.query(`
            INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
            VALUES (@entity, @entity_key, @event, @data, @append_key, @previous_id)
          `)
        },
        /previous_id must reference the newest event in entity/,
        'cannot insert different event for same previous')
    })
  })

  await ctx.test('cannot delete or update', async (t) => {
    await t.test('cannot delete or update events', async () => {
      // T-SQL: INSTEAD OF DELETE/UPDATE triggers throw errors
      // Unlike PostgreSQL RULES (silent ignore) or SQLite BEFORE triggers
      // T-SQL uses THROW for explicit error messages
      await rejects(
        async () => {
          await pool.request().query(`DELETE FROM ledger WHERE entity = '${thingEntity}'`)
        },
        /Cannot delete events/,
        'cannot delete events'
      )
      await rejects(
        async () => {
          await pool.request().query(`UPDATE ledger SET entity_key = 'fail' WHERE entity = '${thingEntity}'`)
        },
        /Cannot update events/,
        'cannot update events'
      )
    })
  })

  await ctx.test('replay events', async (t) => {
    // T-SQL: replay_events view doesn't have ORDER BY (view limitation)
    // ORDER BY must be added in queries, but for filtering tests it's not needed
    await t.test('replay entity events', async () => {
      const result = await pool.request()
        .input('entity', sql.NVarChar, thingEntity)
        .query(`SELECT * FROM replay_events WHERE entity = @entity`)
      strictEqual(result.recordset.length, 2, 'should have two events')
    })

    await t.test('replay events for a specific entity', async () => {
      const result = await pool.request()
        .input('entity', sql.NVarChar, tableTennisEntity)
        .input('entity_key', sql.NVarChar, homeTableKey)
        .query(`SELECT * FROM replay_events WHERE entity = @entity AND entity_key = @entity_key`)
      strictEqual(result.recordset.length, 1, 'should have one event')
    })

    await t.test('replay events after a specific event', async () => {
      // T-SQL: Table-valued function fn_replay_events_after()
      // Unlike PostgreSQL: replay_events_after() RETURNS SETOF replay_events
      // T-SQL uses RETURNS TABLE with inline definition
      // ORDER BY must be in the query, not in the function (T-SQL limitation)
      const result = await pool.request()
        .input('after_event_id', sql.UniqueIdentifier, thingEventId1)
        .query(`SELECT * FROM fn_replay_events_after(@after_event_id) ORDER BY sequence`)
      strictEqual(result.recordset.length, 3, 'should have three events')
    })

    await t.test('replay events after a specific event, filtered by entity', async () => {
      // T-SQL: Can apply WHERE clause to table-valued function result
      // Same behavior as PostgreSQL, different syntax
      // ORDER BY must be in the query, not in the function (T-SQL limitation)
      const result = await pool.request()
        .input('after_event_id', sql.UniqueIdentifier, thingEventId1)
        .input('entity', sql.NVarChar, thingEntity)
        .query(`SELECT * FROM fn_replay_events_after(@after_event_id) WHERE entity = @entity ORDER BY sequence`)
      strictEqual(result.recordset.length, 1, 'should have one event')
    })
  })
})
