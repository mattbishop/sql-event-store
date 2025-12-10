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
 *    - T-SQL: Uses stored procedure append_event (returns @event_id via OUTPUT)
 *    → Reason: T-SQL functions cannot be used like PostgreSQL's RETURNING pattern; stored procedure is cleaner than view+trigger
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
  const dockerComposePath = join(process.cwd(), 'docker-compose.yml')
  const dockerComposeDir = process.cwd()
  
  try {
    // Stop and remove containers and volumes
    console.log('Stopping Docker containers and removing volumes...')
    execSync(`docker-compose -f "${dockerComposePath}" down -v`, {
      cwd: dockerComposeDir,
      stdio: 'inherit'
    })
    // Ensure old sqlserver container with fixed name is removed (avoid name conflict)
    try {
      execSync('docker rm -f sql-event-store', { stdio: 'ignore' })
    } catch (_) {
      // ignore if not present
    }
    
    // Start SQL Server container (root compose includes postgres too)
    console.log('Starting Docker containers...')
    execSync(`docker-compose -f "${dockerComposePath}" up -d sqlserver`, {
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
      // Stored procedure append_event validates via NOT NULL constraints (propagated from table)
      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, null)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, 'test-event')
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, appendKey1)
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
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
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
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
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
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
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
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
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
        },
        /Cannot insert the value NULL into column 'append_key'/,
        'cannot insert null append_key')
    })

    await t.test('cannot insert directly into ledger', async () => {
      // Direct INSERT must be blocked by no_direct_insert_ledger trigger
      await rejects(
        async () => {
          await pool.request().query(`
            INSERT INTO ledger (entity, entity_key, event, data, append_key)
            VALUES ('${thingEntity}', '${thingKey}', 'test-event', '{}', '${nanoid()}')
          `)
        },
        /Use append_event procedure to insert events into the ledger/,
        'direct insert into ledger must fail')
    })

    await t.test('UUIDs format for IDs', async () => {
      // T-SQL: UNIQUEIDENTIFIER type validation
      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, thingCreatedEvent)
          request.input('data', sql.NVarChar, '{}')
          request.input('append_key', sql.NVarChar, appendKey1)
          request.input('previous_id', sql.NVarChar, 'not-a-uuid') // force conversion error
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
        },
        /Conversion failed.*uniqueidentifier|Error converting data type.*uniqueidentifier/,
        'previous_id must be a UUID')
    })

    await t.test('insert events for an entity', async () => {
      // Stored procedure append_event returns event_id via OUTPUT
      await doesNotReject(async () => {
        const result = await pool.request()
          .input('entity', sql.NVarChar, thingEntity)
          .input('entity_key', sql.NVarChar, thingKey)
          .input('event', sql.NVarChar, thingCreatedEvent)
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, appendKey1)
          .input('previous_id', sql.UniqueIdentifier, null)
          .output('event_id', sql.UniqueIdentifier)
          .execute('append_event')
        thingEventId1 = result.output.event_id.toString()
        strictEqual(typeof thingEventId1, 'string', 'event_id should be a string (GUID)')
      })

      await doesNotReject(async () => {
        const result = await pool.request()
          .input('entity', sql.NVarChar, thingEntity)
          .input('entity_key', sql.NVarChar, thingKey)
          .input('event', sql.NVarChar, thingDeletedEvent)
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, appendKey2)
          .input('previous_id', sql.UniqueIdentifier, thingEventId1)
          .output('event_id', sql.UniqueIdentifier)
          .execute('append_event')
        thingEventId2 = result.output.event_id.toString()
      })

      await doesNotReject(async () => {
        const pingHomeKey = nanoid()
        const result = await pool.request()
          .input('entity', sql.NVarChar, tableTennisEntity)
          .input('entity_key', sql.NVarChar, homeTableKey)
          .input('event', sql.NVarChar, pingEvent)
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, pingHomeKey)
          .input('previous_id', sql.UniqueIdentifier, null)
          .output('event_id', sql.UniqueIdentifier)
          .execute('append_event')
        pingEventHomeId = result.output.event_id.toString()
      })

      await doesNotReject(async () => {
        const pingWorkKey = nanoid()
        const result = await pool.request()
          .input('entity', sql.NVarChar, tableTennisEntity)
          .input('entity_key', sql.NVarChar, workTableKey)
          .input('event', sql.NVarChar, pingEvent)
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, pingWorkKey)
          .input('previous_id', sql.UniqueIdentifier, null)
          .output('event_id', sql.UniqueIdentifier)
          .execute('append_event')
        pingEventWorkId = result.output.event_id.toString()
      })
    })

    await t.test('first event per stream enforced', async () => {
      const streamEntity = 'unique-stream'
      const streamKey = nanoid()
      const data = '{}'

      await doesNotReject(async () => {
        await pool.request()
          .input('entity', sql.NVarChar, streamEntity)
          .input('entity_key', sql.NVarChar, streamKey)
          .input('event', sql.NVarChar, 'stream-created')
          .input('data', sql.NVarChar, data)
          .input('append_key', sql.NVarChar, nanoid())
          .input('previous_id', sql.UniqueIdentifier, null)
          .output('event_id', sql.UniqueIdentifier)
          .execute('append_event')
      })

      await rejects(
        async () => {
          await pool.request()
            .input('entity', sql.NVarChar, streamEntity)
            .input('entity_key', sql.NVarChar, streamKey)
            .input('event', sql.NVarChar, 'stream-created-again')
            .input('data', sql.NVarChar, data)
            .input('append_key', sql.NVarChar, nanoid())
            .input('previous_id', sql.UniqueIdentifier, null)
            .output('event_id', sql.UniqueIdentifier)
            .execute('append_event')
        },
        /previous_id can only be null for first entity event|uq_first_event_per_stream/,
        'second first-event for same stream must fail')
    })

    await t.test('previous_id rules', async () => {
      // T-SQL: Error messages from THROW statements in stored procedure
      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, tableTennisEntity)
          request.input('entity_key', sql.NVarChar, homeTableKey)
          request.input('event', sql.NVarChar, pingEvent)
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, nanoid())
          request.input('previous_id', sql.UniqueIdentifier, null)
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
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
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
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
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
        },
        /previous_id must reference the newest event in entity/,
        'previous ID must be newest event in entity')
    })

    await t.test('Cannot insert duplicates', async () => {
      // T-SQL: UNIQUE constraint error format
      await rejects(
        async () => {
          const request = pool.request()
          request.input('entity', sql.NVarChar, thingEntity)
          request.input('entity_key', sql.NVarChar, thingKey)
          request.input('event', sql.NVarChar, thingDeletedEvent)
          request.input('data', sql.NVarChar, data)
          request.input('append_key', sql.NVarChar, appendKey1)
          request.input('previous_id', sql.UniqueIdentifier, thingEventId2)
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
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
          request.output('event_id', sql.UniqueIdentifier)
          await request.execute('append_event')
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
        .input('entity', sql.NVarChar, thingEntity)
        .input('entity_key', sql.NVarChar, thingKey)
        .query(`SELECT * FROM fn_replay_events_after(@after_event_id) WHERE entity = @entity AND entity_key = @entity_key ORDER BY sequence`)
      strictEqual(result.recordset.length, 1, 'should have one event after first')
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
