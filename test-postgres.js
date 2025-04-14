import test from 'tape-promise/tape.js'
import fs from 'fs'
import { nanoid } from 'nanoid'
import { PGlite } from '@electric-sql/pglite'
import { query } from '@electric-sql/pglite/template'


/**
 This test suite exercises the database schema with incorrect data, duplicate data and other likely real-world problems.
 */

const thingEntity = 'thing'
const thingCreatedEvent = 'thing-created'
const thingDeletedEvent = 'thing-deleted'

const tableTennisEntity = 'table-tennis'
const pingEvent = 'ball-pinged'
const pongEvent = 'ball-ponged'

async function initDb() {
  const db = await PGlite.create('memory://')
  await loadDdl(db)
  return db
}

async function loadDdl(db) {
  const createScript = fs.readFileSync('./postgres-event-store.ddl', 'utf8')
  return db.exec(createScript)
}

async function shutdownDb(db) {
  const columns = await db.query("SELECT column_name FROM information_schema.columns WHERE table_name = 'ledger'")
  const ret = await db.query("COPY ledger TO '/dev/blob';")
  const data = await ret.blob.text()
  const body = columns.rows
      .map(r => r.column_name)
      .join('\t')
      + '\n'
      + data.replaceAll('\\N', 'null')
  fs.writeFileSync('postgres-store.tsv', body)
  return db.close()
}

// use t.plan() for async testing too.
test('setup', async setup => {
  const db = await initDb()

  setup.test('insert events', t => {
    const stmt = query`INSERT INTO ledger (entity, entity_key, event, data, append_key, previous_id) 
VALUES (${'entity'}, ${'entityKey'}, ${'event'}, ${'data'}, ${'appendKey'}, ${'previousId'})`
    const thingKey = '1'
    const homeTableKey = 'home'
    const workTableKey = 'work'

    const appendKey1 = nanoid()
    const appendKey2 = nanoid()
    const data = {}


    t.test('cannot insert empty columns', async assert => {
      await assert.rejects(
        () => db.query(stmt.query, [null, thingKey, thingCreatedEvent, data, appendKey1, null]),
        /error: null value in column "entity" of relation "ledger" violates not-null constraint/,
        'cannot insert null entity')
      await assert.rejects(
        () => db.query(stmt.query, [thingEntity, null, thingCreatedEvent, data, appendKey1, null]),
        /error: null value in column "entity_key" of relation "ledger" violates not-null constraint/,
        'cannot insert null entity key')
      await assert.rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, null, data, appendKey1, null]),
        /error: null value in column "event" of relation "ledger" violates not-null constraint/,
        'cannot insert null event')
      await assert.rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, thingCreatedEvent, null, appendKey1, null]),
        /error: null value in column "data" of relation "ledger" violates not-null constraint/,
        'cannot insert null event data')
      await assert.rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, thingCreatedEvent, data, null, null]),
        /error: null value in column "append_key" of relation "ledger" violates not-null constraint/,
        'cannot insert null command')
      assert.end()
    })

    t.test('cannot insert event_id', async assert => {
      await assert.rejects(
        () => db.sql`INSERT INTO ledger (entity, entity_key, event, data, append_key, event_id)
            VALUES (${thingEntity}, ${thingKey}, ${thingCreatedEvent}, ${data}, ${appendKey1}, '00000000-0000-4000-8000-000000000000')`,
        /error: event_id must not be directly set with INSERT statement, it is generated/,
        'cannot insert event_id')
      assert.end()
    })

    t.test('UUIDs format for IDs', async assert => {
      await assert.rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, thingCreatedEvent, data, appendKey1, 'not-a-uuid']),
        /error: invalid input syntax for type uuid: "not-a-uuid"/,
        'previous_id must be a UUID')
      assert.end()
    })

    const appendStmt = query`INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
        VALUES (${'entity'}, ${'entity_key'}, ${'event'}, ${'data'}, ${'append_key'}, ${'previous_id'})
        RETURNING event_id`

    let thingEventId1
    let thingEventId2

    let pingEventHomeId
    let pingEventWorkId

    t.test('insert events for an entity', async assert => {
      await assert.doesNotReject(async () => {
        const {rows:[{event_id}]} = await db.query(appendStmt.query, [thingEntity, thingKey, thingCreatedEvent, data, appendKey1, null])
        thingEventId1 = event_id
      })
      await assert.doesNotReject(async () => {
        const {rows:[{event_id}]} = await db.query(appendStmt.query, [thingEntity, thingKey, thingDeletedEvent, data, appendKey2, thingEventId1])
        thingEventId2 = event_id
      })
      await assert.doesNotReject(async () => {
        const {rows:[{event_id}]} = await db.query(appendStmt.query, [tableTennisEntity, homeTableKey, pingEvent, data, nanoid(), null])
        pingEventHomeId = event_id
      })
      await assert.doesNotReject(async () => {
        const {rows:[{event_id}]} = await db.query(appendStmt.query, [tableTennisEntity, workTableKey, pingEvent, data, nanoid(), null])
        pingEventWorkId = event_id
      })
      assert.end()
    })

    t.test('previous_id rules', async assert => {
      await assert.rejects(
        () => db.query(appendStmt.query, [tableTennisEntity, homeTableKey, pingEvent, data, nanoid(), null]),
        /error: previous_id can only be null for first entity event/,
        'cannot insert multiple null previous_id for an entity')
      await assert.rejects(
        () => db.query(appendStmt.query, [tableTennisEntity, workTableKey, pongEvent, data, nanoid(), pingEventHomeId]),
        /error: previous_id must be in the same entity/,
        'previous_id must be in same entity')
      assert.end()
    })

    t.test('Cannot insert duplicates', async assert => {
      await assert.rejects(
        () => db.query(appendStmt.query, [thingEntity, thingKey, thingDeletedEvent, data, appendKey1, thingEventId2]),
        /error: duplicate key value violates unique constraint "ledger_append_key_key"/,
        'cannot insert different event for same command')
      await assert.rejects(
        () => db.query(appendStmt.query, [thingEntity, thingKey, thingDeletedEvent, data, nanoid(), thingEventId1]),
        /error: duplicate key value violates unique constraint "ledger_previous_id_key"/,
        'cannot insert different event for same previous')
      assert.end()
    })
  })

  setup.test('cannot delete or update', t => {

    t.test('cannot delete or update events', async assert => {
      await assert.doesNotReject(
        () => db.query(`DELETE FROM ledger WHERE entity = '${thingEntity}'`),
        /Cannot delete events/,
        'cannot delete events'
      )
      await assert.doesNotReject(
        () => db.query(`UPDATE ledger SET entity_key = 'fail' WHERE entity = '${thingEntity}'`),
        /Cannot update events/,
        'cannot update events'
      )
      assert.end()
    })
  })

  test.onFinish(async () => await shutdownDb(db))
})
