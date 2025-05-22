import { rejects, doesNotReject, strictEqual } from 'node:assert/strict'
import { before, after, test } from 'node:test'
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

const thingKey = '1'
const homeTableKey = 'home'
const workTableKey = 'work'

let thingEventId1
let thingEventId2

let pingEventHomeId
let pingEventWorkId


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

test('Postgres', async (ctx) => {

  let db
  before(async () => db = await initDb())

  after(() => shutdownDb(db))

  await ctx.test('insert events', async (t) => {
    const stmt = query`INSERT INTO ledger (entity, entity_key, event, data, append_key, previous_id) 
VALUES (${'entity'}, ${'entityKey'}, ${'event'}, ${'data'}, ${'appendKey'}, ${'previousId'})`

    const appendKey1 = nanoid()
    const appendKey2 = nanoid()
    const data = {}


    await t.test('cannot insert empty columns', async () => {
      await rejects(
        () => db.query(stmt.query, [null, thingKey, thingCreatedEvent, data, appendKey1, null]),
        /error: null value in column "entity" of relation "ledger" violates not-null constraint/,
        'cannot insert null entity')
      await rejects(
        () => db.query(stmt.query, [thingEntity, null, thingCreatedEvent, data, appendKey1, null]),
        /error: null value in column "entity_key" of relation "ledger" violates not-null constraint/,
        'cannot insert null entity key')
      await rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, null, data, appendKey1, null]),
        /error: null value in column "event" of relation "ledger" violates not-null constraint/,
        'cannot insert null event')
      await rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, thingCreatedEvent, null, appendKey1, null]),
        /error: null value in column "data" of relation "ledger" violates not-null constraint/,
        'cannot insert null event data')
      await rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, thingCreatedEvent, data, null, null]),
        /error: null value in column "append_key" of relation "ledger" violates not-null constraint/,
        'cannot insert null append_key')
    })

    await t.test('cannot insert event_id', async () => {
      await rejects(
        () => db.sql`INSERT INTO ledger (entity, entity_key, event, data, append_key, event_id)
            VALUES (${thingEntity}, ${thingKey}, ${thingCreatedEvent}, ${data}, ${appendKey1}, '00000000-0000-4000-8000-000000000000')`,
        /error: event_id must not be directly set with INSERT statement, it is generated/,
        'cannot insert event_id')
    })

    await t.test('UUIDs format for IDs', async () => {
      await rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, thingCreatedEvent, data, appendKey1, 'not-a-uuid']),
        /error: invalid input syntax for type uuid: "not-a-uuid"/,
        'previous_id must be a UUID')
    })

    const appendStmt = query`SELECT append_event (${'entity'}, ${'entity_key'}, ${'event'}, ${'data'}, ${'append_key'}, ${'previous_id'}) AS event_id`

    await t.test('insert events for an entity', async () => {
      await doesNotReject(async () => {
        const {rows:[{event_id}]} = await db.query(appendStmt.query, [thingEntity, thingKey, thingCreatedEvent, data, appendKey1, null])
        thingEventId1 = event_id
      })
      await doesNotReject(async () => {
        const {rows:[{event_id}]} = await db.query(appendStmt.query, [thingEntity, thingKey, thingDeletedEvent, data, appendKey2, thingEventId1])
        thingEventId2 = event_id
      })
      await doesNotReject(async () => {
        const {rows:[{event_id}]} = await db.query(appendStmt.query, [tableTennisEntity, homeTableKey, pingEvent, data, nanoid(), null])
        pingEventHomeId = event_id
      })
      await doesNotReject(async () => {
        const {rows:[{event_id}]} = await db.query(appendStmt.query, [tableTennisEntity, workTableKey, pingEvent, data, nanoid(), null])
        pingEventWorkId = event_id
      })
    })

    await t.test('previous_id rules', async () => {
      await rejects(
        () => db.query(appendStmt.query, [tableTennisEntity, homeTableKey, pingEvent, data, nanoid(), null]),
        /error: previous_id can only be null for first entity event/,
        'cannot insert multiple null previous_id for an entity')
      await rejects(
        () => db.query(appendStmt.query, [tableTennisEntity, workTableKey, pongEvent, data, nanoid(), pingEventHomeId]),
        /error: previous_id must be in the same entity/,
        'previous_id must be in same entity')
      await rejects(
          () => db.query(appendStmt.query, [thingEntity, thingKey, thingCreatedEvent, data, nanoid(), thingEventId1]),
          /error: previous_id must reference the newest event in entity/,
          'previous ID must be newest event in entity')
    })

    await t.test('Cannot insert duplicates', async () => {
      await rejects(
        () => db.query(appendStmt.query, [thingEntity, thingKey, thingDeletedEvent, data, appendKey1, thingEventId2]),
        /error: duplicate key value violates unique constraint "ledger_append_key_key"/,
        'cannot insert different event for same append_key')
      await rejects(
        () => db.query(appendStmt.query, [thingEntity, thingKey, thingDeletedEvent, data, nanoid(), thingEventId1]),
        /error: previous_id must reference the newest event in entity/,
        'cannot insert different event for same previous')
    })
  })

  await ctx.test('cannot delete or update', async (t) => {

    await t.test('cannot delete or update events', async () => {
      await doesNotReject(
        () => db.query(`DELETE FROM ledger WHERE entity = '${thingEntity}'`),
        /Cannot delete events/,
        'cannot delete events'
      )
      await doesNotReject(
        () => db.query(`UPDATE ledger SET entity_key = 'fail' WHERE entity = '${thingEntity}'`),
        /Cannot update events/,
        'cannot update events'
      )
    })
  })


  await ctx.test('replay events', async (t) => {
    await t.test('replay entity events', async () => {
      const {rows} = await db.query(`SELECT * FROM replay_events WHERE entity = '${thingEntity}'`)
      strictEqual(rows.length, 2, 'should have two events')
    })

    await t.test('replay events for a specific entity', async () => {
      const {rows} = await db.query(`SELECT * FROM replay_events WHERE entity = '${tableTennisEntity}' AND entity_key = '${homeTableKey}'`)
      strictEqual(rows.length, 1, 'should have one event')
    })

    await t.test('replay events after a specific event', async () => {
      const {rows} = await db.query(`SELECT * FROM replay_events_after('${thingEventId1}')`)
      strictEqual(rows.length, 3, 'should have three events')
    })

    await t.test('replay events after a specific event, filtered by entity', async () => {
      const {rows} = await db.query(`SELECT * FROM replay_events_after('${thingEventId1}') WHERE entity = '${thingEntity}'`)
      strictEqual(rows.length, 1, 'should have one event')
    })
  })
})
