import { throws, doesNotThrow, strictEqual } from 'node:assert/strict'
import { before, after, test } from 'node:test'
import fs from 'fs'
import initSqlJs from 'sql.js'
import { nanoid } from 'nanoid'


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


async function initDb() {
  const SQL = await initSqlJs()
  const db = new SQL.Database()
  loadDdl(db)
  return db
}

function loadDdl(db) {
  const createScript = fs.readFileSync('./sqlite-event-store.ddl', 'utf8')
  db.run(createScript)
}

function shutdownDb(db) {
  const data = db.export()
  const buffer = Buffer.from(data)
  fs.writeFileSync('sqlite-store.db', buffer)
  db.close()
}

test('SQLite', async (ctx) => {

  let db
  before(async () => db = await initDb())

  after(() => shutdownDb(db))

  await ctx.test('insert events', async (t) => {
    const stmt = db.prepare('INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id) VALUES (?, ?, ?, ?, ?, ?)')

    const appendKey1 = nanoid()
    const appendKey2 = nanoid()
    let thingEventId1 = nanoid()
    let thingEventId2 = nanoid()

    let pingEventHomeId = nanoid()
    let pingEventWorkId = nanoid()


    const data = '{}'

    await t.test('cannot insert empty columns', () => {
      throws(
        () => stmt.run([null, thingKey, thingCreatedEvent, data, appendKey1]),
        /NOT NULL constraint failed: ledger\.entity/,
        'cannot insert null entity')
      throws(
        () => stmt.run([thingEntity, null, thingCreatedEvent, data, appendKey1]),
        /NOT NULL constraint failed: ledger\.entity_key/,
        'cannot insert null entity key')
      throws(
        () => stmt.run([thingEntity, thingKey, null, data, appendKey1]),
        /NOT NULL constraint failed: ledger\.event/,
        'cannot insert null event')
      throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, null, appendKey1]),
        /NOT NULL constraint failed: ledger\.data/,
        'cannot insert null event data')

      throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, null]),
        /NOT NULL constraint failed: ledger\.append_key/,
        'cannot insert null append key')
    })

    // Cannot use RETURNING to get the event_id, as sqlite cannot access the generated event_id during INSERT.
    // Need to SELECT it like this.
    const appendStmt = db.prepare(`
INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING (SELECT event_id FROM ledger WHERE append_key = $5) as event_id`)

    await t.test('insert events for an entity', () => {
      doesNotThrow(() => [thingEventId1] = appendStmt.get([thingEntity, thingKey, thingCreatedEvent, data, appendKey1, null]))
      doesNotThrow(() => [thingEventId2] = appendStmt.get([thingEntity, thingKey, thingDeletedEvent, data, appendKey2, thingEventId1]))
      doesNotThrow(() => [pingEventHomeId] = appendStmt.get([tableTennisEntity, homeTableKey, pingEvent, data, nanoid(), null]))
      doesNotThrow(() => [pingEventWorkId] = appendStmt.get([tableTennisEntity, workTableKey, pingEvent, data, nanoid(), null]))
    })

    await t.test('previous_id rules', () => {
      throws(
        () => stmt.run([tableTennisEntity, homeTableKey, pingEvent, data, nanoid(), null]),
        /previous_id can only be null for first entity event/,
        'cannot insert multiple null previous ID for an entity')
      throws(
        () => stmt.run([tableTennisEntity, workTableKey, pongEvent, data, nanoid(), pingEventHomeId]),
        /previous_id must be in same entity/,
        'previous ID must be in same entity')
      throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, nanoid(), thingEventId1]),
        /previous_id must reference the newest event in entity/,
        'previous ID must be newest event in entity')
    })

    await t.test('Cannot insert duplicates', () => {
      throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, appendKey1, thingEventId2]),
        /UNIQUE constraint failed: ledger\.append_key/,
        'cannot insert different event for same append key')
      throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, nanoid(), thingEventId1]),
        /previous_id must reference the newest event in entity/,
        'cannot insert different event for same previous ID')
    })
  })

  await ctx.test('cannot delete or update', async (t) => {
    await t.test('cannot delete or update events', () => {
      throws(
        () => db.exec(`DELETE FROM ledger WHERE entity = '${thingEntity}'`),
        /Cannot delete events from the ledger/,
        'cannot delete events'
      )
      throws(
        () => db.exec(`UPDATE ledger SET entity_key = 'fail' WHERE entity = '${thingEntity}'`),
        /Cannot update events in the ledger/,
        'cannot update events'
      )
    })
  })


  await ctx.test('replay events', async (t) => {
    await t.test('replay entity events', async () => {
      const [{values}] = await db.exec(`SELECT * FROM replay_events WHERE entity = '${thingEntity}'`)
      strictEqual(values.length, 2, 'should have two events')
    })

    await t.test('replay single entity event', async () => {
      const [{values}] = await db.exec(`SELECT * FROM replay_events WHERE entity = '${tableTennisEntity}' AND entity_key = '${homeTableKey}'`)
      strictEqual(values.length, 1, 'should have one event')
    })
  })
})
