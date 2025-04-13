/*
  This test suite exercises the database schema with incorrect data, duplicate data and other likely real-world problems.
 */
const tape = require('tape')
const _test = require('tape-promise').default;
const test = _test(tape)
const fs = require('fs');
const nanoid = require('nanoid').nanoid;
const {PGlite} = require('@electric-sql/pglite');
const {query} = require("@electric-sql/pglite/template");

const thingEntity = 'thing';
const thingCreatedEvent = 'thing-created';
const thingDeletedEvent = 'thing-deleted';

const tableTennisEntity = 'table-tennis';
const pingEvent = 'ball-pinged';
const pongEvent = 'ball-ponged';

async function initDb() {
  // const db = await PGlite.create('./postgres/')
  const db = await PGlite.create('memory://')
  await loadDdl(db);
  return db;
}

async function loadDdl(db) {
  const createScript = fs.readFileSync('./postgres-event-store.ddl', 'utf8');
  return db.exec(createScript);
}

async function shutdownDb(db) {
  await db.close();
}

// use t.plan() for async testing too.
test('setup', async setup => {
  const db = await initDb();

/*
  setup.test('running setup on existing db succeeds', async t => {
    await loadDdl(db);
    t.pass(true);
    t.end();
  });
*/


  setup.test('insert events', t => {
    const stmt = 'INSERT INTO events (entity, entityKey, event, data, eventId, commandId, previousId) VALUES ($1, $2, $3, $4, $5, $6, $7)';
    const thingKey = '1';
    const homeTableKey = 'home';
    const workTableKey = 'work';

    const appendKey1 = nanoid();
    const appendKey2 = nanoid();
    const data = {};


    t.test('cannot insert empty columns', async assert => {
      await assert.rejects(
        () => db.query(stmt, [null, thingKey, thingCreatedEvent, data, thingEventId1, commandId1, null]),
        /error: null value in column "entity" of relation "events" violates not-null constraint/,
        'cannot insert null entity');
      await assert.rejects(
        () => db.query(stmt, [thingEntity, null, thingCreatedEvent, data, thingEventId1, commandId1, null]),
        /error: null value in column "entitykey" of relation "events" violates not-null constraint/,
        'cannot insert null entity key');
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, null, data, thingEventId1, commandId1, null]),
        /error: null value in column "event" of relation "events" violates not-null constraint/,
        'cannot insert null event');
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, thingCreatedEvent, null, thingEventId1, commandId1, null]),
        /error: null value in column "data" of relation "events" violates not-null constraint/,
        'cannot insert null event data');
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, thingCreatedEvent, data, null, commandId1, null]),
        /error: null value in column "eventid" of relation "events" violates not-null constraint/,
        'cannot insert null event id');
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, thingCreatedEvent, data, thingEventId1, null, null]),
        /error: null value in column "commandid" of relation "events" violates not-null constraint/,
        'cannot insert null command');
      assert.end();
    });

    t.test('UUIDs format for IDs', async assert => {
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, thingCreatedEvent, data, 'not-a-uuid', commandId1, null]),
        /error: invalid input syntax for type uuid: "not-a-uuid"/,
        'eventId must be a UUID');
      await assert.rejects(
        () => db.sql`INSERT INTO ledger (entity, entity_key, event, data, append_key, event_id)
            VALUES (${thingEntity}, ${thingKey}, ${thingCreatedEvent}, ${data}, ${appendKey1}, '00000000-0000-4000-8000-000000000000')`,
        /error: event_id must not be directly set with INSERT statement, it is generated/,
        'cannot insert event_id');
      assert.end();
    });

    t.test('UUIDs format for IDs', async assert => {
      await assert.rejects(
        () => db.query(stmt.query, [thingEntity, thingKey, thingCreatedEvent, data, appendKey1, 'not-a-uuid']),
        /error: invalid input syntax for type uuid: "not-a-uuid"/,
        'previous_id must be a UUID');
      assert.end();
    });

    const appendStmt = query`INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
        VALUES (${'entity'}, ${'entity_key'}, ${'event'}, ${'data'}, ${'append_key'}, ${'previous_id'})
        RETURNING event_id;`

    let thingEventId1;
    let thingEventId2;

    let pingEventHomeId;
    let pingEventWorkId;

    t.test('insert events for an entity', async assert => {
      await assert.doesNotReject(() => db.query(stmt, [thingEntity, thingKey, thingCreatedEvent, data, thingEventId1, commandId1, null]));
      await assert.doesNotReject(() => db.query(stmt, [thingEntity, thingKey, thingDeletedEvent, data, thingEventId2, commandId2, thingEventId1]));
      await assert.doesNotReject(() => db.query(stmt, [tableTennisEntity, homeTableKey, pingEvent, data, pingEventHomeId, uuid.v4(), null]));
      await assert.doesNotReject(() => db.query(stmt, [tableTennisEntity, workTableKey, pingEvent, data, pingEventWorkId, uuid.v4(), null]));
      assert.end();
    });

    t.test('previous_id rules', async assert => {
      await assert.rejects(
        () => db.query(appendStmt.query, [tableTennisEntity, homeTableKey, pingEvent, data, nanoid(), null]),
        /error: previous_id can only be null for first entity event/,
        'cannot insert multiple null previous_id for an entity');
      await assert.rejects(
        () => db.query(appendStmt.query, [tableTennisEntity, workTableKey, pongEvent, data, nanoid(), pingEventHomeId]),
        /error: previous_id must be in the same entity/,
        'previous_id must be in same entity');
      assert.end();
    });

    t.test('Cannot insert duplicates', async assert => {
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, thingDeletedEvent, data, uuid.v4(), commandId1, thingEventId2]),
        /error: duplicate key value violates unique constraint "events_commandid_key"/,
        'cannot insert different event for same command');
      await assert.rejects(
        () => db.query(appendStmt.query, [thingEntity, thingKey, thingDeletedEvent, data, nanoid(), thingEventId1]),
        /error: duplicate key value violates unique constraint "ledger_previous_id_key"/,
        'cannot insert different event for same previous');
      assert.end();
    });
  });

  setup.test('cannot delete or update', t => {

    t.test('cannot delete or update events', async assert => {
      await assert.doesNotReject(
        () => db.query(`DELETE FROM ledger WHERE entity = '${thingEntity}'`),
        /Cannot delete events/,
        'cannot delete events'
      );
      await assert.doesNotReject(
        () => db.query(`UPDATE ledger SET entity_key = 'fail' WHERE entity = '${thingEntity}'`),
        /Cannot update events/,
        'cannot update events'
      );
      assert.end();
    });
  });

  test.onFinish(async () => await shutdownDb(db));
});
