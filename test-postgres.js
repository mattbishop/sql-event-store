/*
  This test suite exercises the database schema with incorrect data, duplicate data and other likely real-world problems.
 */
const tape = require('tape')
const _test = require('tape-promise').default;
const test = _test(tape)
const fs = require('fs');
const pg = require('pg');
const uuid = require('uuid');

const thingEntity = 'thing';
const thingCreatedEvent = 'thing-created';
const thingDeletedEvent = 'thing-deleted';

const tableTennisEntity = 'table-tennis';
const pingEvent = 'ball-pinged';
const pongEvent = 'ball-ponged';

async function initDb() {
  const db = new pg.Client()
  db.connect()
  await loadDdl(db);
  return db;
}

async function loadDdl(db) {
  const createScript = fs.readFileSync('./postgres-event-store.ddl', 'utf8');
  return db.query(createScript);
}

async function shutdownDb(db) {
  await db.end();
}

// use t.plan() for async testing too.
test('setup', async setup => {
  const db = await initDb();

  setup.test('running setup on existing db succeeds', async t => {
    await loadDdl(db);
    t.pass(true);
    t.end();
  });

  setup.test('insert entity_events', t => {

    const stmt = 'INSERT INTO entity_events (entity, event) VALUES ($1, $2)';

    t.test('cannot insert null fields', async assert => {
      await assert.rejects(
        () => db.query(stmt, [null, thingCreatedEvent]),
        /error: null value in column "entity" of relation "entity_events" violates not-null constraint/,
        'cannot insert null entity');
      await assert.rejects(
        () => db.query(stmt, [thingEntity, null]),
        /error: null value in column "event" of relation "entity_events" violates not-null constraint/,
        'cannot insert null event');
      assert.end();
    });

    t.test('insert entity_events', async assert => {
      await assert.doesNotReject(() => db.query(stmt, [thingEntity, thingCreatedEvent]));
      await assert.doesNotReject(() => db.query(stmt, [thingEntity, thingDeletedEvent]));
      await assert.doesNotReject(() => db.query(stmt, [tableTennisEntity, pingEvent]));
      await assert.doesNotReject(() => db.query(stmt, [tableTennisEntity, pongEvent]));
      assert.end();
    });

    t.test('insert duplicate entity events does not throw an Error', async assert => {
      assert.plan(4);
      await assert.doesNotReject(() => db.query(stmt, [thingEntity, thingCreatedEvent]));
      await assert.doesNotReject(() => db.query(stmt, [thingEntity, thingCreatedEvent]));
      const result = await db.query(`SELECT COUNT(*) FROM entity_events WHERE entity = '${thingEntity}' AND event = '${thingCreatedEvent}'`);
      assert.equal(result.rows[0].count, '1');
      assert.pass('no entity event duplicates');
      assert.end();
    });
  });

  setup.test('insert events', t => {
    const stmt = 'INSERT INTO events (entity, entityKey, event, data, eventId, commandId, previousId) VALUES ($1, $2, $3, $4, $5, $6, $7)';
    const thingKey = '1';
    const homeTableKey = 'home';
    const workTableKey = 'work';

    const commandId1 = uuid.v4();
    const commandId2 = uuid.v4();
    const thingEventId1 = uuid.v4();
    const thingEventId2 = uuid.v4();

    const pingEventHomeId = uuid.v4();
    const pingEventWorkId = uuid.v4();


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
        () => db.query(stmt, [thingEntity, thingKey, thingCreatedEvent, data, thingEventId1, 'not-a-uuid', null]),
        /error: invalid input syntax for type uuid: "not-a-uuid"/,
        'commandId must be a UUID');
      assert.end();
    });

    t.test('Cannot insert event from wrong entity', async assert => {
      await assert.rejects(
        () => db.query(stmt, [tableTennisEntity, thingKey, thingCreatedEvent, data, thingEventId1, commandId1, null]),
        /error: insert or update on table "events" violates foreign key constraint "events_entity_event_fkey"/,
        'cannot insert event in wrong entity');
      assert.end();
    });

    t.test('insert events for an entity', async assert => {
      await assert.doesNotReject(() => db.query(stmt, [thingEntity, thingKey, thingCreatedEvent, data, thingEventId1, commandId1, null]));
      await assert.doesNotReject(() => db.query(stmt, [thingEntity, thingKey, thingDeletedEvent, data, thingEventId2, commandId2, thingEventId1]));
      await assert.doesNotReject(() => db.query(stmt, [tableTennisEntity, homeTableKey, pingEvent, data, pingEventHomeId, uuid.v4(), null]));
      await assert.doesNotReject(() => db.query(stmt, [tableTennisEntity, workTableKey, pingEvent, data, pingEventWorkId, uuid.v4(), null]));
      assert.end();
    });

    t.test('previousId rules', async assert => {
      await assert.rejects(
        () => db.query(stmt, [tableTennisEntity, homeTableKey, pingEvent, data, pingEventHomeId, uuid.v4(), null]),
        /error: previousid can only be null for first entity event/,
        'cannot insert multiple null previousid for an entity');
      await assert.rejects(
        () => db.query(stmt, [tableTennisEntity, workTableKey, pongEvent, data, uuid.v4(), uuid.v4(), pingEventHomeId]),
        /error: previousid must be in the same entity/,
        'previousid must be in same entity');
      assert.end();
    });

    t.test('Cannot insert duplicates', async assert => {
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, thingCreatedEvent, data, thingEventId2, commandId2, thingEventId1]),
        /error: duplicate key value violates unique constraint "events_eventid_key"/,
        'cannot insert complete duplicate event');
      await assert.rejects(
        () => db.query(stmt, [tableTennisEntity, homeTableKey, pongEvent, data, pingEventHomeId, uuid.v4(), pingEventHomeId]),
        /error: duplicate key value violates unique constraint "events_eventid_key"/,
        'cannot insert different event for same id');
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, thingDeletedEvent, data, uuid.v4(), commandId1, thingEventId2]),
        /error: duplicate key value violates unique constraint "events_commandid_key"/,
        'cannot insert different event for same command');
      await assert.rejects(
        () => db.query(stmt, [thingEntity, thingKey, thingDeletedEvent, data, uuid.v4(), uuid.v4(), thingEventId1]),
        /error: duplicate key value violates unique constraint "events_previousid_key"/,
        'cannot insert different event for same previous');
      assert.end();
    });
  });

  setup.test('cannot delete or update', t => {
    t.test('cannot delete or update entity_events', async assert => {
      await assert.doesNotReject(
        async () => await db.query('DELETE FROM entity_events WHERE entity = $1', [tableTennisEntity]),
        'ignores delete entity_events');
      const deleteResult = await db.query('SELECT COUNT(*) FROM entity_events WHERE entity = $1', [tableTennisEntity])
      assert.equal(deleteResult.rows[0].count, '2')

      await assert.doesNotReject(
        () => db.query('UPDATE entity_events SET event = $1 WHERE entity = $2', ['fail', tableTennisEntity]),
        'ignores update entity_events');
      const updateResult = await db.query('SELECT event FROM entity_events WHERE entity = $1', [tableTennisEntity])
      assert.equal(updateResult.rows.length, 2)
      assert.ok(updateResult.rows.some(r => r.event === pingEvent))
      assert.ok(updateResult.rows.some(r => r.event === pongEvent))

      assert.end();
    });

    t.test('cannot delete or update events', async assert => {
      await assert.doesNotReject(
        () => db.query(`DELETE FROM events WHERE entity = '${thingEntity}'`),
        /Cannot delete events/,
        'cannot delete events'
      );
      await assert.doesNotReject(
        () => db.query(`UPDATE events SET entityKey = 'fail' WHERE entity = '${thingEntity}'`),
        /Cannot update events/,
        'cannot update events'
      );
      assert.end();
    });
  });

  test.onFinish(async () => await shutdownDb(db));
});
