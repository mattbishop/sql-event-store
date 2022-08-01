/*
  This test suite exercises the database schema with incorrect data, duplicate data and other likely real-world problems.
 */
const test = require('tape');
const fs = require('fs');
const initSqlJs = require('sql.js');
const uuid = require('uuid');

const thingEntity = 'thing';
const thingCreatedEvent = 'thing-created';
const thingDeletedEvent = 'thing-deleted';

const tableTennisEntity = 'table-tennis';
const pingEvent = 'ball-pinged';
const pongEvent = 'ball-ponged';

async function initDb() {
  const SQL = await initSqlJs();
  const db = new SQL.Database();
  loadDdl(db);
  return db;
}

function loadDdl(db) {
  const createScript = fs.readFileSync('./sqlite-event-store.ddl', 'utf8');
  return db.run(createScript);
}

function shutdownDb(db) {
  const data = db.export();
  const buffer = Buffer.from(data);
  fs.writeFileSync('test-event-store.sqlite', buffer);
  db.close();
}

test('setup', async setup => {
  const db = await initDb();

  setup.test('running setup on existing db succeeds', t => {
    t.doesNotThrow(() => loadDdl(db));
    t.end();
  });

  setup.test('insert entity_events', t => {

    const stmt = db.prepare('INSERT INTO entity_events (entity, event) VALUES (?, ?)');

    t.test('cannot insert null fields', assert => {
      assert.throws(
        () => stmt.run([null, thingCreatedEvent]),
        /Error: NOT NULL constraint failed: entity_events\.entity/,
        'cannot insert null entity');
      assert.throws(
        () => stmt.run([thingEntity, null]),
        /Error: NOT NULL constraint failed: entity_events\.event/,
        'cannot insert null entity');
      assert.end();
    });

    t.test('insert entity_events', assert => {
      assert.doesNotThrow(() => stmt.run([thingEntity, thingCreatedEvent]));
      assert.doesNotThrow(() => stmt.run([thingEntity, thingDeletedEvent]));
      assert.doesNotThrow(() => stmt.run([tableTennisEntity, pingEvent]));
      assert.doesNotThrow(() => stmt.run([tableTennisEntity, pongEvent]));
      assert.end();
    });

    t.test('insert duplicate entity events does not throw an Error', assert => {
      assert.doesNotThrow(() => stmt.run([thingEntity, thingCreatedEvent]));
      assert.doesNotThrow(() => stmt.run([thingEntity, thingCreatedEvent]));
      const result = db.exec(`SELECT COUNT(*) FROM entity_events WHERE entity = '${thingEntity}' AND event = '${thingCreatedEvent}'`);
      assert.equal(result[0].values[0][0], 1);
      assert.end();
    });
  });

  setup.test('insert events', t => {
    const stmt = db.prepare('INSERT INTO events (entity, entityKey, event, data, eventId, commandId, previousId) VALUES (?, ?, ?, ?, ?, ?, ?)');
    const thingKey = '1';
    const homeTableKey = 'home';
    const workTableKey = 'work';

    const commandId1 = uuid.v4();
    const commandId2 = uuid.v4();
    const thingEventId1 = uuid.v4();
    const thingEventId2 = uuid.v4();

    const pingEventHomeId = uuid.v4();
    const pingEventWorkId = uuid.v4();


    const data = '{}';

    t.test('cannot insert empty columns', assert => {
      assert.throws(
        () => stmt.run([null, thingKey, thingCreatedEvent, data, thingEventId1, commandId1]),
        /NOT NULL constraint failed: events\.entity/,
        'cannot insert null entity');
      assert.throws(
        () => stmt.run([thingEntity, null, thingCreatedEvent, data, thingEventId1, commandId1]),
        /NOT NULL constraint failed: events\.entityKey/,
        'cannot insert null entity key');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, null, data, thingEventId1, commandId1]),
        /NOT NULL constraint failed: events\.event/,
        'cannot insert null event');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, null, thingEventId1, commandId1]),
        /NOT NULL constraint failed: events\.data/,
        'cannot insert null event data');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, null, commandId1]),
        /NOT NULL constraint failed: events\.eventId/,
        'cannot insert null event id');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, thingEventId1, null]),
        /NOT NULL constraint failed: events\.commandId/,
        'cannot insert null command');
      assert.end();
    });

    t.test('UUIDs format for IDs', assert => {
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, 'not-a-uuid', commandId1]),
        /CHECK constraint failed: eventId/,
        'eventId must be a UUID');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, thingEventId1, 'not-a-uuid']),
        /CHECK constraint failed: commandId/,
        'commandId must be a UUID');
      assert.end();
    });

    t.test('Cannot insert event from wrong entity', assert => {
      assert.throws(
        () => stmt.run([tableTennisEntity, thingKey, thingCreatedEvent, data, thingEventId1, commandId1]),
        /FOREIGN KEY constraint failed/,
        'cannot insert event in wrong entity');
      assert.end();
    });

    t.test('insert events for an entity', assert => {
      assert.doesNotThrow(() => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, thingEventId1, commandId1]));
      assert.doesNotThrow(() => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, thingEventId2, commandId2, thingEventId1]));
      assert.doesNotThrow(() => stmt.run([tableTennisEntity, homeTableKey, pingEvent, data, pingEventHomeId, uuid.v4()]));
      assert.doesNotThrow(() => stmt.run([tableTennisEntity, workTableKey, pingEvent, data, pingEventWorkId, uuid.v4()]));
      assert.end();
    });

    t.test('previousId rules', assert => {
      assert.throws(
        () => stmt.run([tableTennisEntity, homeTableKey, pingEvent, data, pingEventHomeId, uuid.v4()]),
        /previousId can only be null for first entity event/,
        'cannot insert multiple null previousId for an entity');
      assert.throws(
        () => stmt.run([tableTennisEntity, workTableKey, pongEvent, data, uuid.v4(), uuid.v4(), pingEventHomeId]),
        /previousId must be in same entity/,
        'previousId must be in same entity');
      assert.end();
    });

    t.test('Cannot insert duplicates', assert => {
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, thingEventId2, commandId2, thingEventId1]),
        /UNIQUE constraint failed/,
        'cannot insert complete duplicate event');
      assert.throws(
        () => stmt.run([tableTennisEntity, homeTableKey, pongEvent, data, pingEventHomeId, uuid.v4(), pingEventHomeId]),
        /UNIQUE constraint failed: events\.eventId/,
        'cannot insert different event for same id');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, uuid.v4(), commandId1, thingEventId2]),
        /UNIQUE constraint failed: events\.commandId/,
        'cannot insert different event for same command');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, uuid.v4(), uuid.v4(), thingEventId1]),
        /UNIQUE constraint failed: events\.previousId/,
        'cannot insert different event for same previous');
      assert.end();
    });
  });

  setup.test('cannot delete or update', t => {
    t.test('cannot delete or update entity_events', assert => {
      assert.throws(
        () => db.exec(`DELETE FROM entity_events WHERE entity = '${tableTennisEntity}'`),
        /Cannot delete entity_events/,
        'cannot delete entity_events'
      );
      assert.throws(
        () => db.exec(`UPDATE entity_events SET entity = 'fail' WHERE entity = '${tableTennisEntity}'`),
        /Cannot update entity_events/,
        'cannot update entity_events'
      );
      assert.end();
    });

    t.test('cannot delete or update events', assert => {
      assert.throws(
        () => db.exec(`DELETE FROM events WHERE entity = '${thingEntity}'`),
        /Cannot delete events/,
        'cannot delete events'
      );
      assert.throws(
        () => db.exec(`UPDATE events SET entityKey = 'fail' WHERE entity = '${thingEntity}'`),
        /Cannot update events/,
        'cannot update events'
      );
      assert.end();
    });
  });

  test.onFinish(() => shutdownDb(db));
});
