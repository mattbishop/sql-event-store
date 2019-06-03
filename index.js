/*
This test suite exercises the database schema with incorrect data, duplicate data and other likely real-world problems.
 */

/*
  BEFORE triggers:
  TODO: first event for an entity key must 'previousId = null'
  TODO: previousId must be in same entity key
 */

const test = require('tape');
const fs = require('fs');
const initSqlJs = require('sql.js');
const uuid = require('uuid/v4');

const thingEntity = 'thing';
const thingCreatedEvent = 'thing-created';
const thingDeletedEvent = 'thing-deleted';
const tableTennisEntity = 'table-tennis';
const pingEvent = 'ball-pinged';
const pongEvent = 'ball-ponged';

async function initDb() {
  const SQL = await initSqlJs();
  const db = new SQL.Database();
  const createScript = fs.readFileSync('./event-store.ddl', 'utf8');
  db.run(createScript);
  return db;
}

function shutdownDb(db) {
  const data = db.export();
  const buffer = Buffer.from(data);
  fs.writeFileSync('test-db.sqlite', buffer);
  db.close();
}

// use t.plan() for async testing too.
test('setup', async setup => {
  const db = await initDb();

  setup.test('insert entity_events', t => {

    const stmt = db.prepare('INSERT INTO entity_events (entity, event) values (?, ?)');

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

    t.test('cannot insert duplicate entity events', assert => {
      assert.throws(
        () => stmt.run([thingEntity, thingCreatedEvent]),
        /UNIQUE constraint failed: entity_events\.entity, entity_events\.event/,
        'cannot duplicate entity events');
      assert.end();
    });
  });

  setup.test('insert events', t => {
    const stmt = db.prepare('INSERT INTO events(entity, entityKey, event, data, eventId, commandId, previousId) VALUES (?, ?, ?, ?, ?, ?, ?)');
    const thingKey = '1';
    const commandId1 = uuid();
    const commandId2 = uuid();
    const eventId1 = uuid();
    const eventId2 = uuid();
    const data = '{}';

    t.test('cannot insert empty columns', assert => {
      assert.throws(
        () => stmt.run([null, thingKey, thingCreatedEvent, data, eventId1, commandId1]),
        /NOT NULL constraint failed: events\.entity/,
        'cannot insert null entity');
      assert.throws(
        () => stmt.run([thingEntity, null, thingCreatedEvent, data, eventId1, commandId1]),
        /NOT NULL constraint failed: events\.entityKey/,
        'cannot insert null entity key');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, null, data, eventId1, commandId1]),
        /NOT NULL constraint failed: events\.event/,
        'cannot insert null event');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, null, eventId1, commandId1]),
        /NOT NULL constraint failed: events\.data/,
        'cannot insert null event data');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, null, commandId1]),
        /NOT NULL constraint failed: events\.eventId/,
        'cannot insert null event id');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, eventId1, null]),
        /NOT NULL constraint failed: events\.commandId/,
        'cannot insert null command');
      assert.end();
    });

    t.test('Cannot insert event from wrong entity', assert => {
      assert.throws(
        () => stmt.run([tableTennisEntity, thingKey, thingCreatedEvent, data, eventId1, commandId1]),
        /FOREIGN KEY constraint failed/,
        'cannot insert event in wrong entity');
      assert.end();
    });

    t.test('insert events for an entity', assert => {
      assert.doesNotThrow(() => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, eventId1, commandId1]));
      assert.doesNotThrow(() => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, eventId2, commandId2, eventId1]));
      assert.end();
    });

    t.test('previous event must exist', assert => {
      assert.throws(() => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, uuid(), uuid(), uuid()]),
        /FOREIGN KEY constraint failed/,
        'previous event id must exist');
      assert.end();
    });

    t.test('Cannot insert duplicates', assert => {
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, eventId1, commandId1]),
        /UNIQUE constraint failed/,
        'cannot insert complete duplicate first event');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, eventId2, commandId2, eventId1]),
        /UNIQUE constraint failed/,
        'cannot insert complete duplicate event');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, eventId1, uuid(), uuid()]),
        /UNIQUE constraint failed: events\.eventId/,
        'cannot insert different event for same id');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, uuid(), commandId1, uuid()]),
        /UNIQUE constraint failed: events\.commandId/,
        'cannot insert different event for same command');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, uuid(), uuid(), eventId1]),
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
