/*
This test suite exercises the database with specific, interleaved calls to simulate multi-client race conditions.
It also tests the schema with incorrect data, duplicate data and other likely real-world problems.
 */

/*
 TODO how can I check if "previous" exists? Maybe:

 WHERE parent in (SELECT id FROM events WHERE id = parent)
 may be able to link previous to an id as an FK
 https://github.com/kripken/sql.js/issues/221

 I can't do this in CHECK, I have to write a BEFORE trigger to test the data

TODO: block insertion of ts, sequence
TODO: disable DELETE and UPDATE
todo: first event for an entity key must 'previous = null'
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
    const stmt = db.prepare('INSERT INTO events(entity, event, key, id, data, command, previous) VALUES (?, ?, ?, ?, ?, ?, ?)');
    const thing1 = '1';
    const command1 = uuid();
    const command2 = uuid();
    const event1 = uuid();
    const event2 = uuid();
    const data = '{}';

    t.test('cannot insert empty columns', assert => {
      assert.throws(
        () => stmt.run([null, thingCreatedEvent, thing1, event1, data, command1]),
        /NOT NULL constraint failed: events\.entity/,
        'cannot insert null entity');
      assert.throws(
        () => stmt.run([thingEntity, null, thing1, event1, data, command1]),
        /NOT NULL constraint failed: events\.event/,
        'cannot insert null event');
      assert.throws(
        () => stmt.run([thingEntity, thingCreatedEvent, null, event1, data, command1]),
        /NOT NULL constraint failed: events\.key/,
        'cannot insert null entity key');
      assert.throws(
        () => stmt.run([thingEntity, thingCreatedEvent, thing1, null, data, command1]),
        /NOT NULL constraint failed: events\.id/,
        'cannot insert null event id');
      assert.throws(
        () => stmt.run([thingEntity, thingCreatedEvent, thing1, event1, null, command1]),
        /NOT NULL constraint failed: events\.data/,
        'cannot insert null event data');
      assert.throws(
        () => stmt.run([thingEntity, thingCreatedEvent, thing1, event1, data, null]),
        /NOT NULL constraint failed: events\.command/,
        'cannot insert null command');
      assert.end();
    });

    t.test('Cannot insert event from wrong entity', assert => {
      assert.throws(
        () => stmt.run([tableTennisEntity, thingCreatedEvent, thing1, event1, data, command1]),
        /FOREIGN KEY constraint failed/,
        'cannot insert event in wrong entity');
      assert.end();
    });

    t.test('insert events for an entity', assert => {
      assert.doesNotThrow(() => stmt.run([thingEntity, thingCreatedEvent, thing1, event1, data, command1]));
      assert.doesNotThrow(() => stmt.run([thingEntity, thingCreatedEvent, thing1, event2, data, command2, event1]));
      assert.end();
    });

    t.test('Cannot insert duplicates', assert => {
      assert.throws(
        () => stmt.run([thingEntity, thingCreatedEvent, thing1, event1, data, command1]),
        /UNIQUE constraint failed/,
        'cannot insert complete duplicate first event');
      assert.throws(
        () => stmt.run([thingEntity, thingCreatedEvent, thing1, event2, data, command2, event1]),
        /UNIQUE constraint failed/,
        'cannot insert complete duplicate event');
      assert.throws(
        () => stmt.run([thingEntity, thingDeletedEvent, thing1, event1, data, uuid(), uuid()]),
        /UNIQUE constraint failed: events\.id/,
        'cannot insert different event for same id');
      assert.throws(
        () => stmt.run([thingEntity, thingDeletedEvent, thing1, uuid(), data, command1, uuid()]),
        /UNIQUE constraint failed: events\.command/,
        'cannot insert different event for same command');
      assert.throws(
        () => stmt.run([thingEntity, thingDeletedEvent, thing1, uuid(), data, uuid(), event1]),
        /UNIQUE constraint failed: events\.previous/,
        'cannot insert different event for same previous');
      assert.end();
    });
  });

  test.onFinish(() => shutdownDb(db));
});
