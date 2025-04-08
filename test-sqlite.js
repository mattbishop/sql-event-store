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
    const stmt = db.prepare('INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id) VALUES (?, ?, ?, ?, ?, ?);');

    const thingKey = '1';
    const homeTableKey = 'home';
    const workTableKey = 'work';

    const appendKey1 = uuid.v4();
    const appendKey2 = uuid.v4();
    let thingEventId1 = uuid.v4();
    let thingEventId2 = uuid.v4();

    let pingEventHomeId = uuid.v4();
    let pingEventWorkId = uuid.v4();


    const data = '{}';

    t.test('cannot insert empty columns', assert => {
      assert.throws(
        () => stmt.run([null, thingKey, thingCreatedEvent, data, appendKey1]),
        /NOT NULL constraint failed: events\.entity/,
        'cannot insert null entity');
      assert.throws(
        () => stmt.run([thingEntity, null, thingCreatedEvent, data, appendKey1]),
        /NOT NULL constraint failed: events\.entity_key/,
        'cannot insert null entity key');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, null, data, appendKey1]),
        /NOT NULL constraint failed: events\.event/,
        'cannot insert null event');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, null, appendKey1]),
        /NOT NULL constraint failed: events\.data/,
        'cannot insert null event data');

      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, null]),
        /NOT NULL constraint failed: events\.append_key/,
        'cannot insert null append key');
      assert.end();
    });

    t.test('Cannot insert event from wrong entity', assert => {
      assert.throws(
        () => stmt.run([tableTennisEntity, thingKey, thingCreatedEvent, data, appendKey1]),
        /FOREIGN KEY constraint failed/,
        'cannot insert event in wrong entity');
      assert.end();
    });

    const appendStmt = db.prepare(`
INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING (SELECT event_id FROM events WHERE append_key = $5) as event_id;`);

    t.test('insert events for an entity', assert => {
      // [thingEventId1] = appendStmt.get([thingEntity, thingKey, thingCreatedEvent, data, uuid.v4(), null]);
      assert.doesNotThrow(() => [thingEventId1] = appendStmt.get([thingEntity, thingKey, thingCreatedEvent, data, appendKey1, null]));
      assert.doesNotThrow(() => [thingEventId2] = appendStmt.get([thingEntity, thingKey, thingDeletedEvent, data, appendKey2, thingEventId1]));
      assert.doesNotThrow(() => [pingEventHomeId] = appendStmt.get([tableTennisEntity, homeTableKey, pingEvent, data, uuid.v4(), null]));
      assert.doesNotThrow(() => [pingEventWorkId] = appendStmt.get([tableTennisEntity, workTableKey, pingEvent, data, uuid.v4(), null]));
      assert.end();
    });

    t.test('previous_id rules', assert => {
      assert.throws(
        () => stmt.run([tableTennisEntity, homeTableKey, pingEvent, data, uuid.v4(), null]),
        /previous_id can only be null for first entity event/,
        'cannot insert multiple null previous ID for an entity');
      assert.throws(
        () => stmt.run([tableTennisEntity, workTableKey, pongEvent, data, uuid.v4(), pingEventHomeId]),
        /previous_id must be in same entity/,
        'previous ID must be in same entity');
      assert.end();
    });

    t.test('Cannot insert duplicates', assert => {
      assert.throws( // ?? this test seems odd
        () => stmt.run([thingEntity, thingKey, thingCreatedEvent, data, appendKey2, thingEventId1]),
        /UNIQUE constraint failed/,
        'cannot insert complete duplicate event');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, appendKey1, thingEventId2]),
        /UNIQUE constraint failed: events\.append_key/,
        'cannot insert different event for same append key');
      assert.throws(
        () => stmt.run([thingEntity, thingKey, thingDeletedEvent, data, uuid.v4(), thingEventId1]),
        /UNIQUE constraint failed: events\.previous_id/,
        'cannot insert different event for same previous ID');
      assert.end();
    });
  });

  setup.test('cannot delete or update', t => {
    t.test('cannot delete or update events', assert => {
      assert.throws(
        () => db.exec(`DELETE FROM events WHERE entity = '${thingEntity}'`),
        /Cannot delete events/,
        'cannot delete events'
      );
      assert.throws(
        () => db.exec(`UPDATE events SET entity_key = 'fail' WHERE entity = '${thingEntity}'`),
        /Cannot update events/,
        'cannot update events'
      );
      assert.end();
    });
  });

  test.onFinish(() => shutdownDb(db));
});
