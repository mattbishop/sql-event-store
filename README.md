# SQL Event Store
Demonstration of a SQL event store with deduplication and guaranteed event ordering. The database rules are intended to prevent incorrect information from entering into an event stream. You are assumed to have familiarity with [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html). Two DDLs are provided – one for [Postgres](https://www.postgresql.org), one for [SQLite](https://sqlite.org).

This project uses a node test suite to ensure the DDLs comply with the design requirements. The DDLs can also be ported to most SQL RDBMS and accessed from any number of writers, including high-load serverless functions, without a coordinating “single writer” process.

### Installing

Good news! Nothing to install! Instead, take the DDLs in this project ([Postgres](https://www.postgresql.org), [SQLite](https://sqlite.org)) and include them in your application’s database definition set.

## Running Tests

Running tests is not necessary but interesting to validate the correctness of the DDLs. One must have [Node](https://nodejs.org) installed (Node 22 is what I used) and then:

```bash
> npm install
```

Once it has finished installing the dependencies, run the test for the database you are interested in.

### SQLite Event Store

The [SQLite version](./sqlite-event-store.ddl) of SQL event store was built and tested with SQLite 3.49; it should run on recent versions of SQLite, at least since 2023.

```bash
> node test-sqlite.js
```

The SQLite test uses [sql.js](https://github.com/kripken/sql.js), the WASM build of SQLite for reliable compilation and test execution. The test will dump the test database to `sqlite-store.db` for your examination.

### Postgres Event Store

The [Postgres version](./postgres-event-store.ddl) of SQL event store has the same behavior as the SQLite version. It was built and tested on Postgres 17 but can be used in other versions.

The Postgres version can be tested with the [test-postgres.js]() script. Run this file instead of `test-sqlite.js`. It does not need a running Postgres server. Instead, it uses [pglite](https://pglite.dev), a WASM compilation of Postgres 17.

```bash
> node test-postgres.js
```

The script will dump the test ledger table to `postgres-store.tsv` for your inspection.

## Usage Model

Both SQLite and Postgres versions have similar SQL usage models but with one main difference. Postgres provides functions, whereas SQLite only has views. The concepts and naming are similar between the two databases, but slightly different in their use and capabilities.

### Appending Events

In order to manage the business rules of an event-sourced application, one must append events to the ledger.

#### SQLite

Append new events by inserting into the `append_event` view. Here is an example:

```sql
-- Add an event. Note the RETURNING clause, which returns the generated event_id for the appended event. This is used to append the next event.
INSERT INTO append_event (entity, entity_key, event, data, append_key) -- first event in entity, omit previous_id
VALUES ('game', 'apr-7-2025', 'game started','true', 'an-append-key')
RETURNING (SELECT event_id FROM ledger WHERE append_key = 'an-append-key');

-- now insert another event, using the first event's id as the previous_id value
INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
VALUES ('game', 'apr-7-2025', 'game going','true', 'another-append-key', '019612a6-38ac-7108-85fd-33e8081cedaf')
RETURNING (SELECT event_id FROM ledger WHERE append_key = 'another-append-key');
```

#### Postgres

Append new events by calling the `append_event` function. Here is an example:

```sql
-- Add an event. This function returns the generated event_id for the appended event.
SELECT append_event ('game', 'apr-7-2025', 'game started','true', 'an-append-key', null);

-- now insert another event, using the first event's id as the previous_id value
SELECT append_event ('game', 'apr-7-2025', 'game going','true', 'another-append-key', '019612a6-38ac-7108-85fd-33e8081cedaf');
```

### Replaying Events

One can replay events in order, without unhelpful data, by using the `replay_events` view.

#### SQLite / Postgres

```sql
-- Replay all the events
SELECT * FROM replay_events;

-- Replay events from a specific entity
SELECT * FROM replay_events
WHERE entity = 'game'
  AND entity_key = '2022 Classic';

-- Replay only certain events
SELECT * FROM replay_events
WHERE entity = 'game'
  AND entity_key = '2022 Classic'
  AND event IN ('game-started', 'game-finished');

-- BEWARE the last event_id in this result set may not be the last event for the entity instance, so it
-- cannot be used to append an event. To find the last event for an entity, use this query:

SELECT event_id FROM ledger
WHERE entity = 'game'
  AND entity_key = '2022 Classic'
ORDER BY sequence DESC LIMIT 1;
```

### Catching Up With New Events

Your application may want to “catch up” from a previously read event and avoid replaying already-seen events. SQLite and Postgres have different mechanisms to do so.

#### Catching Up With SQLite

```sql
-- Catch up with events after a known event
SELECT * FROM replay_events
WHERE entity = 'game' 
  AND entity_key = '2022 Classic'
  AND sequence > (SELECT sequence
                  FROM ledger
                  WHERE event_id = '123e4567-e89b-12d3-a456-426614174000');
```

The last `WHERE event_id` portion will contain the most recent event processed by your application, and the point in the events where you want to continue from.

#### Catching Up With Postgres

Replaying events to catch up after a previous event is a bit easier with Postgres since it has stored functions. The function `replay_events_after` accepts the event ID of the most recent event processed by your application. It returns the same fields as the `replay_events` view described above.

```sql
-- Catch up on new events from a specific entity, after a specific event
-- Postgres-only
SELECT * FROM replay_events_after('123e4567-e89b-12d3-a456-426614174000')
WHERE entity = 'game' 
  AND entity_key = '2922';
```

Notice how your application can add WHERE clauses in the replay query to filter for relevant events.

### Conceptual Model

An Event is an unalterable statement of fact that has occurred in the past. It has a name, like `food-eaten`, and it is scoped to an [Entity](https://en.wikiquote.org/wiki/Entity), or an identifiable existence in the world. Entities are individually identified by business-relevant keys that uniquely identify one entity from another.

In this event store, an event cannot exist without an entity to apply it to. Events can create new entities, and in that first event, the entity key is presented as the identifying key for the newly created entity.

Events follow other events in a sequence. Within an entity instance, each event has a reference to the previous event, much like a backward-linked list. This expression of previous event ID enables SQL Event Store to guarantee that events are written sequentially, without losing any concurrent appends of other events in for the same entity.

Appends to other entities do not affect each other, so many events can be appended to many events concurrently without suffering serialization penalties that “single writer” systems can cause.

### Design

- **Append-Only** Once events are created, they cannot be deleted, updated or otherwise modified. This includes entity event definitions.
- **Insertion-Ordered** Events must be consistently replayable in the order they were inserted.
- **Event race conditions are Impossible** The event store prevents a client from writing an event to an entity if another event has been inserted after the client has replayed an event stream.

### SQL Table Structure

#### `ledger` Table

| Column        | Notes                                                        |
| ------------- | ------------------------------------------------------------ |
| `entity`      | The entity name.                                             |
| `entity_key`  | The business identifier for the entity.                      |
| `event`       | The event name.                                              |
| `data`        | The event data. Cannot be `null` but can be an empty string. |
| `append_key`  | The append key from the client. Database rules ensure an append key can only be used once. Can be a Command ID, or another client-generated unique key for the event append action. Useful for idempotent appends. |
| `previous_id` | The event ID of the immediately-previous event for this entity. If this is the first event for an entity, then it’s value is `NULL`. |
| `event_id`    | The event ID. This value is used by the next event append as it's `previous_id` value to guard against a Lost Event problem. It can also be used to select subsequent events during replay. **AUTOPOPULATES—DO NOT INSERT.** |
| `timestamp`   | The timestamp the event was inserted into the ledger. **AUTOPOPULATES—DO NOT INSERT.** |
| `sequence`    | Overall ledger position for an event. **AUTOPOPULATES—DO NOT INSERT.** |

The `ledger` table is designed to allow multiple concurrent, uncoordinated writers to safely create events. It expects the client to know the difference between an entity's first event and subsequent events.

Multiple constraints are applied to this table to ensure bad events do not make their way into the system. This includes duplicated events and append keys, and ensured sequential events.

### Client Use Cases

Event store clients can use basic SQL statements to add and replay events. Clients follow the typical event sourcing pattern:

1. Receive a command
2. [Replay events](#replay-events) to compute current state
3. Validate entity state for command
4. [Append](#append-events) a new event 

#### Replay Events

Clients must always fetch, or replay, the events for an entity before inserting a new event. Replaying events assures the client that the state of the entity is known so that business rules can be applied for the command before an event is appended. For instance, if a command creates a new entity, the replay step will ensure no events have been appended to the entity's key.

```sql
SELECT event,
       data,
       event_id
FROM replay_events
    WHERE entity = ?
      AND entity_key = ?;
```

If a command produces a subsequent event for an existing entity, the `event_id` of the last event must be used as the `previous_id` of the next event. This design enforces the Event Sourcing pattern of building current read models before appending new events for an entity.

#### Append Events

Two types of events can be appended into the event log. The first event for an entity and subsequent events. The distinction is important for the database rules to control for incorrect events, like incorrect entity key or invalid previous event id.

##### First Event for an Entity

In this case, the `previous_id` does not exist, so it is omitted from the insert statement.

```sql
-- SQLite version, see above for Postgres
INSERT INTO append_event(entity,
                         entity_key,
                         event,
                         data,
                         append_key) 
VALUES (?, ?, ?, ?, ?);
```

##### Subsequent Events

The `previous_id` is the `event_id` of the last event recorded for the specific entity.

```sql
-- SQLite version, see above for Postgres
INSERT INTO append_event(entity,
                         entity_key,
                         event,
                         data,
                         append_key,
                         previous_id) 
VALUES (?, ?, ?, ?, ?, ?);
```

If another event in this entity has been appended using this previous_id, the database will reject the insert and require your application to replay newer events to verify the entity state. Also, if the entity instance has newer events than previous_id, the append will be rejected. [Catch up](#catching-up-with-new-events) with the newest events and run the append again if appropriate.
