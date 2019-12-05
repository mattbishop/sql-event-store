# SQL Event Store
Demonstration of a SQL event store with deduplication and guaranteed event ordering. The database rules are intended to prevent incorrect information from entering into an event stream. You are assumed to have familiarity with [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html).

This project uses a node test suite and SQLite to ensure the DDL complies with the design requirements. This SQLite event store can be used in highly-constrained environments that require an embedded event store, like a mobile device or an IoT system. 

This event store can also be ported to most SQL RDBMS and accessed from any number of writers, including high-load serverless functions, without a coordinating “single writer” process. The project includes a Postgres version of the DDL.

# Postgres Event Store

The [Postgres version](./postgres-event-store.ddl) of SQL event store has the same behavior as the SQLite version. It was built and tested on Postgres 11 but can be ported to earlier versions as needed.

The postgres version can be tested with the [test-postgres.js]() script. Run this file instead of `index.js`. It will connect to the postgres server defined in the environment variables, according to [node-postgres](https://node-postgres.com/features/connecting). 

### Running

One must have Node and NPM installed (Node 10 is what I used) and then:

```bash
> npm install
```

Once it has finished installing the dependencies, run the [tests](./index.js) with:

```bash
> node index.js
```

The test uses [sql.js](https://github.com/kripken/sql.js), the pure Javascript port of SQLite for reliable compilation and test execution. The test will dump the test database to `test-event-store.sqlite` for your examination.

### Conceptual Model

An Event is an unalterable statement of fact that has occurred in the past. It has a name, like `food-eaten`, and it is scoped to an [Entity](https://en.wikiquote.org/wiki/Entity), or an identifiable existence in the world. Entities are individually identified by business-relevant keys that uniquely identify one entity from another.

In this event store, an event cannot exist without an entity to apply it to. Events can create new entities, and in that first event, the entity key is presented as the identifying key for newly-created entity.

Events follow other events in a sequence. In this event store, each event has a reference to the previous event, much like a backward-linked list. This expression of previous event ID enables SQL Event Store to guarantee that events are written sequentially, without losing any concurrent appends of other events in for the same entity.

Appends to other entities do not affect each other, so many events can be appended to many events concurrently without suffering serialization penalties that “single writer” systems can cause.

### Design

- **Append-Only** Once events are created, they cannot be deleted, updated or otherwise modified. This includes entity event definitions.
- **Insertion-Ordered** Events must be consistently replayable in the order they were inserted.
- **Event race conditions are Impossible** The event store prevents a client from writing an event to an entity if another event has been inserted after the client has replayed an event stream.
- **Entity and Event Validation** Event and Entity names cannot be mispelled or misapplied. A client cannot insert an event from the wrong entity. Event and entity names must be defined before use.

### SQL Table Structure

This event store consists of two tables [as described in the DDL](./sqlite-event-store.ddl). The first, `entity_events`, contains the event definitions for an entity type. It must be populated before events can be appended to the main table called `events`.

#### `entity_events` Table

| Column   | Notes                 |
| -------- | --------------------- |
| `entity` | The entity name.      |
| `event`  | An entity event name. |

The `entity_events` table controls the entity and event names that can be used in the `events` table itself therough the use of composite foreign keys.

#### `events` Table

| Column       | Notes                                                        |
| ------------ | ------------------------------------------------------------ |
| `entity`     | The entity name. Part of a composite foreign key to `entity_events`. |
| `entityKey`  | The business identifier for the entity.                      |
| `event`      | The event name. Part of a composite foreign key to `entity_events`. |
| `data`       | The event data. Cannot be `null` but can be an empty string. |
| `eventId`    | The event ID. This value is used by the next event as it's `previousId` value to guard against a Lost Event problem. |
| `commandId`  | The command ID causing this event. Database rules ensure a Command ID can only be used once. |
| `previousId` | The event ID of the immediately-previous event for this entity. If this is the first event for an entity, then omit or send `NULL`. |
| `ts`         | The timestamp of the event insertion. **AUTOPOPULATES—DO NOT INSERT.** |
| `sequence`   | Auto-incrementing sequence number. Used to sort the events for replaying in the insertion order. **AUTOPOPULATES—DO NOT INSERT.** |

The `events` table is designed to allow multiple concurrent, uncoordinated writers to safely create events. It expects the client to know the difference between an entity's first event and subsequent events.

Multiple constraints are applied to this table to ensure bad events do not make their way into the system. This includes duplicated events and commands, incorrect naming and ensured sequential events.

### Client Use Cases

Event store clients can use basic SQL statements to add and replay events. These use cases start with the first—[Insert Entity Event Definitions](#insert-entity-event-definitions)—to set up the database for individual event instances. Thereafter clients follow the typical event sourcing pattern:

1. Receive a command
2. [Replay events](#replay-events) to compute current state
3. Validate entity state for command
4. [Append](#append-events) a new event 

#### Insert Entity Event Definitions

The `entity_events` table is a definition table that contains all the available events for a given entity type. A system will need to populate this table with the entity events expected before attempting to append events.

```sql
INSERT INTO entity_events (entity, 
                           event) 
                           VALUES (?, ?);
```

Use this statement for every event in an entity and for every entity type in the system.

#### Replay Events

Clients must always fetch, or replay, the events for an entity before inserting a new event. Replaying events assures the client that the state of the entity is known so that business rules can be applied for the command before an event is appended. For instance, if a command creates a new entity, the replay step will ensure no events have been appended to the entity's key.

```sql
SELECT event, 
       data, 
       eventId 
FROM events 
    WHERE entityKey = ? 
      AND entity = ? 
ORDER BY sequence;
```

If a command produces a subsequent event for an existing entity, the `eventId` of the last event must be used as the `previousId` of the next event.

#### Append Events

Two types of events can be appended into the event log. The first event for an entity and subsequent events. The distinction is important for the database rules to control for incorrect events, like incorrect entity key or invalid previous event id.

##### First Event for an Entity

In this case, the `previousId` does not exist, so it is omitted from the insert statement.

```sql
INSERT INTO events(entity, 
                   entityKey, 
                   event, 
                   data, 
                   eventId, 
                   commandId) 
                   VALUES (?, ?, ?, ?, ?, ?);
```

##### Subsequent Events

The `previousId` is the `eventId` of the last event recorded for the entity.

```sql
INSERT INTO events(entity, 
                   entityKey, 
                   event, 
                   data, 
                   eventId, 
                   commandId, 
                   previousId) 
                   VALUES (?, ?, ?, ?, ?, ?, ?);
```

