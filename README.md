# SQL Event Store
Demonstration of a SQ event store with deduplication and guaranteed event ordering. The database rules are intended to prevent incorrect information from entering into an event stream. You are assumed to have familiarity with [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html).

This project uses SQLite and a node test suite to ensure the DDL complies with the design requirements. It can be ported to most SQL RDBMS and accessed from an number of writers, including high-load serverless functions.

### Running

One must have Node and NPM installed (Node 10+ should do fine) and then:

```bash
> npm install
```

Once it has finished installing the dependencies, run the test with:

```bash
> node index.js
```

This project uses [sql.js](https://github.com/kripken/sql.js), the pure Javascript port of SQLite to save you from compilation difficulties.

### Conceptual Model

Event sourcing has some variations in practice. This event store works from a few opinions and terms that are easy to understand and map to other variations.

An Event is an unalterable statement of fact, an event that has occurred in the past. It has a name, like `food-eaten`, and it is scoped to an [Entity](https://en.wikiquote.org/wiki/Entity), or an identifiable existence in the world. Entities are individually identified by business-relevant keys that uniquely identify one entity from another.

In this event store, an event cannot exist without an entity to apply it to. Events can create entities, and in that first event, the entity key is presented as the identifying key for newly-created entity.

Events follow other events in a sequence. In this event store, each event has a reference to the previous event, much like a backward-linked list. This expression of previous event ID enables sql event store to guarantee that events are written sequentially, without losing a concurrent insertion of another event.

### Design

- Append-only. Once events are created, they cannot be deleted, updated or otherwise modified. This includes entity event definitions.
- Insertion-ordered. Events must be consistently replayable in the order they were inserted.
- No event race conditions. The event store prevents a client from writing an event to an entity if another event has been inserted after the client has replayed an event stream.
- Event and Entity names cannot be mispelled or misapplied. A client cannot insert an event from the wrong entity. Event and entity names must be defined before use.

### SQL Table Structure

This event store consists of two tables. The first, `entity_events`, contains the event definitions for an entity type. It must be populated before events can be appended to the main table called `events`.

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
| `ts`         | The timestamp of the event insertion. **DO NOT INSERT, AUTOPOPULATES.** |
| `sequence`   | Auto-incrementing sequence number. Used to sort the events for replaying in the insertion order. **DO NOT INSERT, AUTOPOPULATES.** |

The `events` table is designed to allow multiple concurrent, uncoordinated writers to safely create events. It expects the client to know the difference between an entity's first event and subsequent events.

Multiple constraints are applied to this table to ensure bad events do not make it's way into the system. This includes duplicated events and commands, incorrect naming and ensured sequential events.

### Client Use Cases

Event store clients can use basic SQL statements to add and replay events. These use cases start with the first—Insert Entity Events—to set up the database for individual event instances.

#### Insert Entity Event Definitions

The `entity_events` table is a definition table that contains all the available events for a given entity type. A system will need to populate this table with the entity events expected before attempting to append events.

```sql
INSERT INTO entity_events (entity, 
                           event) 
                           VALUES (?, ?);
```

Use this statement for every event in an entity and for every entity type in the system.

#### Replaying Events

Clients must always fetch, or replay, the events for an entity before inserting a new event. Replaying events assures the client that the state of the entity is known so that business rules can be applied for the command before an event is appended. For instance, if a command creates a new entity, the replay step will ensure no events have been appended to the entity's key.

```sql
SELECT event, 
       data, 
       eventId 
FROM events 
    WHERE entity = ? 
      AND entityKey = ? 
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

