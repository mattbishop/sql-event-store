# SQL Event Store
Demonstration of a SQL event store with deduplication and guaranteed event ordering. This event store can be ported to most SQL RDBMS and accessed from an number of writers, including high-load serverless functions.

This project assumes familiarity with [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html).

### Conceptual Model

Event sourcing has some variations in practice. This event store works from a few opinions and terms that are easy to understand and map to other variations.

An Event is an unalterable statement of fact, an event that has occurred in the past. It has a name, like `food-eaten`, and it is scoped to an [Entity](https://en.wikiquote.org/wiki/Entity), or an identifiable existence in the world. Entities are individually identified by business-relevant keys that uniquely identify one entity from another.

In this event store, an event cannot exist without an entity to apply it to. Events can create entities, and in that event the entity key is presented as the identifying key for newly-created entity.

Events follow other events in a sequence. In this event store, each event has a reference to the previous event, much like a backward-linked list. This expression of previous event ID is what enables sql event store to guarantee that events are written sequentially, without losing a concurrent insertion of another event.

### Design

- Append-only. Once events are created, they cannot be deleted, updated or otherwise modified.
- Insertion-ordered. Events must be consistently replayable in the order they were inserted.
- No event race conditions. The event store prevents a client from writing an event to an entity if another event has been inserted after the client has replayed an event stream.
- Event and Entity names cannot be mispelled or misapplied. A client cannot insert an event from the wrong entity. Event and entity names must be defined before use.

### SQL Table Structure



### Client Use

Insert Entity Events

Insert Events

First Event for an Entity

Subsequent Events

Replaying Events

For an Entity

For multiple Entities