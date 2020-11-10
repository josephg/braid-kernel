# Braid kernel prototype

This is a simple prototype / experiment in database design.

The goal is to have a small, simple database with the following properties:

- Simple key/value pairs for records
- With types defined at the 'directory' level - eg `{"users/*": [User]}`
- Simple REST API and GraphQL-like API
- Document links
- Realtime updates & subscriptions
- Indexes
- Presence (transient per-client information)
- Git-like multi-master sync between nodes
- Conflict resolution options:
  - Last-writer wins
  - Keep conflicts / manual merging
  - CRDT (Yjs)
  - Owned data with custom action based transaction model
- Branches
- Computed views (store javsacript code to turn a document into HTML)
- Good looking dev tools
- Capability based security model

In its current form it will not:

- Work with anything other than a local lmdb, in a single process

