# Database design

The database is made up of a set of key ranges / collections. Each collection:

- Has the same URL prefix (eg `users/`)
- Conforms to a *type*
- Shares some configuration parameters:
    - Conflict type (LWW, store-all, CRDT, abort)
    - Read / Write permissions (?)
    - Transient flag (? For session data)
    - Owned flag (? Data has a single writer)
- (Not implemented): Has a unique prefix in the database
- Transient flag (indicates this data is not persisted)
- Type:
    - Data (JSON with typescriptish named fields)
    - Index
    - View
    - Blob with mime-type (Content-addressable? ?)


Keys starting with `_` are reserved for internal use.

Each collection's metadata is defined by a key in `_schema/[name]`.


# Versioning and naming

The database is keyed and versioned using actor IDs.

- Each agent in the network assigns itself a random globally unique ID when it writes for the first time
- Each database node maintains a mapping from agent ID (hash) -> local id (number). The numbers are local only - they are never sent over the network.
- An HTTP client can either create and use its own agent ID or rely on the agent ID of the HTTP server its connecting through.
- Each write from each an agent has a unique (agent, sequence) pair. (The sequence number goes up by 1 with each write by that agent).
- Whenever unique IDs are needed, the default ID is based on the (agent, sequence) pair since that is known to be globally unique.
- Each write names the known version frontier when the write was made. This implicitly defines a DAG of writes.
    - Given two versions A and B, either A dominates B (`A>B`), B dominates A (`A<B`) or A and B are concurrent. Ie, versions are a [strict partial order](https://en.wikipedia.org/wiki/Partially_ordered_set#Strict_and_non-strict_partial_orders).
    - Every version implicitly dominates all previous sequence numbers from the same agent. Ie, ∀ *a*: agent, *x*, *y*: (*a*, *x*) < (*a*, *y*) ⇔ *x* < *y*.

Each document stores its last modified version. Whenever a write happens, each document's version is compared to the version named by the write. If the write version dominates the document's version, the value is overwritten. Otherwise behaviour follows the conflict type named by the schema:

- **Last writer wins**: A winner is chosen in an arbitrary, consistent way. The other value is deleted.
- **Store all**: All conflicting values are stored. The next read will return multiple versions of the data.
- **CRDT**: We use a CRDT like y.js or something to store the value. Changes are represented as CRDT edits and merged following the logic of the embedded crdt.
- **Abort**: This is only applicable for data that is owned by the local node. (Ie, data that cannot be edited by other peers). In this case, the semantics follow traditional transactional database semantics and conflicting concurrent edits are aborted and should be retried by the client.

The version is *global* for the whole database. (As opposed to local to each document).


# Realtime updates

Everything in the database that can be queried can also be subscribed to, such that a client gets a live stream of changes to the resulting document set.


# Links

One of the builtin types for document fields is a link. Like URLs, links connect to other documents in the database, (or documents hosted by other databases?).

The query system will work like graphql. Queries can auto-follow named links to fetch related data.



