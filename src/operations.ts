/**
 * This is a simple example program showing a working in-memory database storing
 * full history and branches. In this demo the "database" is a Map from string
 * keys to values.
 *
 * Everything is versioned - you can wind time forward and backwards, and mark a
 * branch and teleport the database back to that point in time.
 *
 * Conflicts are handled similarly to Basho's riak. When concurrent writes
 * happen, the database stores every concurrent version of the document. A
 * subsequent read will fetch all values, and a subsequent write can supercede
 * all existing values to "solve" the conflict.
 *
 * For the most part I'm using a hybrid model between lamport timestamp version
 * vectors and using implied versions via transitive dependancies. The idea is
 * to allow arbitrary peers to write data (not limited to a "server pool" of
 * writers). But still avoid sibling explosions by rarely actually naming the
 * fully expanded version.
 *
 * At the bottom of the file is a fuzz test which creates 3 database peers then
 * generates random operations to apply to them. Then random pairs of peers sync
 * changes with one another. The whole database must stay consistent. The sync
 * algorithm currently isn't written to work over a network - a proper protocol
 * should also manage round-trips and whatnot, and ideally not take size linear
 * with the number of agent IDs.
 *
 * The fuzz test is slower than I'd like because of deep cloning in the test and
 * excessive comparisons (which grow linearly over time). A real database node
 * should be much quicker.
 */

import assert from 'assert'
import { Console } from "console"
import seedrandom from 'seedrandom'
import Map2 from 'map2'
import PriorityQueue from 'priorityqueuejs'

// A simple version interface. Externally you'd want to use random uuids for
// agent identifiers.
interface RawVersion {
  agent: string,
  seq: number
}

const ROOT_VERSION: RawVersion = {
  agent: 'ROOT', seq: 0
}

const console = new Console({
  stdout: process.stdout,
  stderr: process.stderr,
  inspectOptions: {
    depth: null
  }
})

const CHECK = process.env['NOCHECK'] ? false : true
if (CHECK) console.log('running in checked mode')

// An operation affects multiple documents 
interface RawOperation {
  version: RawVersion, // These could be RawVersions.
  succeedsSeq: number | null,
  parents: RawVersion[], // Only direct, non-transitive dependancies. May or may not include own agent's parent.

  docOps: DocOperation[],
}

interface LocalOperation {
  version: RawVersion,

  // This is filled in when an operation is added to the store. It is a local
  // order defined such that if a > b, a.localOrder > b.localOrder. If two
  // operations are concurrent they could end up in any order.
  order: number,
  succeedsOrder: number, // Or -1.
  parents: number[] // localOrder of parents. Semantics same as RawOperation.parents.
  docOps: DocOperation[],

  // For network sync. TODO: Remove this.
  raw: RawOperation,
}

interface DocOperation {
  key: string,
  // collection: string,

  // TODO: Change parents to a local order too.
  parents: RawVersion[], // Specific to the document. The named operations edit this doc.
  newValue: any,
}

type DocValue = { // Has multiple entries iff version in conflict.
  version: RawVersion,
  value: any
}[]

type OperationSet = Map2<string, number, number>

interface DBState {
  data: Map<string, DocValue>

  // Version information is all relative / related to the current branch.
  version: Map<string, number> // Expanded for all agents.
  // The number of times each (agent, seq) is referenced by an operations
  refs: Map2<string, number, number>
  // This contains every entry in version where foreignRefs is 0 (or equivalently undefined).
  versionFrontier: Set<string> // set of agent ids

  // This stuff is not compared when we compare databases. This is not relative
  // to the current branch - a database will store all seen operations here.
  operations: LocalOperation[], // order => operation.
  versionToOrder: OperationSet, // agent,seq => order.
}

const assertDbEq = (a: DBState, b: DBState) => {
  if (!CHECK) return

  assert.deepStrictEqual(a.data, b.data)
  assert.deepStrictEqual(a.version, b.version)
  assert.deepStrictEqual(a.refs, b.refs)
  assert.deepStrictEqual(a.versionFrontier, b.versionFrontier)
}

// There are 4 cases:
// - A dominates B (return +ive)
// - B dominates A (return -ive)
// - A and B are equal (not checked here)
// - A and B are concurrent (return 0)
const versionToOrder = (allOps: OperationSet, agent: string, seq: number): number => (
  agent === ROOT_VERSION.agent && seq === 0 ? -1 : allOps.get(agent, seq)!
)

const orderToVersion = (db: DBState, order: number): RawVersion => (
  order === -1 ? ROOT_VERSION : db.operations[order].version
)

// This is used to break ties between versions.
const vCmp = (a: RawVersion, b: RawVersion) => (
  a.agent.localeCompare(b.agent, 'en-US') || (a.seq - b.seq)
)

/**
 * This method calculates whether two versions are concurrent, if a dominates b
 * or if b dominates a.
 *
 * Returns +ive if a dominates b, -ive if b dominates a, or 0 if either the
 * operations are the same or the operations are concurrent. It is the
 * responsibility of the caller to differentiate these cases. (Via vCmp(a,b) === 0).
 */
const compareVersions = (db: DBState, a: RawVersion, b: RawVersion): number => {
  if (a.agent === b.agent) return a.seq - b.seq

  // This works is via a DFS from the operation with a higher localOrder looking
  // for the localOrder of the smaller operation.

  const aOrder = versionToOrder(db.versionToOrder, a.agent, a.seq)
  const bOrder = versionToOrder(db.versionToOrder, b.agent, b.seq)
  assert(aOrder !== bOrder) // Should have returned above in this case.

  const [start, target] = aOrder > bOrder ? [aOrder, bOrder] : [bOrder, aOrder]

  const visited = new Set<number>() // Set of localOrders.

  let found = false

  const visit = (order: number) => {
    // iter2++
    // console.log('visiting', agent, seq)
    
    if (order === target) {
      found = true
      return
    } else if (order < target) { return }
    
    const op = db.operations[order]
    assert(op != null)

    if (visited.has(order)) return
    visited.add(order)

    assert(op) // If we hit the root operation, we should have already returned.

    if (op.succeedsOrder > -1) visit(op.succeedsOrder)

    for (const p of op.parents) {
      if (found) return
      visit(p)
    }
  }

  visit(start)

  // Its impossible for the operation with a smaller localOrder to dominate the
  // op with a larger localOrder.
  if (found) return aOrder - bOrder
  else return 0
}

/**
 * This method takes in two versions (expressed as a fronteir in order numbers).
 * And it returns the set of operations only appearing in the history of one
 * version or the other.
 *
 * There are some helper methods below for getting the diff of two versions
 * expressed using RawVersions (diffv) and methods for converting a
 * db.versionFrontier into a compatible list of orders.
 */
const diff = (db: DBState, a: number[], b: number[]) => {
  // console.log('calculating difference between', a, b)
  // console.log('=', a.map(order => getLocalVersion(db, order)), b.map(order => getLocalVersion(db, order)))

  // There's a bunch of ways to implement this. I'm not sure this is the best.
  // In essence, we track 2 data structures:
  //
  // 1. A priority queue of order numbers. When we pop, we get the highest order
  //    first.
  // 2. A tag for each order in the priority queue naming the type. This is held
  //    separate from the queue so we can tell when the same order is added to
  //    the priority queue twice (and at insertion time). The tag marks the
  //    entry as only in A's history, only in B's history or in the history of
  //    both items.
  //
  // We expand the priority queue until the only entries left are in the shared
  // history of both A and B.

  const enum ItemType { Shared, A, B }

  const itemType = new Map<number, ItemType>()

  // Every order is in here at most once. Every entry in the queue is also in
  // itemType.
  const queue = new PriorityQueue<number>()
  
  // Number of items in the queue in both transitive histories (state Shared).
  let numShared = 0

  const enq = (order: number, type: ItemType) => {
    const currentType = itemType.get(order)
    if (currentType == null) {
      queue.enq(order)
      itemType.set(order, type)
      // console.log('+++ ', order, type, getLocalVersion(db, order))
      if (type === ItemType.Shared) numShared++
    } else if (type !== currentType && currentType !== ItemType.Shared) {
      // This is sneaky. If the two types are different they have to be {A,B},
      // {A,Shared} or {B,Shared}. In any of those cases the final result is
      // Shared. If the current type isn't shared, set it as such.
      itemType.set(order, ItemType.Shared)
      numShared++
    }
  }

  for (const order of a) enq(order, ItemType.A)
  for (const order of b) enq(order, ItemType.B)

  const aOnly: number[] = [], bOnly: number[] = []

  // Loop until everything is shared.
  while (queue.size() > numShared) {
    const order = queue.deq()
    const type = itemType.get(order)!
    // It should be safe to remove the item from itemType here.

    // console.log('--- ', order, 'of type', type, getLocalVersion(db, order), 'shared', numShared, 'num', queue.size())
    assert(type != null)

    if (type === ItemType.Shared) numShared--
    else (type === ItemType.A ? aOnly : bOnly).push(order)

    if (order < 0) continue // Bottom out at the root operation.
    const op = db.operations[order]
    // if (op.succeedsOrder >= 0) enq(op.succeedsOrder, type)
    for (const p of op.parents) enq(p, type)
  }

  // console.log('diff', aOut.map(order => getLocalVersion(db, order)), bOut.map(order => getLocalVersion(db, order)))
  return {aOnly, bOnly}
}

const diffV = (db: DBState, a: RawVersion[], b: RawVersion[]) => {
  const aOrder = a.map(v => versionToOrder(db.versionToOrder, v.agent, v.seq))
  const bOrder = b.map(v => versionToOrder(db.versionToOrder, v.agent, v.seq))
  return diff(db, aOrder, bOrder)
}

const vEq = (a: RawVersion, b: RawVersion) => a.agent === b.agent && a.seq === b.seq

const getVal = (db: DBState, key: string): DocValue => (
  db.data.get(key) ?? [{
    version: ROOT_VERSION,
    value: null
  }]
)

/**
 * Apply an operation to the database, and move the current branch forward based
 * on the operation's version.
 *
 * Note: It would be cleaner to separate this into two methods, one to ingest
 * the operation and one to merge the operation's contents into the local
 * database. (Ie, this does the equivalent of `git fetch` then `git merge`.
 * These functions should be separated.)
 */
const applyForwards = (db: DBState, op: RawOperation) => {
  // The operation supercedes all the versions named in parents with version.
  let oldV = db.version.get(op.version.agent)
  assert(oldV == null || op.version.seq > oldV, "db already contains inserted operation")

  for (const {agent, seq} of op.parents) {
    const dbSeq = db.version.get(agent)!
    assert(dbSeq != null && dbSeq >= seq, "Cannot apply operation from the future")

    const oldRefs = db.refs.get(agent, seq) ?? 0
    db.refs.set(agent, seq, oldRefs + 1)

    if (dbSeq === seq && oldRefs === 0) db.versionFrontier.delete(agent)
  }
  db.version.set(op.version.agent, op.version.seq)
  db.versionFrontier.add(op.version.agent)

  // *** The version metadata is now up to date. Add the raw operation to the
  // database

  let order = db.versionToOrder.get(op.version.agent, op.version.seq)
  // We check becuase the operation might already be known in the database.
  if (order == null) {
    order = db.operations.length
    db.versionToOrder.set(op.version.agent, op.version.seq, order)
  }
  
  const localOp: LocalOperation = {
    version: op.version,
    order,
    parents: op.parents.map(v => db.versionToOrder.get(v.agent, v.seq) ?? -1),
    succeedsOrder: op.succeedsSeq == null ? -1
      : db.versionToOrder.get(op.version.agent, op.succeedsSeq)!,
    docOps: op.docOps,

    raw: op,
  }
  db.operations[order] = localOp // Appends if necessary.
  
  // *** And apply the named changes to the data model in db.data.
  for (const {key, parents, newValue} of op.docOps) {
    const prevVals = getVal(db, key)

    // The doc op's parents field contains a subset of the versions present in
    // oldVal.
    // - Any entries in prevVals that aren't named in the new operation are kept
    // - And any entries in parents that aren't directly named in prevVals must
    //   be ancestors of the current document value. This indicates a conflict,
    //   and we'll keep everything.

    // We'll check ancestry. Every parent of this operation (parents) must
    // either be represented directly in prevVals or be dominated of one of
    // them.
    for (const v of parents) {
      const exists = !!prevVals.find(({version: v2}) => vEq(v, v2))
      if (!exists) {
        // console.log('Checking ancestry')
        // We expect one of v2 to dominate v.
        const ancestry = prevVals.map(({version: v2}) => compareVersions(db, v2, v))
        assert(ancestry.findIndex(a => a > 0) >= 0)
      }
    }

    const newVal = [{version: op.version, value: newValue}]
    for (const oldEntry of prevVals) {
      if (parents.find(v => vEq(v, oldEntry.version)) == null) {
        // Keep this.
        newVal.push(oldEntry)
      }
    }

    // If there's multiple conflicting values, we keep them in version-sorted
    // order to make db comparisons easier in the fuzzer.
    db.data.set(key, newVal.sort((a, b) => vCmp(a.version, b.version)))
  }

  checkDb(db)
}

/**
 * This is the inverse of applyForwards above. It purges the operation from the
 * data model and the version, but it does not prune the operation from the
 * known set of operations.
 */
const applyBackwards = (db: DBState, op: RawOperation) => {
  // Version stuff
  assert(db.version.get(op.version.agent) === op.version.seq)
  assert(db.versionFrontier.has(op.version.agent), "Cannot unapply operations in violation of partial order")

  if (op.succeedsSeq != null) {
    db.version.set(op.version.agent, op.succeedsSeq)
    if ((db.refs.get(op.version.agent, op.succeedsSeq) ?? 0) !== 0) {
      db.versionFrontier.delete(op.version.agent)
    }
  } else {
    db.version.delete(op.version.agent)
    db.versionFrontier.delete(op.version.agent)
  }

  for (const {agent, seq} of op.parents) {
    const dbSeq = db.version.get(agent)
    // console.log('dbseq', dbSeq, 'seq', seq)
    assert(dbSeq == null || dbSeq >= seq, "internal consistency error in versions")

    const newRefs = db.refs.get(agent, seq)! - 1
    assert(newRefs >= 0)

    if (newRefs === 0) db.refs.delete(agent, seq) // Makes equals simpler.
    else db.refs.set(agent, seq, newRefs)

    if (newRefs === 0 && dbSeq === seq) db.versionFrontier.add(agent)
  }

  // Ok now update the data.
  for (const {key, parents} of op.docOps) {
    const prevVals = db.data.get(key)!

    // The values should instead contain:
    // - Everything in prevVals not including op.version
    // - All the objects named in parents that aren't superceded by another
    //   document version
    const newVals = prevVals.filter(({version}) => !vEq(version, op.version))
    // console.log('prevVals', prevVals)
    // And append all the parents that aren't dominated by another value.
    for (const p of parents) {
      // TODO: Assert p not already represented in prevVals, which would be invalid.

      // If p is dominated by a value in prevVals, skip.
      const dominated = newVals.map(({version}) => compareVersions(db, p, version))
      if (dominated.findIndex(x => x < 0) >= 0) continue

      if (vEq(p, ROOT_VERSION)) {
        assert(newVals.length === 0)
      } else {
        const parentOp = db.operations[db.versionToOrder.get(p.agent, p.seq)!]
        assert(parentOp)
        const parentDocOp = parentOp.docOps.find(({key: key2}) => key === key2)!
        assert(parentDocOp)
        newVals.push({version: p, value: parentDocOp.newValue})
      }
    }

    // console.log('newVals', newVals)
    // This is a bit dirty, to handle the root.
    if (newVals.length === 0) db.data.delete(key)
    else db.data.set(key, newVals)
  }

  // Not done: Remove the operation itself from the db.operations set.

  checkDb(db)
}

// Verify the database is internally consistent.
const checkDb = (db: DBState) => {
  if (!CHECK) return

  // The frontier entries should always be at the current version
  // console.log('checkdb', db)
  for (const [agent, seq] of db.version) {
    const isFrontier = db.versionFrontier.has(agent)
    if (isFrontier) {
      assert.strictEqual(db.refs.get(agent, seq), undefined)
      assert(db.refs.get(agent, seq) !== 0)
    } else {
      assert.notStrictEqual(db.refs.get(agent, seq) ?? 0, 0, "missing frontier element: " + agent)
    }
  }

  // Scan all the operations. For each operation with parents, those parents
  // should have a smaller localOrder field.
  for (const [order, op] of db.operations.entries()) {
    assert.strictEqual(op.order, order)
    const {agent, seq} = op.version
    assert.strictEqual(db.versionToOrder.get(agent, seq), order)

    // Check the direct parent
    if (op.succeedsOrder > -1) {
      assert(op.succeedsOrder < order)
      // Direct parent has a matching agent id.
      assert.strictEqual(db.operations[op.succeedsOrder].version.agent, agent)
    }
    
    for (const o of op.parents) {
      assert(o < order)
      // if (o > -1) assert.notStrictEqual(db.operations[o].version.agent, agent)
    }
  }
}

const randomReal = seedrandom("hi")
const randomInt = (max: number) => (randomReal.int32() + 0xffffffff) % max
const randItem = <T>(arr: T[]): T => arr[randomInt(arr.length)]

const randomOp = (db: DBState, ownedAgents: string[], iter: number): RawOperation => {
  const agent = ownedAgents[randomInt(ownedAgents.length)]
  const oldVersion = db.version.get(agent)
  // We'll need to use the next sequence number from the current state
  const seq = oldVersion == null ? 1 : oldVersion + 1

  // The parents of the operation are the current frontier set.
  // It'd be good to occasionally make a super old operation and throw that in too though.
  const parents: RawVersion[] = Array.from(db.versionFrontier)
  .map(agent => ({agent, seq: db.version.get(agent)!}))

  // The set of keys should always grow, but we'll make it grow slower as the run goes on.
  const key = 'KEY_' + randomInt(Math.sqrt(iter)|0)
  // const key = randItem(['a', 'b', 'c', 'd', 'e', 'f'])
  const docParents = getVal(db, key).map(({version}) => version)

  return {
    version: {agent, seq},
    succeedsSeq: oldVersion ?? null,
    parents,
    docOps: [{
      key: key,
      newValue: randomInt(100),
      parents: docParents
    }],
  }
}

const getOrderVersion = (db: DBState): number[] => (
  Array.from(db.versionFrontier).map(agent => (
    agent === ROOT_VERSION.agent ? -1 : db.versionToOrder.get(agent, db.version.get(agent)!)!
  ))
)

const cmp = (a: number, b: number) => a - b

const getBranch = (db: DBState): RawVersion[] => (
  Array.from(db.versionFrontier).map(agent => (
    {agent, seq: db.version.get(agent)!}
  ))
)

// Branch specified as a frontier map - sparce agent => seq.
// Returns original branch.
const switchBranch = (db: DBState, branch: RawVersion[]): RawVersion[] => {
  const start = getBranch(db)
  const end = branch

  const {aOnly, bOnly} = diffV(db, start, end)

  for (const order of aOnly) {
    applyBackwards(db, db.operations[order].raw)
  }

  for (const order of bOnly.reverse()) {
    applyForwards(db, db.operations[order].raw)
  }
  return start
}

/**
 * Sync a and b. Eventually this should not depend on the db.version field. It
 * should instead generate a bloom filter or something of known versions and
 * then use a proper network protocol to sync. But for now this is fine.
 */
const syncPeers = (a: DBState, b: DBState) => {
  const onlyA = [], onlyB = []

  const agents = new Set([...a.version.keys(), ...b.version.keys()])

  const getOperationsFrom = (db: DBState, agent: string, seqStart: number, seqEnd: number | null) => {
    const resultOrders: number[] = []
    let order = versionToOrder(db.versionToOrder, agent, seqStart)
    let orderEnd = seqEnd == null ? -1 : versionToOrder(db.versionToOrder, agent, seqEnd)

    while (order > orderEnd) {
      const op = db.operations[order]
      assert(op != null)
      resultOrders.push(order)
      order = op.succeedsOrder
    }
    return resultOrders
  }

  for (let agent of agents) {
    const aSeq = a.version.get(agent)
    const bSeq = b.version.get(agent)
    if (aSeq == bSeq) continue
    assert(aSeq != null || bSeq != null) // Either they're both set or one is null.

    const aBigger = bSeq == null || (aSeq! > bSeq)

    if (aBigger) {
      onlyA.push(...getOperationsFrom(a, agent, aSeq!, bSeq ?? null))
    } else {
      onlyB.push(...getOperationsFrom(b, agent, bSeq!, aSeq ?? null))
    }
  }
  const onlyAOps = onlyA.sort(cmp).map(order => a.operations[order].raw)
  const onlyBOps = onlyB.sort(cmp).map(order => b.operations[order].raw)

  // console.log('syncing peers', aId, bId, 'distance:', onlyAOps.length, onlyBOps.length)

  for (const op of onlyBOps) {
    applyForwards(a, op)
  }
  for (const op of onlyAOps) {
    applyForwards(b, op)
  }
}

const test = () => {
  // const numPeers = 1
  const numPeers = 3
  const numOpsPerIter = 10
  // const numOpsPerIter = 10
  const numIterations = 2000
  // const numIterations = 300

  const peers = new Array(numPeers).fill(null).map(() => ({
    db: <DBState>{
      data: new Map<string, DocValue>(),
      version: new Map([[ROOT_VERSION.agent, ROOT_VERSION.seq]]), // Expanded for all agents
      refs: new Map2(),
      versionFrontier: new Set([ROOT_VERSION.agent]), // Kept in sorted order based on comparator.
      versionToOrder: new Map2(),
      operations: []
    },
    iterStartV: new Map<string, number>(),
    iterStartData: new Map<string, DocValue>(),
    iterOps: <RawOperation[]>[] // Operations this iteration
  }))

  let nextAgent = 100

  for (let iter = 0; iter < numIterations; iter++) {
    if ((iter % 100) == 0) console.log('***** ITER', iter)
    for (const peer of peers) {
      peer.iterStartV = new Map(peer.db.version)
      peer.iterStartData = new Map(peer.db.data)
      peer.iterOps.length = 0
    }

    // const startVersion = new Map(peers[0].db.version)
    // const startState = new Map(peers[0].db.data)
    // They should all start at the same version.
    // for (const {db} of peers.slice(1)) assert.deepStrictEqual(db.version, startVersion)

    // Generate numOpsPerIter assigned to random peers in our pool
    for (let i = 0; i < numOpsPerIter; i++) {
      const peerId = randomInt(peers.length)
      const peer = peers[peerId]
      const op = randomOp(peer.db, [`P_${peerId + 1}`, `P_${peerId + 1}_${nextAgent++}`], iter)

      // console.log(op)
      applyForwards(peer.db, op)
      peer.iterOps.push(op)
    }

    for (const {db} of peers) checkDb(db)

    // For each peer, rewind the newly created operations and reapply them.
    for (const peer of peers) {
      const finalVersion = new Map(peer.db.version)
      const finalFrontierOrder = getOrderVersion(peer.db)

      peer.iterOps.slice().reverse().forEach(op => {
        applyBackwards(peer.db, op)
      })
      assert.deepStrictEqual(peer.db.version, peer.iterStartV)
      assert.deepStrictEqual(peer.db.data, peer.iterStartData)
      const initialFrontierOrder = getOrderVersion(peer.db)

      peer.iterOps.forEach(op => {
        applyForwards(peer.db, op)
      })
      assert.deepStrictEqual(peer.db.version, finalVersion)

      // The difference between startVersion and finalVersion should just be the
      // operations.
      const {aOnly, bOnly} = diff(peer.db, initialFrontierOrder, finalFrontierOrder)
      assert.strictEqual(aOnly.length, 0)
      assert.strictEqual(bOnly.length, peer.iterOps.length)
      const bVersions = bOnly.map(order => peer.db.operations[order].version).reverse()
      const peerVersions = peer.iterOps.map(op => op.version)
      assert.deepStrictEqual(bVersions, peerVersions)

      checkDb(peer.db)
    }

    // We'll pick a random pair of peers and synchronize them.
    if (peers.length >= 2) {
      const aId = randomInt(peers.length)
      let bId = randomInt(peers.length - 1)
      if (bId >= aId) ++bId
      const [a, b] = [peers[aId].db, peers[bId].db]

      const [aHead, bHead] = [getBranch(a), getBranch(b)]

      // Actually sync, bringing both a and b into alignment.
      syncPeers(a, b)

      assertDbEq(a, b)
      checkDb(a)
      checkDb(b)
      
      const mergeHead = getBranch(a)
      
      // console.log('merge', mergeHead, aHead, bHead)

      // So we should be able to bounce between branches now.
      switchBranch(a, aHead)
      switchBranch(b, aHead)
      assertDbEq(a, b)

      switchBranch(a, bHead)
      switchBranch(b, bHead)
      assertDbEq(a, b)

      switchBranch(a, mergeHead)
      switchBranch(b, mergeHead)
      assertDbEq(a, b)
    }

  }
}

test()