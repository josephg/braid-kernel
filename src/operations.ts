import { LocalValue, LocalVersion, ROOT_VERSION } from "./types"
import assert from 'assert'
import { versions } from "process"
import seedrandom from 'seedrandom'
import Map2 from 'map2'
import { Console } from "console"
import PriorityQueue from 'priorityqueuejs'

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
export interface RawOperation {
  version: LocalVersion, // These could be RawVersions.
  succeedsSeq: number | null,
  parents: LocalVersion[], // Only direct, non-transitive dependancies. May or may not include own agent's parent.

  docOps: DocOperation[],
}

interface LocalOperation {
  version: LocalVersion,

  // This is filled in when an operation is added to the store. It is a local
  // order defined such that if a > b, a.localOrder > b.localOrder. If two
  // operations are concurrent they could end up in any order.
  order: number,
  succeedsOrder: number, // Or -1.
  parents: number[] // localOrder of parents. Semantics same as RawOperation.parents.
  docOps: DocOperation[],
}

export interface DocOperation {
  key: string,
  // collection: string,

  // TODO: Change parents to a local order too.
  parents: LocalVersion[], // Specific to the document. The named operations edit this doc.
  newValue: any,
}

export type DocValue = { // Has multiple entries iff version in conflict.
  version: LocalVersion,
  value: any
}[]

type OperationSet = Map2<number, number, number>

interface DBState {
  data: Map<string, DocValue>

  version: Map<number, number> // Expanded for all agents
  // The number of times each (agent, seq) is referenced by an operations
  refs: Map2<number, number, number>
  // This contains every entry in version where foreignRefs is 0 (or equivalently undefined).
  versionFrontier: Set<number> // set of agent ids.

  // This stuff is not compared when we compare databases.
  operations: LocalOperation[], // order => operation.
  versionToOrder: OperationSet, // agent,seq => order.
}

const cloneDb = (db: DBState): DBState => ({
  // Could also just use new[k] = new old[k].constructor(old[k]).
  data: new Map(db.data),
  version: new Map(db.version),

  refs: new Map2(db.refs),
  
  versionFrontier: new Set(db.versionFrontier),

  // operations: db.operations,
  // versionToOrder: db.versionToOrder,
  operations: db.operations.slice(),
  versionToOrder: new Map2(db.versionToOrder),
})

const assertDbEq = (a: DBState, b: DBState) => {
  if (!CHECK) return

  assert.deepStrictEqual(a.data, b.data)
  assert.deepStrictEqual(a.version, b.version)
  assert.deepStrictEqual(a.refs, b.refs)
  assert.deepStrictEqual(a.versionFrontier, b.versionFrontier)
  // assert.deepStrictEqual(a.knownOperations, b.knownOperations)
}

// There are 4 cases:
// - A dominates B (return +ive)
// - B dominates A (return -ive)
// - A and B are equal (not checked here)
// - A and B are concurrent (return 0)
const getLocalOrder = (allOps: OperationSet, agent: number, seq: number): number => (
  agent === 0 && seq === 0 ? -1 : allOps.get(agent, seq)!
)

const getLocalVersion = (db: DBState, order: number) => (
  order === -1 ? ROOT_VERSION : db.operations[order].version
)

const compareVersions = (db: DBState, a: LocalVersion, b: LocalVersion): number => {
  if (a.agent === b.agent) return a.seq - b.seq

  // This works is via a DFS from the operation with a higher localOrder looking
  // for the localOrder of the smaller operation.

  const aOrder = getLocalOrder(db.versionToOrder, a.agent, a.seq)
  // console.log(b.agent, b.seq)
  const bOrder = getLocalOrder(db.versionToOrder, b.agent, b.seq)
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

  // Note its impossible for the operation with a smaller localOrder to dominate
  // the op with a larger localOrder.
  if (found) return aOrder - bOrder
  else return 0
}

const diff = (db: DBState, a: number[], b: number[]) => {
  console.log('calculating difference between', a, b)
  console.log('=', a.map(order => getLocalVersion(db, order)), b.map(order => getLocalVersion(db, order)))
  // I'm going to try and work purely with order numbers here.
  // const aOnly = new Set<number>(a)
  // const bOnly = new Set<number>(b)

  // Ok now aOnly and bOnly are strict subsets. The algorithm works by 

  // There's a bunch of ways to implement this. I'm not sure this is the best.
  const enum ItemType {
    Shared, A, B
  }
  // type Item = {
  //   type: ItemType,
  //   order: number
  // }

  const itemType = new Map<number, ItemType>()

  // Every order is in here at most once.
  const queue = new PriorityQueue<number>()
  
  // Number of items in the queue in both transitive histories
  let numShared = 0
  // const queueItems = new Set<number>()

  const enq = (order: number, type: ItemType) => {
    const currentType = itemType.get(order)
    if (currentType == null) {
      queue.enq(order)
      itemType.set(order, type)
      console.log('+++ ', order, type, getLocalVersion(db, order))
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

  const aOut: number[] = [], bOut: number[] = []

  // Loop until everything is shared.
  while (queue.size() > numShared) {
    const order = queue.deq()
    const type = itemType.get(order)!
    console.log('--- ', order, 'of type', type, getLocalVersion(db, order), 'shared', numShared, 'num', queue.size())
    assert(type != null)

    if (type === ItemType.Shared) numShared--
    else (type === ItemType.A ? aOut : bOut).push(order)

    if (order < 0) continue // Bottom out at the root operation.
    const op = db.operations[order]
    // if (op.succeedsOrder >= 0) enq(op.succeedsOrder, type)
    for (const p of op.parents) enq(p, type)
  }

  console.log('diff', aOut.map(order => getLocalVersion(db, order)), bOut.map(order => getLocalVersion(db, order)))
  return {aOut, bOut}
}

const vEq = (a: LocalVersion, b: LocalVersion) => a.agent === b.agent && a.seq === b.seq

const getVal = (db: DBState, key: string): DocValue => (
  db.data.get(key) ?? [{
    version: ROOT_VERSION,
    value: null
  }]
)

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
  }
  db.operations[order] = localOp // Appends if necessary.
  
  // Then apply document changes
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

    db.data.set(key, newVal)
  }

  checkDb(db)
}

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

  // Not strictly necessary. Actually its tricky - I'd need to delete the
  // versionToOrder entry and the operation out of the known set.
  // db.versionToOrder.delete(op.version.agent, op.version.seq)

  checkDb(db)
}

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

const randomOp = (db: DBState, ownedAgents: number[]): RawOperation => {
  const agent = ownedAgents[randomInt(ownedAgents.length)]
  const oldVersion = db.version.get(agent)
  // We'll need to use the next sequence number from the current state
  const seq = oldVersion == null ? 1 : oldVersion + 1

  // The parents of the operation are the current frontier set.
  // It'd be good to occasionally make a super old operation and throw that in too though.
  const parents: LocalVersion[] = Array.from(db.versionFrontier)
  .map(agent => ({agent, seq: db.version.get(agent)!}))

  const key = randItem(['a', 'b', 'c', 'd', 'e', 'f'])
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
    agent === 0 ? -1 : db.versionToOrder.get(agent, db.version.get(agent)!)!
  ))
)

const test = () => {
  // const numPeers = 1
  const numPeers = 3
  const numOpsPerIter = 10
  // const numOpsPerIter = 10
  const numIterations = 2
  // const numIterations = 300

  let db: DBState = {
    data: new Map<string, DocValue>(),
    version: new Map<number, number>([[ROOT_VERSION.agent, ROOT_VERSION.seq]]), // Expanded for all agents
    refs: new Map2(),
    versionFrontier: new Set([ROOT_VERSION.agent]), // Kept in sorted order based on comparator.
    versionToOrder: new Map2(),
    operations: []
  }
  checkDb(db)

  let nextAgent = 100
  
  for (let iter = 0; iter < numIterations; iter++) {
    let peerState = new Array(numPeers).fill(null).map(() => ({
      db: cloneDb(db),
      ops: [] as RawOperation[]
    }))

    // console.log('db', db.data)
    // Generate some random operations for the peers
    for (let i = 0; i < numOpsPerIter; i++) {
      const peerId = randomInt(peerState.length)
      const peer = peerState[peerId]
      const op = randomOp(peer.db, [peerId + 1, nextAgent++])

      // console.log(op)
      applyForwards(peer.db, op)
      peer.ops.push(op)
    }

    // console.dir(peerState, {depth: null})
    

    {
      // From db, apply all the operations for each agent and rewind them again.
      // We should end up in the original state.
      peerState.forEach(peer => {
        const localDb = cloneDb(db)
        const v1 = getOrderVersion(localDb)
        // console.log('db frontier', db.versionFrontier)

        peer.ops.forEach(op => {
          // console.log('apply forwards', localDb, op)
          applyForwards(localDb, op)
        })
        // console.log('->', localDb)
        assertDbEq(localDb, peer.db)
        const v2 = getOrderVersion(localDb)
        // console.log(peer.ops)
        // diff(localDb, v1, v2)
        
        peer.ops.slice().reverse().forEach(op => {
          // console.log('apply backwards', localDb, op)
          applyBackwards(localDb, op)
        })
        // console.log('<-', localDb)
        assertDbEq(localDb, db)
      })
    }

    let final1

    {
      // Ok all the agents' ops look good. We'll apply them all to db, then
      // unapply them in a different order from the total order we apply them
      // in. Because the operations are concurrent, we should end up in the
      // original state.

      const localDb = cloneDb(db)

      peerState.forEach(peer => {
        peer.ops.forEach(op => {
          applyForwards(localDb, op)
        })
      })

      final1 = cloneDb(localDb)
      // console.log('end state', final1)

      peerState.forEach(peer => {
        peer.ops.slice().reverse().forEach(op => {
          // console.log('apply backwards', localDb, op)
          applyBackwards(localDb, op)
        })
      })

      assertDbEq(localDb, db)
    }

    db = final1

    {
      // Test diff

      const fs = peerState.map(peer => (
        Array.from(peer.db.versionFrontier).map(agent => (
          agent === 0 ? -1 : db.versionToOrder.get(agent, db.version.get(agent)!)!
        ))
      ))

      // console.log(peerState.map(p => p.db.versionFrontier))

      console.log('fs', fs)

      for (let i = 0; i < peerState.length; i++) {
        diff(db, fs[i], fs[(i+1) % peerState.length])
      }
    }

    // // Make some operations from different peers
    // const op1: Operation = {
    //   docOps: [],
    //   parents: [ROOT_VERSION],
    //   version: {seq: 0, agent: 1}
    // }
    // console.log(db)
    // applyForwards(db, op1)

    // console.log(db)
    // applyBackwards(db, op1)
    // console.log(db)
  }
}

test()