import { LocalValue, LocalVersion, ROOT_VERSION } from "./types"
import assert from 'assert'
import { versions } from "process"
import seedrandom from 'seedrandom'
import Map2 from 'map2'
import { Console } from "console"

const console = new Console({
  stdout: process.stdout,
  stderr: process.stderr,
  inspectOptions: {
    depth: null
  }
})

const CHECK = process.env['NOCHECK'] ? false : true

// An operation affects multiple documents 
export interface Operation {
  version: LocalVersion,
  succeeds: number | null,
  parents: LocalVersion[], // This does not include the direct parent.

  docOps: DocOperation[],
}

type OperationWithOrder = Operation & {
  // This is filled in when an operation is added to the store. It is a local
  // order defined such that if a > b, a.localOrder > b.localOrder. If two
  // operations are concurrent they could end up in any order.
  localOrder: number,
}

export interface DocOperation {
  key: string,
  // collection: string,
  parents: LocalVersion[], // Specific to the document. The named operations edit this doc.
  newValue: any,
}

export type DocValue = { // Has multiple entries iff version in conflict.
  version: LocalVersion,
  value: any
}[]

type OperationSet = Map2<number, number, OperationWithOrder>

interface DBState {
  data: Map<string, DocValue>

  version: Map<number, number> // Expanded for all agents
  // The number of times each (agent, seq) is referenced by an operation with a different version agent.
  foreignRefs: Map2<number, number, number>
  // This contains every entry in version where foreignRefs is 0 (or equivalently undefined).
  versionFrontier: Set<number> // set of agent ids.

  // This stuff is not compared when we compare databases.
  nextOrder: number,
  knownOperations: OperationSet // History of all seen operations
}

const cloneDb = (db: DBState): DBState => ({
  // Could also just use new[k] = new old[k].constructor(old[k]).
  data: new Map(db.data),
  version: new Map(db.version),

  foreignRefs: new Map2(db.foreignRefs),
  
  versionFrontier: new Set(db.versionFrontier),

  nextOrder: db.nextOrder,
  knownOperations: new Map2(db.knownOperations),
})

const assertDbEq = (a: DBState, b: DBState) => {
  if (!CHECK) return

  assert.deepStrictEqual(a.data, b.data)
  assert.deepStrictEqual(a.version, b.version)
  assert.deepStrictEqual(a.foreignRefs, b.foreignRefs)
  assert.deepStrictEqual(a.versionFrontier, b.versionFrontier)
  // assert.deepStrictEqual(a.knownOperations, b.knownOperations)
}

// There are 4 cases:
// - A dominates B (return +ive)
// - B dominates A (return -ive)
// - A and B are equal (not checked here)
// - A and B are concurrent (return 0)
const compareVersions = (allOps: OperationSet, a: LocalVersion, b: LocalVersion): number => {
  // So there's absolutely ways to optimize this using fancy skip list nonsense
  // and whatnot, but for now I'm just going to use a pretty standard BFS.
  if (a.agent === b.agent) return a.seq - b.seq

  const dominates = (a: LocalVersion, bagent: number, bmaxseq: number): boolean => {
    // We'll expand `a` out into a full version map.
    const frontier: number[] = [] // Pairs of agent, old max seq
    const maxVersion = new Map<number, number>() // max version seen for each agent, including in frontier.

    const add = ({agent, seq}: LocalVersion) => {
      const curMax = maxVersion.get(agent) ?? 0
      if (curMax == null || seq > curMax) {
        frontier.push(agent, curMax)
        maxVersion.set(agent, seq)
      } // Otherwise ignore it.
    }
    add(a)
    
    while (frontier.length > 0) {
      let oldMax = frontier.pop()!
      let agent = frontier.pop()!
      let seq = maxVersion.get(agent)!

      // Iterate through all operations from seq back to oldMax
      while (seq >= oldMax) {
        const op = allOps.get(agent, seq)
        if (!op) throw Error(`missing operation: ${agent} ${seq}`)
        for (const v of op.parents) add(v)
        if (op.succeeds == null) break
        seq = op.succeeds
      }

      // Early return if we've seen it already.
      if ((maxVersion.get(bagent) ?? 0) >= bmaxseq) break
    }

    return (maxVersion.get(bagent) ?? 0) >= bmaxseq
  }

  const adomb = dominates(a, b.agent, b.seq)
  const bdoma = dominates(b, a.agent, a.seq)
  if (adomb && bdoma) throw Error('Invalid state - operations do not dominate each other')
  else return (adomb && !bdoma) ? 1
    : (!adomb && bdoma) ? -1
    : 0 // Neither operation dominates the other.
}

// const localVersionCmp = (a: LocalVersion, b: LocalVersion) => (
//   a.agent === b.agent ? a.seq - b.seq : a.agent - b.agent
// )

const vEq = (a: LocalVersion, b: LocalVersion) => a.agent === b.agent && a.seq === b.seq

const getVal = (db: DBState, key: string): DocValue => (
  db.data.get(key) ?? [{
    version: ROOT_VERSION,
    value: null
  }]
)

const applyForwards = (db: DBState, op: Operation) => {
  // The operation supercedes all the versions named in parents with version.
  let oldV = db.version.get(op.version.agent)
  assert(oldV == null || op.version.seq > oldV, "db already contains inserted operation")

  for (const {agent, seq} of op.parents) {
    const dbSeq = db.version.get(agent)
    assert(dbSeq! >= seq, "Cannot apply operation from the future")
    assert(agent != op.version.agent, "Versions should not include self")

    const oldRefs = db.foreignRefs.get(agent, seq) ?? 0
    db.foreignRefs.set(agent, seq, oldRefs + 1)

    if (dbSeq === seq && oldRefs === 0) db.versionFrontier.delete(agent)
  }
  db.version.set(op.version.agent, op.version.seq)
  db.versionFrontier.add(op.version.agent)
  db.knownOperations.set(op.version.agent, op.version.seq, {...op, localOrder: db.nextOrder++})

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
        const ancestry = prevVals.map(({version: v2}) => compareVersions(db.knownOperations, v2, v))
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

const applyBackwards = (db: DBState, op: Operation) => {
  // Version stuff
  assert(db.version.get(op.version.agent) === op.version.seq)
  assert(db.versionFrontier.has(op.version.agent), "Cannot unapply operations in violation of partial order")

  if (op.succeeds != null) {
    db.version.set(op.version.agent, op.succeeds)
    if ((db.foreignRefs.get(op.version.agent, op.succeeds) ?? 0) !== 0) {
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
    assert(agent !== op.version.agent)

    const newRefs = db.foreignRefs.get(agent, seq)! - 1
    assert(newRefs >= 0)

    if (newRefs === 0) db.foreignRefs.delete(agent, seq) // Makes equals simpler.
    else db.foreignRefs.set(agent, seq, newRefs)

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
      const dominated = newVals.map(({version}) => compareVersions(db.knownOperations, p, version))
      if (dominated.findIndex(x => x < 0) >= 0) continue

      if (vEq(p, ROOT_VERSION)) {
        assert(newVals.length === 0)
      } else {
        const parentOp = db.knownOperations.get(p.agent, p.seq)!
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

  // Not strictly necessary.
  db.knownOperations.delete(op.version.agent, op.version.seq)

  checkDb(db)
}

const checkDb = (db: DBState) => {
  if (!CHECK) return

  // The frontier entries should always be at the current version
  // console.log('checkdb', db)
  for (const [agent, seq] of db.version) {
    const isFrontier = db.versionFrontier.has(agent)
    if (isFrontier) {
      assert.strictEqual(db.foreignRefs.get(agent, seq), undefined)
      assert(db.foreignRefs.get(agent, seq) !== 0)
    } else {
      assert.notStrictEqual(db.foreignRefs.get(agent, seq) ?? 0, 0, "missing frontier element: " + agent)
    }
  }

  // Scan all the operations. For each operation with parents, those parents
  // should have a smaller localOrder field.
  for (const [agent, seq, op] of db.knownOperations) {
    const thisOrder = op.localOrder
    // Check the direct parent
    if (op.succeeds != null) {
      assert(db.knownOperations.get(agent, op.succeeds)!.localOrder < thisOrder)
    }
    
    for (const {agent, seq} of op.parents) {
      if (agent !== 0 && seq !== 0) {
        assert(db.knownOperations.get(agent, seq)!.localOrder < thisOrder)
      }
    }
  }
}

const randomReal = seedrandom("hi")
const randomInt = (max: number) => (randomReal.int32() + 0xffffffff) % max
const randItem = <T>(arr: T[]): T => arr[randomInt(arr.length)]

const randomOp = (db: DBState, ownedAgents: number[]): Operation => {
  const agent = ownedAgents[randomInt(ownedAgents.length)]
  const oldVersion = db.version.get(agent)
  // We'll need to use the next sequence number from the current state
  const seq = oldVersion == null ? 1 : oldVersion + 1

  // The parents of the operation are the current frontier set.
  // It'd be good to occasionally make a super old operation and throw that in too though.
  const parents: LocalVersion[] = Array.from(db.versionFrontier)
  .filter(a => a !== agent)
  .map((agent) => ({agent, seq: db.version.get(agent)!}))

  const key = randItem(['a', 'b', 'c', 'd', 'e', 'f'])
  const docParents = getVal(db, key).map(({version}) => version)

  return {
    version: {agent, seq},
    succeeds: oldVersion || null,
    docOps: [{
      key: key,
      newValue: randomInt(100),
      parents: docParents
    }],
    parents
  }
}

const test = () => {
  // const numPeers = 1
  const numPeers = 3
  const numOpsPerIter = 10
  // const numOpsPerIter = 10
  const numIterations = 100

  let db: DBState = {
    data: new Map<string, DocValue>(),
    version: new Map<number, number>([[ROOT_VERSION.agent, ROOT_VERSION.seq]]), // Expanded for all agents
    foreignRefs: new Map2(),
    versionFrontier: new Set([ROOT_VERSION.agent]), // Kept in sorted order based on comparator.
    nextOrder: 0,
    knownOperations: new Map2(),
  }
  checkDb(db)

  let nextAgent = 100
  
  for (let iter = 0; iter < numIterations; iter++) {
    let peerState = new Array(numPeers).fill(null).map(() => ({
      db: cloneDb(db),
      ops: [] as Operation[]
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

        peer.ops.forEach(op => {
          // console.log('apply forwards', localDb, op)
          applyForwards(localDb, op)
        })
        // console.log('->', localDb)
        assertDbEq(localDb, peer.db)
        
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