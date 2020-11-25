import { LocalValue, LocalVersion, ROOT_VERSION } from "./types"
import assert from 'assert'
import { versions } from "process"
import seedrandom from 'seedrandom'
import Map2 from 'map2'

// An operation affects multiple documents 
export interface Operation {
  version: LocalVersion,
  succeeds: number | null,
  parents: LocalVersion[], // This does not include the direct parent.

  docOps: DocOperation[],
}

export interface DocOperation {
  key: string,
  collection: string,
  prevVersions: LocalVersion[], // Usually one item. Multiple if this is a merge operation.
  newValue: any,
}

export type DocValue = {
  version: LocalVersion
  value: any,
} | { // Version in conflict
  version: LocalVersion,
  value: any
}[]


interface DBState {
  data: Map<string, DocValue>

  version: Map<number, number> // Expanded for all agents
  // The number of times each (agent, seq) is referenced by an operation with a different version agent.
  foreignRefs: Map2<number, number, number>
  // This contains every entry in version where foreignRefs is 0 (or equivalently undefined).
  versionFrontier: Set<number> // set of agent ids.
}

const cloneDb = (db: DBState): DBState => ({
  data: new Map(db.data),
  version: new Map(db.version),

  foreignRefs: new Map2(db.foreignRefs),
  
  versionFrontier: new Set(db.versionFrontier),
})


// const localVersionCmp = (a: LocalVersion, b: LocalVersion) => (
//   a.agent === b.agent ? a.seq - b.seq : a.agent - b.agent
// )

const applyForwards = (db: DBState, op: Operation) => {
  // assert(op.parents.find(p => p.agent === op.version.agent) != null, "Operations must succeed own parent")

  // The operation supercedes all the versions named in parents with version.
  let oldV = db.version.get(op.version.agent)
  assert(oldV == null || oldV < op.version.seq)

  for (const {agent, seq} of op.parents) {
    const dbSeq = db.version.get(agent)
    assert(dbSeq! >= seq, "Cannot apply operation from the future")
    assert(agent != op.version.agent, "Versions should not include self")

    const oldRefs = db.foreignRefs.get(agent, seq) ?? 0
    db.foreignRefs.set(agent, seq, oldRefs + 1)

    if (dbSeq === seq && oldRefs === 0) db.versionFrontier.delete(agent)

    // if (db.versionFrontier.has(agent)) {
    //   if (dbSeq === seq) db.versionFrontier.delete(agent)
    //   // else if dbSeq < seq we're forking. Keep the current frontier and add to it.
    // }
  }
  db.version.set(op.version.agent, op.version.seq)
  db.versionFrontier.add(op.version.agent)
  // db.foreignRefs.set(op.version.agent, op.version.seq, 0)

  // Then apply document changes
  checkDb(db)
}

const applyBackwards = (db: DBState, op: Operation) => {
  assert(db.version.get(op.version.agent) === op.version.seq)

  assert(db.versionFrontier.has(op.version.agent), "Cannot unapply operations in violation of partial order")
  assert(db.version.get(op.version.agent) === op.version.seq)
  // db.versionFrontier.delete(op.version.agent)

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

    // if (agent === op.version.agent) {
    //   db.version.set(agent, seq) // Replace the parent in version.
    // }

    // if (versionFrontier.get(agent) === seq)
    // if (dbSeq == null) db.versionFrontier.add(agent)
  }
  checkDb(db)
}

const checkDb = (db: DBState) => {
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
}

const randomReal = seedrandom("hi")
const randomInt = (max: number) => (randomReal.int32() + 0xffffffff) % max

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
  // if (oldVersion != null) parents.push({agent, seq: oldVersion})

  return {
    version: {agent, seq},
    succeeds: oldVersion || null,
    docOps: [],
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
    versionFrontier: new Set([ROOT_VERSION.agent]) // Kept in sorted order based on comparator.
  }
  checkDb(db)

  let nextAgent = 100
  
  for (let iter = 0; iter < numIterations; iter++) {
    let peerState = new Array(numPeers).fill(null).map(() => ({
      db: cloneDb(db),
      ops: [] as Operation[]
    }))

    // console.log('db', db)
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
          applyForwards(localDb, op)
        })
        assert.deepStrictEqual(localDb, peer.db)
        
        peer.ops.slice().reverse().forEach(op => {
          // console.log('apply backwards', localDb, op)
          applyBackwards(localDb, op)
        })
        assert.deepStrictEqual(localDb, db)
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

      assert.deepStrictEqual(localDb, db)
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