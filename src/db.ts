/**
 * For now I'm using a simple KV store (LMDB). Later I'd like to add support
 * for:
 *
 * - In memory store (largely for fuzzer testing)
 * - FoundationDB (for scaling)
 *
 * We need to store the following:
 *
 * - The append-only set of historical operations. I could also imagine trimming
 *   this data set periodically and adding support for that. Also for CRDTs like
 *   yjs/automerge we'll need a different wire format for the historical
 *   operations to keep the data set compact.
 * - One or multiple "live" views of the data at a branch / version. This
 *   contains a KV map from collection + document name => {document value(s),
 *   version(s)}. The actual format here might depend on information in the
 *   schema.
 * - The schema. Is this another database? It should be editable by the clients
 *   with a metaschema. But I think it might work differently with branches and
 *   stuff. I'll think about this later.
 */

import {open as lmdbOpen, Database} from 'lmdb-store'
import {pack, unpack} from 'fdb-tuple'
import {LocalOperation, RemoteVersion, RemoteOperation, ROOT_VERSION, DocId, DocValue} from './types'
import { getLastKey, keyInc } from './util'
import assert from 'assert'

// const DEEPCHECK = process.env['CHECK'] ? true : false
const DEEPCHECK = true
if (DEEPCHECK) console.log('running in checked mode')


export interface Db {
  ops: Database, // All operations ever.
  data: Database // Might be worth having multiple of this.
}

export function open(): Db {
  const root = lmdbOpen({
    path: 'store',
    keyIsBuffer: true,
  })

  // Two databases - one with all the operations:
  const ops = root.openDB('ops', {keyIsBuffer: true})
  // And another with a snapshot of the data at some point in time:
  const data = root.openDB('data', {keyIsBuffer: true})

  return {ops, data}
}

// *** Operations ***

/*
 * Operations are stored in a local list with list numbers constrained by
 * partial order of dependancies.
 *
 * For now this set is append only.
 *
 * The op
 */

// *** Operation database
// order => LocalOperation
const getOpKey = (order: number | null) => pack(order == null ? 'op' : ['op', order])
// version => order (number)
const getOrderForVersionKey = (version: RemoteVersion) => pack(['order', version.agent, version.seq])

// *** Document database
const getDocKey = (key: DocId) => pack(['doc', key.collection, key.key])
const branchKey = pack('branch') // Contains a list of orders.

// Get the highest known sequence number for the specified agent. Returns -1 if none found.
const getMaxSeq = (db: Db, agent: string): number | null => {
  const key = getLastKey(db.ops, pack(['order', agent]))
  console.log('getMaxSeq for agent', agent, 'is', key)
  if (key == null) return null
  else return key[2] as number
}

type LocalOpWithoutOrder = Omit<LocalOperation, 'order'>
export function getOperation(db: Db, order: number): LocalOperation {
  // Currently we store the LocalOperation directly, which contains the full
  // agent string. Might be good to compact this.
  const val = db.ops.get(getOpKey(order)) as LocalOperation | null // Type is a lie. There's no order field.
  if (val == null) throw Error('Missing operation ' + order)
  val.order = order
  return val
}

export function getOperationByVersion(db: Db, version: RemoteVersion): LocalOperation {
  return getOperation(db, getOrder(db, version))
}

export function getOrder(db: Db, version: RemoteVersion): number {
  if (version.agent == ROOT_VERSION.agent) return -1
  const entry = db.ops.get(getOrderForVersionKey(version))
  assert(entry != null)
  return entry as number
}

const branchContainsVersion = (db: Db, target: number, branch: number[]): boolean => {
  // TODO: Might be worth checking if the target version has the same agent id
  // as one of the items in the branch and short circuiting if so.

  if (target === -1 || branch.indexOf(target) >= 0) return true
  if (branch.length === 0) return false

  // This works is via a DFS from the operation with a higher localOrder looking
  // for the localOrder of the smaller operation.

  const visited = new Set<number>() // Set of localOrders.

  let found = false

  // LIFO queue. We could use a priority queue here but I'm not sure it'd be any
  // faster in practice.
  const queue: number[] = branch.slice().sort((a, b) => b - a) // descending so we hit the lowest first.

  while (queue.length > 0 && !found) {
    const order = queue.pop()!

    if (order <= target) {
      if (order === target) found = true
      continue
    }

    if (visited.has(order)) continue
    visited.add(order)

    const op = getOperation(db, order)
    assert(op != null) // If we hit the root operation, we should have already returned.

    queue.push(...op.parents) // TODO: Could sort in descending order.

    // Ordered so we hit this next.
    if (op.succeeds > -1) queue.push(op.succeeds)
  }

  return found
}

export function addOperation(db: Db, op: RemoteOperation): number {
  assert(op.parents.length > 0, 'Parents field with empty length is invalid')

  const vKey = getOrderForVersionKey(op.version)
  let order = db.ops.get(vKey) as number | null
  if (order != null) return order // The operation is already in the database

  // Logic from applyForwards in operations.ts.
  for (const version of op.parents) {
    // Check that the parent is in the database.
    if (version.agent !== ROOT_VERSION.agent) {
      assert(db.ops.get(getOrderForVersionKey(version)) != null, 'Cannot merge future operation')
    }
  }

  // Ok looks good. We'll assign an order and merge.
  const lastOrder = getLastKey(db.ops, getOpKey(null))
  const newOrder = lastOrder == null ? 0 : (lastOrder[1] as number) + 1

  const localOp: LocalOpWithoutOrder = {
    version: op.version,
    // order: newOrder,
    docOps: op.docOps,
    parents: op.parents.map(v => getOrder(db, v)),
    succeeds: op.succeedsSeq === -1 ? -1
    : getOrder(db, {agent: op.version.agent, seq: op.succeedsSeq})
  }

  // And save the new operation in the store. TODO: Do this in a transaction?
  // The transaction could be issued from our parent, but I'm not sure if thats
  // a good idea.
  db.ops.put(getOpKey(newOrder), localOp)
  db.ops.put(getOrderForVersionKey(op.version), newOrder)

  return newOrder
}


const checkOps = (db: Db) => {
  // Iterate through all the operations. Make sure everything is consistent.
  const iter = db.ops.getRange({
    start: getOpKey(null),
    end: keyInc(getOpKey(null))
  })

  let last = -1
  for (const entry of iter) {
    const {key, value: op} = entry as {key: Buffer, value: LocalOperation}
    const order = unpack(key)[1] as number // ['op', order]

    assert(order > last)
    last = order

    assert(op.parents.length > 0)
    for (const parent of op.parents) assert(parent < order)

    // Make sure we can look up the order based on the version
    const order2 = getOrder(db, op.version)
    assert.strictEqual(order, order2)
  }
}




export function getVal(db: Db, key: DocId): DocValue {
  // Every document implicitly exists, but with a value of null.
  return (db.data.get(getDocKey(key)) as DocValue | null) ?? [{
    order: -1,
    value: null
  }]
}

export function getBranch(db: Db): number[] {
  return (db.data.get(branchKey) as number[]) ?? [-1]
}

// Returns new branch.
export function applyForwards(db: Db, order: number): number[] {
  // This method is often called after addOperation - in which case it might
  // make more sense to take the LocalOperation itself as an argument.
  const op = getOperation(db, order)
  const oldBranch = getBranch(db)

  // Check the operation fits. The operation should not be in the branch, but
  // all the operation's parents should be.
  assert(!branchContainsVersion(db, order, oldBranch), 'db already contains version')
  for (const parent of op.parents) {
    assert(branchContainsVersion(db, parent, oldBranch), 'operation in the future')
  }

  // Every version named in oldBranch is either:
  // - Equal to a branch in the new operation's parents (in which case remove it)
  // - Or newer than a branch in the operation's parents (in which case keep it)
  // If there were any versions which are older, we would have aborted above.
  const newBranch = [order,
    ... oldBranch.filter(o => !op.parents.includes(o))
  ]
  db.data.put(branchKey, newBranch)

  for (const {key, parents, opData} of op.docOps) {
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
    if (DEEPCHECK) for (const p of parents) {
      const exists = prevVals.findIndex(({order}) => order === p) >= 0

      if (!exists) {
        const branch = prevVals.map(({order}) => order)
        assert(branchContainsVersion(db, p, branch))
      }
    }

    const newVal: DocValue = [{order, value: opData}]
    for (const oldEntry of prevVals) {
      if (!parents.includes(oldEntry.order)) {
        // Keep this.
        newVal.push(oldEntry)
      }
    }

    // If there's multiple conflicting values, we keep them in version-sorted
    // order to make db comparisons easier in the fuzzer.
    newVal.sort((a, b) => a.order - b.order)
    db.data.put(getDocKey(key), newVal)
  }

  return newBranch
}


const test = async () => {
  const db = open()
  console.log('branch', getBranch(db))
  await db.ops.transaction(() => {
    const order = addOperation(db, {
      version: {
        agent: 'hi',
        seq: 0
      },
      parents: [ROOT_VERSION],
      succeedsSeq: -1,
      docOps: [{
        key: {collection: 'posts', key: 'yo'},
        parents: [-1],
        opData: {
          title: 'heeey',
        }
      }],
    })

    applyForwards(db, order)
  })
  checkOps(db)
  console.log('branch', getBranch(db))
  console.log(getVal(db, {collection: 'posts', key: 'yo'}))
}
test()