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

import {open as lmdbOpen, Database, RootDatabase} from 'lmdb-store'
import {pack, unpack} from 'fdb-tuple'
import {LocalOperation, RemoteVersion, RemoteOperation, ROOT_VERSION, DocId, DocValue, RemoteValue, LocalDocOp, RemoteDocOp} from './types'
import { getLastKey, keyInc } from './util'
import assert from 'assert'

// const DEEPCHECK = process.env['CHECK'] ? true : false
const DEEPCHECK = true
if (DEEPCHECK) console.log('running in checked mode')

export type View = Database // I'll probably add more fields here eventually.

export type DocUpdate = {id: DocId, vals: RemoteValue}
export type OnChange = (view: View, newBranch: RemoteVersion[], ops: DocUpdate[]) => void

export interface Db {
  ops: Database, // All operations ever.
  views: {[k: string]: Database},

  _root: RootDatabase

  onChange?: OnChange
}

const assertType = (db: Database, type: string) => {
  const actualType = db.get(typeKey)
  if (actualType == null) db.putSync(typeKey, type)
  else assert.strictEqual(actualType, type)
}

export function openChild(db: Db, name: string, type: string): Database {
  const child = db._root.openDB(name, {keyIsBuffer: true})
  assertType(child, type)
  return child
}

export interface DbOptions {
  onChange?: OnChange
}

export function open(opts: DbOptions = {}): Db {
  const root = lmdbOpen({
    path: 'store',
    keyIsBuffer: true,
  })

  // Two databases - one with all the operations:
  const ops = root.openDB('ops', {keyIsBuffer: true})
  assertType(ops, 'ops')
  // And another with a snapshot of the data at some point in time:
  // const data = root.openDB('data', {keyIsBuffer: true})

  return {
    ops,
    views: {},
    _root: root,
    onChange: opts.onChange
  }
}

export function openView(db: Db, viewName: string): Database {
  if (db.views[viewName] != null) return db.views[viewName]
  else {
    assert(viewName != 'ops', 'Reserved view name')

    const view = openChild(db, viewName, 'view')
    db.views[viewName] = view
    return view
  }
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
const getDocKey = (id: DocId) => pack(['doc', ...id])
const branchKey = pack('branch') // Contains a list of orders.
const typeKey = pack('db type')

// Get the highest known sequence number for the specified agent. Returns -1 if none found.
export function getMaxSeq(db: Db, agent: string): number | null {
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

// This is just used so frequently.
export function getRemoteVersion(db: Db, order: number): RemoteVersion {
  return order === -1 ? ROOT_VERSION : getOperation(db, order).version
}

const idEq = (a: DocId, b: DocId) => (
  a.length === b.length && a.every((val, i) => b[i] === val)
)

const branchContainsVersion = (db: Db, target: number, branch: number[], atId?: DocId): boolean => {
  // TODO: Might be worth checking if the target version has the same agent id
  // as one of the items in the branch and short circuiting if so.

  if (DEEPCHECK && atId != null) {
    // When we're in document mode, all operations named in the branch must
    // contain an operation modifying the document ID.
    for (const v of branch) {
      const op = getOperation(db, v)
      assert(op.docOps.find(({id}) => idEq(id, atId)) != null)
    }
  }

  // Order matters between these two lines because of how this is used in applyBackwards.
  if (branch.length === 0) return false
  if (target === -1 || branch.indexOf(target) >= 0) return true

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

    if (atId == null) {
      // We're using operation versions.
      queue.push(...op.parents) // TODO: Could sort in descending order.

      // Ordered so we hit this next. This isn't necessary, the succeeds field
      // will often be smaller than the parents.
      if (op.succeeds > -1 && op.parents[0] != op.succeeds) {
        queue.push(op.succeeds)
      }
    } else {
      // We only care about the operations which modified this key. We'll skip a
      // lot of operations this way.
      const docOp = op.docOps.find(({id: key}) => idEq(key, atId))
      assert(docOp != null)
      queue.push(...docOp.parents)
    }
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
    docOps: op.docOps.map(({id, parents, opData}): LocalDocOp => ({
      id, opData, parents: parents.map(version => getOrder(db, version))
    })),
    parents: op.parents.map(v => getOrder(db, v)),
    succeeds: op.succeedsSeq === -1 ? -1
    : getOrder(db, {agent: op.version.agent, seq: op.succeedsSeq})
  }

  // And save the new operation in the store. TODO: Do this in a transaction?
  // The transaction could be issued from our parent, but I'm not sure if thats
  // a good idea.
  db.ops.put(getOpKey(newOrder), localOp)
  db.ops.put(getOrderForVersionKey(op.version), newOrder)

  // We also store the frontier in the ops store for easy syncing. But this only moves forward!
  const newBranch = advanceBranchByOp(db, getBranch(db.ops), newOrder, localOp)
  db.ops.put(branchKey, newBranch)

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




export function getVal(view: View, key: DocId): DocValue {
  // Every document implicitly exists, but with a value of null.
  return (view.get(getDocKey(key)) as DocValue | null) ?? [{
    order: -1,
    value: null
  }]
}

export function getRemoteVal(db: Db, view: View, key: DocId): RemoteValue {
  const val = getVal(view, key)
  return val.map(({order, value}) => ({
    version: getRemoteVersion(db, order),
    value
  }))
}

export function getBranch(view: View): number[] {
  return (view.get(branchKey) as number[]) ?? [-1]
}

export function branchAsRemoteVersions(db: Db, branch: number[]): RemoteVersion[] {
  return branch.map(order => getRemoteVersion(db, order))
}

const advanceBranchByOp = (db: Db, branch: number[], order: number, op: LocalOpWithoutOrder) => {
  // Check the operation fits. The operation should not be in the branch, but
  // all the operation's parents should be.
  assert(!branchContainsVersion(db, order, branch), 'db already contains version')
  for (const parent of op.parents) {
    assert(branchContainsVersion(db, parent, branch), 'operation in the future')
  }

  // Every version named in branch is either:
  // - Equal to a branch in the new operation's parents (in which case remove it)
  // - Or newer than a branch in the operation's parents (in which case keep it)
  // If there were any versions which are older, we would have aborted above.
  return [order,
    ... branch.filter(o => !op.parents.includes(o))
  ]
}

// Returns new branch.
export function applyForwards(db: Db, view: View, order: number): number[] {
  // This method is often called after addOperation - in which case it might
  // make more sense to take the LocalOperation itself as an argument.
  const op = getOperation(db, order)

  const newBranch = advanceBranchByOp(db, getBranch(view), order, op)
  // This feels a little fragile because I don't know how lmdb-store handles
  // exceptions during a transaction.
  view.put(branchKey, newBranch)

  const docUpdates: DocUpdate[] = []

  for (const {id, parents, opData} of op.docOps) {
    const prevVals = getVal(view, id)

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
        const docBranch = prevVals.map(({order}) => order)
        assert(branchContainsVersion(db, p, docBranch, id))
      }
    }

    const newVals: DocValue = [{order, value: opData}]
    for (const oldEntry of prevVals) {
      if (!parents.includes(oldEntry.order)) {
        // Keep this.
        newVals.push(oldEntry)
      }
    }

    // If there's multiple conflicting values, we keep them in version-sorted
    // order to make db comparisons easier in the fuzzer.
    newVals.sort((a, b) => a.order - b.order)
    view.put(getDocKey(id), newVals)

    // For update notifications
    docUpdates.push({
      id, vals: newVals.map(({order, value}) => ({value, version: getRemoteVersion(db, order)}))
    })
  }

  if (db.onChange) {
    db.onChange(view, branchAsRemoteVersions(db, newBranch), docUpdates)
  }

  return newBranch
}

export function applyBackwards(db: Db, view: View, order: number): number[] {
  const op = getOperation(db, order)
  const branch = getBranch(view)

  // Remove the operation from the frontier.
  const idx = branch.indexOf(order)
  assert(idx >= 0, 'Can only remove versions from the frontier')
  branch.splice(idx, 1) // Could do a bag trim, but eh.

  // And add operations from the parents back.

  for (const parent of op.parents) {
    console.log('bcv', parent, branch, branchContainsVersion(db, parent, branch))
    if (!branchContainsVersion(db, parent, branch)) {
      branch.push(parent)
    }
  }

  view.put(branchKey, branch)

  const docUpdates: DocUpdate[] = []

  // And update the data.
  for (const {id, parents} of op.docOps) {
    const prevVals = getVal(view, id)

    // The values should instead contain:
    // - Everything in prevVals not including op.version
    // - All the objects named in parents that aren't superceded by another
    //   document version

    // Remove this operation's contribution to the value
    const newVals = prevVals.filter(({order: docOrder}) => order != docOrder)
    const docBranch = newVals.map(({order}) => order)

    // And add back all the parents that aren't dominated by another value already.
    for (const p of parents) {
      // TODO: Assert p not already represented in prevVals, which would be invalid.

      // If p is dominated by a value in prevVals, skip.
      if (branchContainsVersion(db, p, docBranch, id)) continue
      // const dominated = newVals.map(({version}) => compareVersions(db, p, version))
      // if (dominated.some(x => x < 0)) continue

      if (p === -1) {
        // If all we have is the root, we'll delete the key.
        assert(newVals.length === 0)
      } else {
        const parentOp = getOperation(db, p)
        const parentDocOp = parentOp.docOps.find(({id: id2}) => idEq(id, id2))
        assert(parentDocOp)
        newVals.push({order: p, value: parentDocOp.opData})
      }
    }

    console.log('newVals', newVals)
    // This is a bit dirty, to handle the root.
    if (newVals.length === 0) view.remove(getDocKey(id))
    else view.put(getDocKey(id), newVals)

    // For update notifications
    docUpdates.push({
      id, vals: newVals.map(({order, value}) => ({value, version: getRemoteVersion(db, order)}))
    })
  }

  if (db.onChange) {
    db.onChange(view, branchAsRemoteVersions(db, branch), docUpdates)
  }

  return branch
}

const test = async () => {
  const db = open()
  const view = openView(db, 'stuff')
  console.log('branch', getBranch(view))
  console.log('ops branch', getBranch(db.ops))

  const order = await db.ops.transaction(() => {
    const order = addOperation(db, {
      version: {
        agent: 'hi',
        seq: 0
      },
      parents: [ROOT_VERSION],
      succeedsSeq: -1,
      docOps: [{
        id: ['posts', 'yo'],
        parents: [ROOT_VERSION],
        opData: {
          title: 'heeey',
        }
      }],
    })

    applyForwards(db, view, order)
    return order
  })
  checkOps(db)
  console.log('branch', getBranch(view))
  console.log(getVal(view, ['posts', 'yo']))

  await view.transaction(() => {
    applyBackwards(db, view, order)
  })

  checkOps(db)
  console.log('branch', getBranch(view))
  console.log(getVal(view, ['posts', 'yo']))
  console.log('ops branch', getBranch(db.ops))
}

if (require.main === module) {
  test()
}