import polka from 'polka'
import {open} from 'lmdb-store'
import {pack, unpack} from 'fdb-tuple'
import { getOrCreateAgentId, newAgentName } from './agent'
import {SchemaInfo, RawValue, LocalVersion, NULL_VALUE} from './types'
import bodyParser from 'body-parser'
import { ServerResponse } from 'http'
import {keyInc, getLastKey} from './util'

const PORT = process.env.PORT || 4040

const db = open({
  path: 'store',
  keyIsBuffer: true,
})

// Internal data:
// - Set of schemas
// - Agent info - hash <-> number mapping set and last seen versions

// interface Operation {
//   new_value: any,
// }

// interface WriteTransaction {

// }


// const applyTxn = async (txn: WriteTransaction) => {

// }

const collections = new Map<string, SchemaInfo>([
  ['authors', {}],
  ['posts', {}],
  ['slugs', {}],
])

// The local transient version should never be saved in the database.
export const transient_version: LocalVersion = {
  agent: -1, seq: 0
}

let localAgentHash = db.get(pack('_localagent'))
if (localAgentHash == null) {
  localAgentHash = newAgentName()
  console.log('made new local agent hash', localAgentHash)
  db.put(pack('_localagent'), localAgentHash)
}
let localAgent = getOrCreateAgentId(db, localAgentHash)

console.log('localagent', localAgent, localAgentHash)

const opKey = (agent: number, seq?: number): Buffer => (
  pack(seq == null ? ['_op', agent] : ['_op', agent, seq])
)

const docKey = (collection: string, key: string): Buffer => pack([collection, key])


const urlToParts = (urlpath: string): string[] => {
  // if (keypath[keypath.length-1] === '/') throw Error('Cannot use raw folder as path')

  const parts = urlpath.split('/')
  if (parts[0] === '') parts.shift() // Drop leading /
  if (parts[parts.length-1] === '') parts.pop() // Drop trailing /

  return parts
}

const getDb = (parts: string[]): RawValue | null => {
  if (parts.length !== 2) return null // always /collection/key

  const [collectionName, key] = parts
  const schema = collections.get(collectionName)
  if (schema == null) return null

  // lmdb-store can fetch data synchronously. I'll want to relax this at some
  // point but its dang convenient.

  // Also at some point use the fdb subspace code to compact subspace prefixes
  return db.get(docKey(collectionName, key)) as RawValue ?? NULL_VALUE
}

const getLastVersion = (id: number): number => {
  const lastKey = getLastKey(db, opKey(id))
  // console.log('lastkey', lastKey)
  return lastKey == null ? -1 : lastKey[lastKey.length - 1] as number
}

const putDb = (parts: string[], docValue: any): Promise<LocalVersion> | null => {
  if (parts.length !== 2) return null
  const [collectionName, key] = parts
  const schema = collections.get(collectionName)
  if (schema == null) return null

  return db.transaction(async () => {
    const newSeq = getLastVersion(localAgent) + 1
    const version: LocalVersion = {
      agent: localAgent,
      seq: newSeq
    }
    const value: RawValue = {
      value: docValue,
      version
    }

    const dbKey = docKey(collectionName, key)
    const oldVersion = db.get(dbKey) ?? NULL_VALUE
    await db.put(dbKey, value)
    // console.log('put', opKey(localAgent, newSeq))
    await db.put(opKey(localAgent, newSeq), [
      // The transaction is a list of written values.
      {
        collection: collectionName,
        key,
        replaces: oldVersion,
        value: docValue,
      }
      // Do we need anything else here? Probably...
    ])

    return version
  })
}

const getInternal = (parts: string[]): RawValue | null => {
  // console.log('internal', parts)
  switch (parts[0]) {
    case '_status': return { version: transient_version, value: 'ok' }
    default: return NULL_VALUE
  }
}

const notFound = (res: ServerResponse) => {
  res.statusCode = 404
  res.end('Not found')
}

const app = polka()

app.use('/raw', (req, res, next) => {
  // const path = req.path.slice(1) // Slice off the 
  const parts = urlToParts(req.path)
  if (parts.length < 1) return notFound(res)

  ;(req as any).parts = parts
  next()
})

app.get('/raw/*', (req, res) => {
  const parts = (req as any).parts
  console.log('get parts', parts)
  const data = parts[0][0] === '_' ? getInternal(parts) : getDb(parts)
  console.log('data', data)
  if (data == null) return notFound(res)

  res.setHeader('content-type', 'application/json')
  // res.setHeader('etag', 'application/json')

  // TODO: Convert agent to hash.
  res.setHeader('x-version', JSON.stringify([data.version.agent, data.version.seq]))
  res.write(JSON.stringify(data))
  // res.write(JSON.stringify(data.value))
  res.end()
})

app.put('/raw/*', bodyParser.json(), async (req, res) => {
  const parts = (req as any).parts
  if (parts[0][0] === '_') return notFound(res)

  // For now we'll use the local version, but clients should also be able to
  // set their own version.
  //
  // And clients should be able to specify the version that this document is
  // replacing - to trigger conflict behaviour.
  const versionP = putDb(parts, req.body)
  if (versionP == null) return notFound(res)

  const version = await versionP
  res.setHeader('x-version', JSON.stringify([version.agent, version.seq]))
  
  // TEMPORARY.
  res.setHeader('content-type', 'application/json')
  res.write(JSON.stringify(version))
  
  res.end()
})

app.listen(PORT, (err?: Error) => {
  if (err) throw err
  console.log(`running on localhost:${PORT}`)
})