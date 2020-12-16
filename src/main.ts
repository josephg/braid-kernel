import polka from 'polka'
import {pack, unpack} from 'fdb-tuple'
import { DocId, LocalOperation, RemoteOperation, RemoteValue, RemoteVersion, SchemaInfo } from './types'
import bodyParser from 'body-parser'
import {keyInc, getLastKey, encodeVersion, splitAndEncode, encodeBranch} from './util'
import fresh from 'fresh'
import compress from 'compression'
import sirv from 'sirv'
import { IncomingMessage, ServerResponse } from 'http'
import { getSSE, notifySubscriptions } from './subscriptions'
import { addOperation, applyForwards, branchAsRemoteVersions, getBranch, getMaxSeq, getOrder, getRemoteVal, getVal, open, openChild, openView } from './db'
import { newAgentName } from './agent'

const PORT = process.env.PORT || 4040

// Internal data:
// - Set of schemas
// - Agent info - hash <-> number mapping set and last seen versions


const collections = new Map<string, SchemaInfo>([
  ['authors', {
    // types
    // configuration for change tracking
    // conflict resolution
  }],
  ['posts', {}],
  ['slugs', {}],
])

// The local transient version should never be saved in the database.
// export const transient_version: LocalVersion = {
//   agent: -1, seq: 0
// }

const db = open({
  onChange(view, branch, ops) {
    console.log('onchange now in branch', branch)
    for (const {id, vals} of ops) {
      notifySubscriptions(id, vals)
    }

    // Super gross. Figure out a better way to do this.
    notifySubscriptions(['_branch'], [{value: branch, version: branch[0]}])
  }
})
const localInfoDb = openChild(db, 'local', 'local')
const view = openView(db, 'stuff')


let localAgentHash = localInfoDb.get(pack('_localagent'))
if (localAgentHash == null) {
  localAgentHash = newAgentName()
  console.log('made new local agent hash', localAgentHash)
  localInfoDb.put(pack('_localagent'), localAgentHash)
}
// let localAgent = getOrCreateAgentId(localInfoDb, localAgentHash)

// console.log('localagent', localAgent, localAgentHash)


const urlToParts = (urlpath: string): string[] => {
  // if (keypath[keypath.length-1] === '/') throw Error('Cannot use raw folder as path')

  const parts = urlpath.split('/')
  if (parts[0] === '') parts.shift() // Drop leading /
  if (parts[parts.length-1] === '') parts.pop() // Drop trailing /

  return parts
}

const getDb = (id: DocId): RemoteValue => {
  // lmdb-store can fetch data synchronously. I'll want to relax this at some
  // point but its dang convenient.

  // Also at some point use the fdb subspace code to compact subspace prefixes
  // return db.get(docKey(collectionName, key)) as LocalValue ?? NULL_VALUE
  return getRemoteVal(db, view, id)
}

const putDb = async (id: DocId, docValue: any, docParents?: RemoteVersion[]): Promise<RemoteVersion> => {
  return await db.ops.transaction(() => {
    const oldVersion = getMaxSeq(db, localAgentHash) ?? -1
    const version: RemoteVersion = {
      agent: localAgentHash,
      seq: oldVersion + 1
    }
    const branch = getBranch(view)

    if (docParents == null) {
      docParents = getRemoteVal(db, view, id).map(({version}) => version)
    }

    const op: RemoteOperation = {
      version,
      parents: branchAsRemoteVersions(db, branch),
      succeedsSeq: oldVersion,
      docOps: [{
        id: id,
        opData: docValue,
        parents: docParents,
      }]
    }

    const order = addOperation(db, op)
    applyForwards(db, view, order)
    return version
  })
}

// const getInternal = (parts: string[]): LocalValue | null => {
//   // console.log('internal', parts)
//   switch (parts[0]) {
//     case '_status': return { version: transient_version, value: 'ok' }
//     default: return NULL_VALUE
//   }
// }

const notFound = (res: ServerResponse) => {
  res.statusCode = 404
  res.end('Not found')
}


const app = polka()

app.use(compress(), sirv(__dirname + '/../public', {
  // maxAge: 31536000, // 1Y
  // immutable: true
  dev: process.env.NODE_ENV !== 'production'
}))

app.use('/raw', (req, res, next) => {
  // const path = req.path.slice(1) // Slice off the 
  const docId = urlToParts(req.path)
  if (docId.length < 1) return notFound(res)

  ;(req as any).docId = docId
  next()
})

app.get('/raw/*', (req, res) => {
  const docId = (req as any).docId
  // console.log('get docId', docId)
  // const data = docId[0][0] === '_' ? getInternal(docId) : getDb(docId)
  const remoteValue = getDb(docId)

  if (req.headers['accept'] === 'text/event-stream') return getSSE(req, res, docId, remoteValue)
  // console.log('data', data)
  // if (data == null) return notFound(res)

  // const remoteVersion = localToRemoteVersion(db, data.version)

  const {data, version} = splitAndEncode(remoteValue)

  const resHeaders = {
    'content-type': 'application/json',
    etag: version,
    'x-version': version,
  }
  if (fresh(req.headers, resHeaders)) {
    res.writeHead(304, resHeaders)
    res.end()
  } else {
    res.writeHead(200, resHeaders)
    // res.write(JSON.stringify(data.value))
    res.end(JSON.stringify(data))
  }
})

app.put('/raw/*', bodyParser.json(), async (req, res) => {
  const docId = (req as any).docId as DocId
  // if (parts[0][0] === '_') return notFound(res)

  // For now we'll use the local version, but clients should also be able to
  // set their own version.
  //
  // And clients should be able to specify the version that this document is
  // replacing - to trigger conflict behaviour.
  const versionP = putDb(docId, req.body)
  if (versionP == null) return notFound(res)

  const remoteVersion = await versionP
  res.setHeader('x-version', encodeVersion(remoteVersion))

  // TEMPORARY. I'm not actually sure what data to return in the body here.
  // Maybe the version is as good as any.
  res.setHeader('content-type', 'application/json')
  res.write(JSON.stringify(remoteVersion))

  res.end()
})

// app.post('/setversion'

app.listen(PORT, (err?: Error) => {
  if (err) throw err
  console.log(`running on localhost:${PORT}`)
})