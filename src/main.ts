import polka from 'polka'
import {open} from 'lmdb-store'
import {pack, unpack} from 'fdb-tuple'
import { getAgentHash, getOrCreateAgentId, localToRemoteValue, localToRemoteVersion, newAgentName } from './agent'
import {SchemaInfo, LocalValue, LocalVersion, NULL_VALUE, RemoteVersion} from './types'
import bodyParser from 'body-parser'
import { IncomingMessage, ServerResponse } from 'http'
import {keyInc, getLastKey} from './util'
import fresh from 'fresh'
import asyncstream, { Stream } from 'ministreamiterator'
import compress from 'compression'
import sirv from 'sirv'

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

const getDb = (parts: string[]): LocalValue | null => {
  if (parts.length !== 2) return null // always /collection/key

  const [collectionName, key] = parts
  const schema = collections.get(collectionName)
  if (schema == null) return null

  // lmdb-store can fetch data synchronously. I'll want to relax this at some
  // point but its dang convenient.

  // Also at some point use the fdb subspace code to compact subspace prefixes
  return db.get(docKey(collectionName, key)) as LocalValue ?? NULL_VALUE
}

const getLastVersion = (id: number): number => {
  const lastKey = getLastKey(db, opKey(id))
  // console.log('lastkey', lastKey)
  return lastKey == null ? -1 : lastKey[lastKey.length - 1] as number
}

interface StreamingClient {
  stream: Stream<any>
}
const streamsForDocs = new Map<string, Set<StreamingClient>>()

const getStreamsForDoc = (parts: string[]): Set<StreamingClient> => {
  const k = parts.join('/')
  let set = streamsForDocs.get(k)
  if (set == null) {
    set = new Set()
    streamsForDocs.set(k, set)
  }
  return set
}

const putDb = (parts: string[], docValue: any): Promise<LocalVersion> | null => {
  // TODO: Handle multiple objects being set at once
  // TODO: Handle conflicts
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
    const value: LocalValue = {
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

    // Notify listeners
    for (const c of getStreamsForDoc(parts)) {
      c.stream.append(JSON.stringify(localToRemoteValue(db, value)))
    }

    return version
  })
}

const getInternal = (parts: string[]): LocalValue | null => {
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

const tryFlush = (res: ServerResponse) => {
  ;(res as any).flush && (res as any).flush()
}

const getSSE = async (req: IncomingMessage, res: ServerResponse, parts: string[], data: LocalValue) => {
  console.log('get sse')
  // There's 3 cases here:
  // - The client did not request a version. Send the document then stream updates.
  // - The client requested an old version.
  //   - Send all updates since that version if we can
  //   - Or just send a snapshot
  // - The client requested the current version. Tell them they're up to date and stream.

  res.writeHead(200, 'OK', {
    'Cache-Control': 'no-cache',
    'Content-Type': 'text/event-stream',
    'Connection': 'keep-alive'
  })

  // Tell the client to retry every second if connectivity is lost
  res.write('retry: 3000\n\n');

  let connected = true
  // const r = get_room(room)
  const stream = asyncstream()
  const client = {
    stream,
  }
  const docStreams = getStreamsForDoc(parts)
  docStreams.add(client)

  // For now we'll just start with a snapshot.
  stream.append(JSON.stringify(localToRemoteValue(db, data)))

  res.once('close', () => {
    console.log('Closed connection to client for doc', parts)
    connected = false
    stream.end()
    docStreams.delete(client)
  })

  ;(async () => {
    // 30 second heartbeats to avoid timeouts
    while (true) {
      await new Promise(res => setTimeout(res, 30*1000))

      if (!connected) break
      
      // res.write(`event: heartbeat\ndata: \n\n`);
      res.write(`data: {}\n\n`)
      tryFlush(res)
    }
  })()

  while (connected) {
    // await new Promise(resolve => setTimeout(resolve, 1000));

    // console.log('Emit', ++count);
    // Emit an SSE that contains the current 'count' as a string
    // res.write(`event: message\r\ndata: ${count}\r\n\r\n`);
    // res.write(`data: ${count}\nid: ${count}\n\n`);
    for await (const val of stream.iter) {
      // console.log('sending val', val)
      res.write(`data: ${val}\n\n`)
      tryFlush(res)
    }
  }
}

const app = polka()

app.use(compress(), sirv(__dirname + '/../public', {
  // maxAge: 31536000, // 1Y
  // immutable: true
  dev: process.env.NODE_ENV !== 'production'
}))

app.use('/raw', (req, res, next) => {
  // const path = req.path.slice(1) // Slice off the 
  const parts = urlToParts(req.path)
  if (parts.length < 1) return notFound(res)

  ;(req as any).parts = parts
  next()
})

const encodeVersion = (version: RemoteVersion): string => (
  pack([version.agentHash, version.seq]).toString('base64')
)

app.get('/raw/*', (req, res) => {
  const parts = (req as any).parts
  // console.log('get parts', parts)
  const data = parts[0][0] === '_' ? getInternal(parts) : getDb(parts)

  if (req.headers['accept'] === 'text/event-stream') return getSSE(req, res, parts, data ?? NULL_VALUE)
  // console.log('data', data)
  if (data == null) return notFound(res)
  
  const remoteVersion = localToRemoteVersion(db, data.version)
  const resHeaders = {
    'content-type': 'application/json',
    etag: encodeVersion(remoteVersion),
    'x-version': encodeVersion(remoteVersion),
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
  const parts = (req as any).parts
  if (parts[0][0] === '_') return notFound(res)

  // For now we'll use the local version, but clients should also be able to
  // set their own version.
  //
  // And clients should be able to specify the version that this document is
  // replacing - to trigger conflict behaviour.
  const versionP = putDb(parts, req.body)
  if (versionP == null) return notFound(res)

  const localVersion = await versionP
  const remoteVersion = localToRemoteVersion(db, localVersion)
  res.setHeader('x-version', encodeVersion(remoteVersion))
  
  // TEMPORARY. I'm not actually sure what data to return in the body here.
  // Maybe the version is as good as any.
  res.setHeader('content-type', 'application/json')
  res.write(JSON.stringify(remoteVersion))
  
  res.end()
})



app.listen(PORT, (err?: Error) => {
  if (err) throw err
  console.log(`running on localhost:${PORT}`)
})