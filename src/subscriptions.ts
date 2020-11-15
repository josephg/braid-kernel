import asyncstream, { Stream } from 'ministreamiterator'
import { getAgentHash, getOrCreateAgentId, localToRemoteValue, localToRemoteVersion, newAgentName } from './agent'
import {SchemaInfo, LocalValue, LocalVersion, NULL_VALUE, RemoteVersion, RemoteValue} from './types'
import { IncomingMessage, ServerResponse } from 'http'

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

export const notifySubscriptions = (parts: string[], value: RemoteValue) => {
  for (const c of getStreamsForDoc(parts)) {
    c.stream.append(JSON.stringify(value))
  }
}

const tryFlush = (res: ServerResponse) => {
  ;(res as any).flush && (res as any).flush()
}

export const getSSE = async (req: IncomingMessage, res: ServerResponse, parts: string[], initialData: RemoteValue) => {
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
  stream.append(JSON.stringify(initialData))

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