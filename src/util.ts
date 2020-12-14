// Key increment. Find the next string (well, buffer) after this buffer.

import { TupleItem, pack, unpack } from "fdb-tuple";
import { Database } from "lmdb-store";
import {RemoteValue, RemoteVersion} from './types'
// import { LocalVersion, RemoteVersion, ROOT_VERSION } from "./types"

// Stolen from node-foundationdb.
export const keyInc = (val: string | Buffer): Buffer => {
  const buf = typeof val === 'string' ? Buffer.from(val) : val

  let lastNonFFByte
  for(lastNonFFByte = buf.length-1; lastNonFFByte >= 0; --lastNonFFByte) {
    if(buf[lastNonFFByte] != 0xFF) break;
  }

  if(lastNonFFByte < 0) {
    throw new Error(`invalid argument '${val}': prefix must have at least one byte not equal to 0xFF`)
  }

  const result = Buffer.alloc(lastNonFFByte + 1)
  buf.copy(result, 0, 0, result.length)
  ++result[lastNonFFByte]

  return result;
}

// TODO: This might have a bug where if keyInc(prefix) is also a valid key, we
// return it.
export const getLastKey = (db: Database, prefix: Buffer): TupleItem[] | null => {
  const iter = db.getKeys({
    start: keyInc(prefix),
    end: prefix,
    reverse: true,
    limit: 1,
  })[Symbol.iterator]()
  const entry = iter.next().value
  // console.log('prefix', prefix, 'entry', entry)

  return entry ? unpack(entry) : null
}

// const ROOT_AGENT = ROOT_VERSION.agent
// export const cmpVersions = (_db: Database, a: LocalVersion, b: LocalVersion): number => {
//   if (a.agent !== ROOT_AGENT && b.agent !== ROOT_AGENT && a.agent !== b.agent) {
//     throw Error('Not implemented')
//   }

//   return a.agent === b.agent ? a.seq - b.seq
//     : a.agent === ROOT_AGENT ? -1
//     : b.agent === ROOT_AGENT ? 1
//     : 0 // Unreachable.
// }

export const encodeVersion = (version: RemoteVersion): string => (
  pack([version.agent, version.seq]).toString('base64')
)

// This is used to break ties between versions.
export const vCmp = (a: RemoteVersion, b: RemoteVersion) => (
  a.agent.localeCompare(b.agent, 'en-US') || (a.seq - b.seq)
)

export const encodeBranch = (version: RemoteVersion[]): string => (
  pack(version.sort(vCmp).map(v => [v.agent, v.seq])).toString('base64')
)

export const splitAndEncode = (val: RemoteValue) => ({
  version: encodeBranch(val.map(({version}) => version)),
  data: val.map(({value}) => value) // List of concurrent values
})
