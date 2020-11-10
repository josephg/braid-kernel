import crypto from 'crypto'
import {getLastKey} from './util'
import {Database} from 'lmdb-store'
import {pack, unpack} from 'fdb-tuple'
import { LocalVersion, RemoteVersion } from './types'

export const newAgentName = (): string => (
  crypto.randomBytes(6).toString('base64') // Might need to be longer later.
  // crypto.randomBytes(12).toString('base64')
)

// const agentNumToHash = new Map()
// const agentHashToNum = new Map()

const hashforIdKey = (id: number) => pack(['_agent', 'hashof', id])
const idForHashKey = (hash: string) => pack(['_agent', 'idof', hash])

export const getAgentHash = (db: Database, id: number) => {
  const hash = db.get(hashforIdKey(id))
  if (hash == null) throw Error('Could not find agent hash for id ' + id)
  return hash as string
}

export const getAgentId = (db: Database, hash: string): number | undefined => (
  db.get(idForHashKey(hash))
)

export const localToRemoteVersion = (db: Database, v: LocalVersion): RemoteVersion => ({
  agentHash: getAgentHash(db, v.agent),
  seq: v.seq
})

export const getOrCreateAgentId = (db: Database, hash: string): number => (
  db.transaction(() => {
    let id = getAgentId(db, hash)
    if (id == null) {
      // Make a new ID and return it.
      const prefix = pack(['_agent', 'hashof'])
      const key = getLastKey(db, prefix)
      if (key == null) {
        console.log('There are no existing agents defined')
        id = 1
      } else {
        console.log('entry', key)
        // const lastId = unpack(entry.key)
        id = key[key.length - 1] as number + 1
        console.log('made new id number', id)
      }
      // We only need to look at the first item to find out the last id in use.
      // iter.forEach(({key, value}) => console.log(unpack(key as Buffer), value))
      // console.log('iter', Array.from(iter))

      db.putSync(idForHashKey(hash), id)
      db.putSync(hashforIdKey(id), hash)
    }
    return id
  })
)

// export const getOrCreateAgentId = (db: Database, hash: string): number => (
//   db.transaction(() => {
//     let id = getAgentId(db, hash)
//     if (id == null) {
//       // Make a new ID and return it.
//       const k = pack(['_agent', 'hashof'])
//       const iter = db.getRange({
//         start: keyInc(k),
//         end: k,
//         reverse: true,
//         limit: 1,
//         values: false,
//       })[Symbol.iterator]()
//       const entry = iter.next().value
//       if (entry == null) {
//         console.log('There are no existing agents defined')
//         id = 1
//       } else {
//         const key = unpack(entry.key)
//         console.log('entry', entry, key)
//         // const lastId = unpack(entry.key)
//         id = key[key.length - 1] as number + 1
//         console.log('made new id number', id)
//       }
//       // We only need to look at the first item to find out the last id in use.
//       // iter.forEach(({key, value}) => console.log(unpack(key as Buffer), value))
//       // console.log('iter', Array.from(iter))

//       db.putSync(idForHashKey(hash), id)
//       db.putSync(hashforIdKey(id), hash)
//     }
//     return id
//   })
// )
// export const getOrCreateAgentId = (db: Database, hash: string): Promise<number> => (
//   db.transaction(async () => {
//     let id = getAgentId(db, hash)
//     if (id == null) {
//       // Make a new ID and return it.
//       const iter = db.getRange({
//         start: keyInc(pack(['_agent', 'hashof'])),
//         end: pack(['_agent', 'hashof']),
//         reverse: true,
//         limit: 1,
//         values: false,
//       })[Symbol.iterator]()
//       const entry = iter.next().value
//       if (entry == null) {
//         console.log('There are no existing agents defined')
//         id = 1
//       } else {
//         const key = unpack(entry.key)
//         console.log('entry', entry, key)
//         // const lastId = unpack(entry.key)
//         id = key[key.length - 1] as number + 1
//         console.log('made new id number', id)
//       }
//       // We only need to look at the first item to find out the last id in use.
//       // iter.forEach(({key, value}) => console.log(unpack(key as Buffer), value))
//       // console.log('iter', Array.from(iter))

//       await db.put(pack(['_agent', 'idof', hash]), id)
//       await db.put(pack(['_agent', 'hashof', id]), hash)
//     }
//     return id
//   })
// )