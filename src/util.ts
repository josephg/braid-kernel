// Key increment. Find the next string (well, buffer) after this buffer.

import { TupleItem, unpack } from "fdb-tuple";
import { Database } from "lmdb-store";

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

export const getLastKey = (db: Database, prefix: Buffer): TupleItem[] | null => {
  const iter = db.getRange({
    start: keyInc(prefix),
    end: prefix,
    reverse: true,
    limit: 1,
    values: false,
  })[Symbol.iterator]()
  const entry = iter.next().value
  // console.log('prefix', prefix, 'entry', entry)

  return entry ? unpack(entry) : null
}
