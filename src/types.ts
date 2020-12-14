
export interface RemoteVersion {
  agent: string,
  seq: number
}

// Internally versions are named with a number.

export interface RemoteOperation {
  version: RemoteVersion,

  /**
   * Usually `version.seq - 1`. This allows sparse versions. -1 for the first operation from an agent.
   */
  succeedsSeq: number,

  /**
   * This names all the direct, non-transitive dependancies. Note this may or
   * may not include the previous operation from this agent, depending on
   * concurrency.
   */
  parents: RemoteVersion[],

  // Unordered set.
  docOps: DocOperation[]
}

export interface LocalOperation {
  order: number, // Might be always inferrable.
  version: RemoteVersion,
  parents: number[],
  docOps: DocOperation[],

  // Order of previous version from this agent. Not sure if this is necessary.
  // -1 if none.
  succeeds: number,
}

export type DocId = {
  collection: string,
  key: string,
}

export interface DocOperation {
  id: DocId,
  parents: number[], // Specific to the document.
  opData: any, // This is usually the new value.
}

export type DocValueStoreAll = {
  order: number,
  value: any
}[]
export type DocValue = DocValueStoreAll // | DocValueLWW | ...



// Alright - some fixed agent IDs:
// -1: Transient (these IDs will never appear
//  0: Root - the only valid sequence number is (0,0).
//  1: This nodejs process, though this should be fetched from the variable

export const ROOT_VERSION: RemoteVersion = {
  agent: 'ROOT', seq: 0
}

// export const NULL_VALUE: LocalValue = {
//   version: ROOT_VERSION, value: null
// }



// export interface SchemaInfo {
//   // Type
//   // Conflict behaviour
//   // Is this writable?
//   // Is this a computed index or view?
//   // ...
// }
