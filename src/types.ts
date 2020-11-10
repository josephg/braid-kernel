
export interface RemoteVersion {
  agentHash: string // Hash or something
  seq: number
}

export interface LocalVersion {
  agent: number,
  seq: number
}

export interface RemoteValue {
  version: RemoteVersion
  value: any,
}

export interface LocalValue {
  version: LocalVersion // Operation which last modified this key
  value: any,
}

// Alright - some fixed agent IDs:
// -1: Transient (these IDs will never appear
//  0: Root - the only valid sequence number is (0,0).
//  1: This nodejs process, though this should be fetched from the variable

export const ROOT_VERSION: LocalVersion = {
  agent: 0, seq: 0
}

export const NULL_VALUE: LocalValue = {
  version: ROOT_VERSION, value: null
}



export interface SchemaInfo {
  // Type
  // Conflict behaviour
  // Is this writable?
  // Is this a computed index or view?
  // ...
}
