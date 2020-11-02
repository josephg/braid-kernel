import polka from 'polka'
import {open} from 'lmdb-store'
import {pack, unpack} from 'fdb-tuple'

const PORT = process.env.PORT || 4040

const db = open({
  path: 'store',
  keyIsBuffer: true,
})

polka()
.listen(PORT, (err?: Error) => {
  if (err) throw err
  console.log(`running on localhost:${PORT}`)
})