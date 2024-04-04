# Ouroboros

Define derived indexes on autobase hyperbees.

This is done by creating an api similar to SimpleAutobee to support hyperbee
operations w/ autobase, but also providing simple ways to watch key ranges and
derive values base on changes to those ranges as well.

The ability for indexes to update based on themselves or other indexes is
likened to eating its own tail, hence Ouroboros. Be wary of infinite loops.

## Usage

```js
import Autobase from 'autobase'
import Corestore from 'corestore'
import b4a from 'b4a'
import { wrap, apply, createIndex } from '@lejeunerenard/ouroboros'

const store = new Corestore('./db')
const bootstrap = null
const open = (viewStore) => {
  const core = viewStore.get('ouroboros')
  return new Hyperbee(core, {
    keyEncoding: 'utf-8',
    valueEncoding: 'json',
    extension: false
  })
}
const base = wrap(new Autobase(store, bootstrap, {
  open,
  apply,
  valueEncoding: 'json'
}))

const range = { gte: 'entry!', lt: 'entry"' }
const [sub] = createIndex('2x', base, range, async (node, sub) =>
  sub.put(node.key, 2 * node.value))

const key = 'entry!foo'
await base.put(key, 2)

await sub.update()

const node = await sub.get(key)
console.log(node.value) // 4
```
