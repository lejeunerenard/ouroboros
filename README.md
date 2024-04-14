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

## API

### `const hyperbeeLikeBase = wrap(base)`

`wrap` adds functions to the `Autobase` `base` object with a `Hyperbee`
`.view` to support a similar API to `Hyperbee`, eg. `.put(key, value)` &
`.get(key)`.

### `async apply (batch, bee, base)`

This `apply` function provides the functionality necessary to process the
`autobase` op log style input blocks for the API `wrap` provides. This is a
convenience function to be used as / or composed with the `autobase`'s `apply`.

### `const sub = await createIndex(name, base, ranges, callback, opts)`

Create a `SubIndex` with the given `name` which updates, as described by the
`callback`, when the `ranges` change on the provided `base`'s hyperbee `.view`.

For example, the following creates a '2x' index which watches keys between
`0`–`10` and updates an entry to 2 times the original value.:

```js
const [sub] = createIndex('2x', base, { lt: '10', gte: '0' }, async (node, sub) =>
  sub.put(node.key, 2 * node.value))
```

`ranges` are the key ranges accepted by [`hyperbee-diff-stream`'s
`opts`](https://github.com/holepunchto/hyperbee-diff-stream?tab=readme-ov-file#const-diffstream--new-beediffstreamleftsnapshot-rightsnapshot-options).

`callback` is provided with the `node` that changed and the current `SubIndex`
for the index.

### `SubIndex`

Represents the index's db and provides a similar API to a `Hyperbee` database.
Can only be created via `createIndex()`. Currently supports the following
`Hyperbee` methods:

- `await sub.put(key, value)`
- `await sub.del(key)`
- `const { seq, key, value } = await sub.get(key)`
- `sub.createReadStream(range, opts)`
- `const { seq, key, value } = await sub.peek([range], [options])`
- `await sub.update()`

#### `update` event

This debounced event triggers after the `callback` finishes running indicating
the index has been updated based on the `ranges`' changes.

#### `await sub.drained()`

Triggered after a index callback is finished processing changes. Like stream's
`drain` event, this event signals that the index is caught up and ready for more
updates.
