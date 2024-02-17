import test from 'tape'
import Autobase from 'autobase'
import Hyperbee from 'hyperbee'
import Corestore from 'corestore'
import RAM from 'random-access-memory'
import b4a from 'b4a'
import { apply, createIndex, wrap } from '../index.mjs'

function bump (key) {
  key[key.length - 1]++
  return key
}

function makeBase () {
  const store = new Corestore(RAM)
  const bootstrap = null
  const open = (viewStore) => {
    const core = viewStore.get('ouroboros')
    return new Hyperbee(core, {
      keyEncoding: 'utf-8',
      valueEncoding: 'json',
      extension: false
    })
  }

  return wrap(new Autobase(store, bootstrap, {
    open,
    apply,
    valueEncoding: 'json'
  }))
}

test('test various index types', (t) => {
  t.test('agregate indexes', async (t) => {
    t.test('per put', async (t) => {
      const base = makeBase()
      const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
      const [sub] = await createIndex('sum', base, range, async (node, sub) => {
        const total = await sub.get('total')
        await sub.put('total', (total ? total.value : 0) + node.value)
      })

      await base.put('entry!a', 1)
      await base.put('entry!b', 2)
      await base.put('entry!c', 3)

      const total = await sub.get('total')
      t.equal(total.value, 6)
    })

    t.test('over range', async (t) => {
      const base = makeBase()
      const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
      const [sub] = await createIndex('sum', base, range, async (node, sub) => {
        let total = 0
        for await (const node of base.view.createReadStream(range)) {
          total += node.value
        }
        await sub.put('total', total)
      })

      await base.put('entry!a', 1)
      await base.put('entry!b', 2)
      await base.put('entry!c', 3)
      await base.put('entry!a', 3)

      const total = await sub.get('total')
      t.equal(total.value, 8)
    })
  })
})
