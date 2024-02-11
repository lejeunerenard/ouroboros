import test from 'tape'
import Autobase from 'autobase-next'
import Hyperbee from 'hyperbee'
import Corestore from 'corestore'
import RAM from 'random-access-memory'
import { setTimeout } from 'timers/promises'
import b4a from 'b4a'
import { apply, createIndex, wrap } from '../index.mjs'

function bump (key) {
  key[key.length - 1]++
  return key
}

test('basic', (t) => {
  t.test('readme example', async (t) => {
    const store = new Corestore(RAM)
    const bootstrap = null
    const open = (viewStore) => {
      const core = viewStore.get('ouroborus')
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

    const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
    const [sub] = createIndex('2x', base, range, async (node, sub) =>
      sub.put(node.key, 2 * node.value))

    const rangeFor4x = sub.enc.encodeRange(range)
    const [sub4x] = createIndex('4x', base, rangeFor4x, async (node, mySub) => {
      const key = sub.enc.decode(b4a.from(node.key))
      await mySub.put(key, 2 * node.value)
    })

    const key = 'entry!foo'
    await base.put(key, 2)
    const originalNode = await base.get(key)
    t.equal(originalNode.value, 2, 'default put took')
    const node = await sub.get(key)
    t.equal(node.value, 4, 'derived value is 2 times')

    const node4x = await sub4x.get(key)
    t.equal(node4x.value, 8, 'double derived value is 4 times')
  })

  t.test('supports .del()', async (t) => {
    const store = new Corestore(RAM)
    const bootstrap = null
    const open = (viewStore) => {
      const core = viewStore.get('ouroborus')
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
    await base.ready()

    const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
    const [sub] = createIndex('2x', base, range, async (node, sub) => {
      // TODO Not sure why it returns the appended node here
      // Likely an error with RangeWatcher
      return ('type' in node && node.type === 'del')
        ? sub.del(node.key)
        : sub.put(node.key, 2 * node.value)
    })

    const key = 'entry!foo'
    await base.put(key, 2)
    const originalNode = await base.get(key)
    t.equal(originalNode.value, 2, 'default put took')
    const node = await sub.get(key)
    t.equal(node.value, 4, 'derived value is 2 times')

    await base.del(key)
    const delNode = await base.get(key)
    t.equal(delNode, null)

    await setTimeout(0)

    const nodeAfterDel = await sub.get(key)
    t.equal(nodeAfterDel, null, 'updates index from deleted key')
  })
})
