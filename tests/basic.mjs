import test from 'tape'
import Autobase from 'autobase-next'
import Hyperbee from 'hyperbee'
import Corestore from 'corestore'
import RAM from 'random-access-memory'
import { setTimeout } from 'timers/promises'
import b4a from 'b4a'
import { apply, createIndex, wrap, indexMetaSubEnc } from '../index.mjs'

function bump (key) {
  key[key.length - 1]++
  return key
}

function makeBase () {
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

  return wrap(new Autobase(store, bootstrap, {
    open,
    apply,
    valueEncoding: 'json'
  }))
}

test('basic', (t) => {
  t.test('basic indexes', async (t) => {
    const base = makeBase()
    const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
    const [sub] = await createIndex('2x', base, range, async (node, sub) =>
      sub.put(node.key, 2 * node.value))

    const rangeFor4x = sub.enc.encodeRange(range)
    const [sub4x] = await createIndex('4x', base, rangeFor4x, async (node, mySub) => {
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
    const base = makeBase()
    await base.ready()

    const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
    const [sub] = await createIndex('2x', base, range, async (node, sub) => {
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

    const nodeAfterDel = await sub.get(key)
    t.equal(nodeAfterDel, null, 'updates index from deleted key')
  })

  t.test('version support', (t) => {
    t.test('declaring new version wipes existing index', async (t) => {
      const base = makeBase()

      const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
      const [sub] = await createIndex('+1', base, range, async (node, sub) =>
        sub.put(node.key, node.value + 1))

      t.equal(sub.version, 1, 'defaults to version 1')
      const { value: versionInDb } = await base.get('+1', { keyEncoding: indexMetaSubEnc })
      t.equal(versionInDb, 1, 'db meta index sets version')

      await base.put('entry!foo', 1)
      await base.put('entry!bar', 2)

      const is2 = await sub.get('entry!foo')
      t.equal(is2.value, 2, '1 + 1 = 2')

      await base.del('entry!foo')

      const notGone = await sub.get('entry!foo')
      t.notEqual(notGone, null, 'foo indexed value isnt deleted')

      // Add .del support
      const [subV2] = await createIndex('+1', base, range, async (node, sub) => {
        return ('type' in node && node.type === 'del')
          ? sub.del(node.key)
          : sub.put(node.key, node.value + 1)
      }, { version: 2 })

      // flush events
      await setTimeout(0)

      const indexRemoved = await subV2.get('entry!foo')
      t.equal(indexRemoved, null, 'index values removed w/ new version')

      const barNodeV2 = await subV2.get('entry!bar')
      t.notEqual(barNodeV2, null, 'existing keys are processed immediately')

      t.end()
    })
  })
})
