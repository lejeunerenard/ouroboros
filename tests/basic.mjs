import test from 'tape'
import Autobase from 'autobase'
import Hyperbee from 'hyperbee'
import Corestore from 'corestore'
import RAM from 'random-access-memory'
import b4a from 'b4a'
import { setTimeout } from 'timers/promises'
import { apply, createIndex, wrap, indexMetaSubEnc } from '../index.mjs'

function bump (key) {
  key[key.length - 1]++
  return key
}

function makeBase (opts = {}) {
  const storage = opts.storage || (() => RAM.reusable())
  const store = new Corestore(storage())
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

test('basic', (t) => {
  t.test('basic indexes', async (t) => {
    const base = makeBase()
    const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
    const [sub] = await createIndex('2x', base, range, async (node, sub) =>
      sub.put(node.key, 2 * node.value))

    const rangeFor4x = sub.enc.encodeRange(range)
    const [sub4x] = await createIndex('4x', base, rangeFor4x,
      async (node, mySub) => {
        const key = sub.enc.decode(b4a.from(node.key))
        await mySub.put(key, 2 * node.value)
      })

    const key = 'entry!foo'
    await base.put(key, 2)

    await sub.update()

    const originalNode = await base.get(key)
    t.equal(originalNode.value, 2, 'default put took')
    const node = await sub.get(key)
    t.equal(node.value, 4, 'derived value is 2 times')

    const node4x = await sub4x.get(key)
    t.equal(node4x.value, 8, 'double derived value is 4 times')
  })

  t.test('starts from current bee version', async (t) => {
    t.plan(4)
    const reusable = RAM.reusable()
    const storage = () => reusable
    const base = makeBase({ storage })

    const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
    const [sub] = await createIndex('sum', base, range, async (node, sub) => {
      const total = await sub.get('total')
      await sub.put('total', node.value + (total !== null ? total.value : 0))
    })

    await base.put('entry!foo', 2)
    await base.put('entry!bar', 3)
    await base.put('entry!baz', 4)

    await sub.update()

    t.equal((await sub.get('total')).value, 9)
    await base.close()

    const base2 = makeBase({ storage })
    const [sub2] = await createIndex('sum', base2, range, async (node, sub) => {
      const total = await sub.get('total')
      t.equal(total.value, 9)
      await sub.put('total', node.value + (total !== null ? total.value : 0))
    })

    t.deepEquals(base.key, base2.key, 'loaded the same autobase')

    await base2.put('entry!biz', 5)
    await sub2.update()

    t.equal((await sub2.get('total')).value, 14)
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
    await sub.update()

    const originalNode = await base.get(key)
    t.equal(originalNode.value, 2, 'default put took')
    const node = await sub.get(key)
    t.equal(node.value, 4, 'derived value is 2 times')

    await base.del(key)
    await sub.update()
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
      const { value: versionInDb } = await base.get('+1', {
        keyEncoding: indexMetaSubEnc
      })
      t.equal(versionInDb, 1, 'db meta index sets version')

      await base.put('entry!foo', 1)
      await base.put('entry!bar', 2)

      const is2 = await sub.get('entry!foo')
      t.equal(is2.value, 2, '1 + 1 = 2')

      await base.del('entry!foo')

      const notGone = await sub.get('entry!foo')
      t.notEqual(notGone, null, 'foo indexed value isnt deleted')

      // Add .del support
      const [subV2] = await createIndex('+1', base, range,
        async (node, sub) => ('type' in node && node.type === 'del')
          ? sub.del(node.key)
          : sub.put(node.key, node.value + 1)
        , { version: 2 })

      await subV2.update()

      const indexRemoved = await subV2.get('entry!foo')
      t.equal(indexRemoved, null, 'index values removed w/ new version')

      const barNodeV2 = await subV2.get('entry!bar')
      t.notEqual(barNodeV2, null, 'existing keys are processed immediately')

      t.end()
    })

    t.test('version upgrade w/o callback update stops indexing', async (t) => {
      const base = makeBase()

      const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
      const shouldTrigger = true
      const [sub] = await createIndex('+1', base, range, async (node, sub) => {
        if (!shouldTrigger) t.fail('fired callback even though version is old')
        return sub.put(node.key, node.value + 1)
      })

      // Check version tracking
      t.equal(sub.version, 1, 'defaults to version 1')
      const { value: versionInDb } = await base.get('+1', {
        keyEncoding: indexMetaSubEnc
      })
      t.equal(versionInDb, 1, 'db meta index sets version')

      await base.put('entry!foo', 1)
      await base.put('entry!bar', 2)

      const is2 = await sub.get('entry!foo')
      t.equal(is2.value, 2, '1 + 1 = 2')

      // Artificially upgrade aka simulate peer updates version
      await base.put('+1', 2, {
        keyEncoding: indexMetaSubEnc
      })

      // Attempt to trigger index
      await base.put('entry!baz', 3)

      await sub.drained()

      const shouldntExist = await sub.get('entry!baz')
      t.equal(shouldntExist, null, 'new entry wasn\'t added')

      t.end()
    })

    t.test('declaring older version index doesnt run', async (t) => {
      const base = makeBase()

      // Artificially set version to 2
      await base.put('+1', 2, {
        keyEncoding: indexMetaSubEnc
      })

      const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
      const [sub] = await createIndex('+1', base, range, async (node, sub) => {
        t.fail('fired callback even though version is old')
      }, { version: 1 })

      // Attempt to trigger index
      await base.put('entry!baz', 3)

      await sub.drained()

      const shouldntExist = await sub.get('entry!baz')
      t.equal(shouldntExist, null, 'new entry wasn\'t added')

      t.end()
    })
  })

  t.test('two range dependencies', async (t) => {
    const base = makeBase()
    const ranges = [
      { gte: 'add!', lt: bump(b4a.from('add!')) },
      { gte: 'subtract!', lt: bump(b4a.from('subtract!')) }
    ]

    const [sub] = await createIndex('result', base, ranges,
      async (node, sub) => {
        let total = 0
        for (const range of ranges) {
          for await (const node of base.view.createReadStream(range)) {
            const op = node.key.split('!')[0]
            switch (op) {
              case 'add':
                total += node.value
                break
              case 'subtract':
                total -= node.value
                break
            }
          }
        }
        await sub.put('total', total)
      })

    await base.put('add!a', 1)
    await base.put('add!b', 2)
    await base.put('subtract!c', 3)
    await base.put('add!a', 4)
    await base.put('foo', 3)

    await sub.update()

    const total = await sub.get('total')
    t.equal(total.value, 3)
  })

  t.test('update()', (t) => {
    t.test('basic', async (t) => {
      t.plan(2)
      const base = makeBase()
      const ranges = [
        { gte: 'foo!', lt: bump(b4a.from('foo!')) }
      ]

      const [sub] = await createIndex('result', base, ranges,
        async (node, sub) => sub.put('latest', node.value))

      // Test that the update returns when there is nothing to process
      await sub.update()
      t.pass('immediately runs')

      await base.put('foo!a', 1)
      await base.put('foo!b', 2)
      await base.put('foo!c', 4)
      await base.put('foo!a', 9)
      await base.put('foo!0', 3)

      // Clears updates
      await sub.update()

      const latest = await sub.get('latest')
      t.equal(latest.value, 3)
    })

    t.test('doesnt wait', async (t) => {
      t.plan(2)
      const base = makeBase()
      const ranges = [
        { gte: 'delay!', lt: bump(b4a.from('delay!')) }
      ]

      const [sub] = await createIndex('result', base, ranges,
        async (node, sub) => {
          await setTimeout(node.value)
          await sub.put('latest', node.key)
        })

      // Test that the update returns when there is nothing to process
      await sub.update()
      t.pass('immediately runs')

      await base.put('delay!a', 0)
      await base.put('delay!b', 50)
      await base.put('delay!0', 50)

      // Clears updates
      await sub.update()

      const latest = await sub.get('latest')
      t.equal(latest.value, 'delay!a', 'first key was last key after update')
    })
  })

  t.test('drained()', async (t) => {
    t.plan(1)
    const base = makeBase()
    const ranges = [
      { gte: 'delay!', lt: bump(b4a.from('delay!')) }
    ]

    const [sub] = await createIndex('result', base, ranges,
      async (node, sub) => {
        await setTimeout(10)
        await sub.put('latest', node.key)
      })

    await base.put('delay!a', 0)
    await base.put('delay!0', 50)
    // TODO No guarantee that just because the key was updated last that it is the last run in the index if jumping versions
    await base.put('delay!1', 50)

    // Clears updates
    await sub.drained()

    const latest = await sub.get('latest')
    t.equal(latest.value, 'delay!1', 'waited for last put')
  })
})
