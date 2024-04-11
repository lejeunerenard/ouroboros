import test from 'tape'
import Autobase from 'autobase'
import Hyperbee from 'hyperbee'
import Corestore from 'corestore'
import RAM from 'random-access-memory'
import b4a from 'b4a'
import { setTimeout } from 'timers/promises'
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

      await sub.update()

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

      await sub.update()

      const total = await sub.get('total')
      t.equal(total.value, 8)
    })

    t.test('over sub range', async (t) => {
      const base = makeBase()
      const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
      const [sub] = await createIndex('sum', base, range, async (node, sub) => {
        await sub.put(node.key, node.value * 2)

        let total = 0
        for await (const node of sub.createReadStream(range)) {
          total += node.value
        }
        await sub.put('total', total)
      })

      await base.put('entry!a', 1)
      await base.put('entry!b', 2)
      await base.put('entry!c', 3)
      await base.put('entry!a', 3)

      await sub.update()

      const total = await sub.get('total')
      t.equal(total.value, 16)
    })

    t.test('sub peek', async (t) => {
      const base = makeBase()
      const range = { gte: 'n!', lt: bump(b4a.from('n!')) }
      const [sub] = await createIndex('f(n)', base, range, async (node, sub) => {
        const n = node.value
        const nStr = String(n).padStart(3)
        let factIter = 0
        const nMinus1 = await sub.peek({ lt: b4a.from(nStr), reverse: true })
        if (nMinus1) {
          factIter += nMinus1.value

          // Attempt n-2
          const nMinus2 = await sub.peek({ lt: nMinus1.key, reverse: true })
          if (nMinus2) {
            factIter += nMinus2.value
          } else {
            factIter = 1
          }
        } else {
          factIter = 1
        }

        await sub.put(nStr, factIter)
      })

      for (let i = 1; i <= 16; i++) {
        await base.put('n!' + String(i).padStart(3), i)
      }

      await sub.update()

      const total = await sub.get(String(16).padStart(3))
      t.equal(total.value, 987, 'implement fibonacci numbers w/ peek')
    })
  })

  t.test('slow indexes', async (t) => {
    const base = makeBase()
    const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }

    // Increase work with each iteration to simulate "exponential work"
    let runCount = 1
    const [sub] = await createIndex('seconds later', base, range, async (node, sub) => {
      runCount++
      await setTimeout(runCount * 50)
      await sub.put(node.key, node.value)
    })

    const getKey = (i) => 'entry!' + String(i).padStart(3)
    for (let i = 1; i <= 16; i++) {
      await base.put(getKey(i), i)
    }

    await sub.drained()

    const total = await sub.get(getKey(16))
    t.notEqual(total, null, 'index was written to')
  })

  t.test('many deferred indexes', async (t) => {
    const base = makeBase()
    const range = { gte: 'entry!', lt: bump(b4a.from('entry!')) }
    const [sub] = await createIndex('ski', base, range, async (node, sub) => {
      await setTimeout(0)
      await sub.put(node.key, node.value)
    })

    const many = 100
    const getKey = (i) => 'entry!' + String(i).padStart(3)
    for (let i = 1; i <= many; i++) {
      await base.put(getKey(i), i)
    }

    await new Promise((resolve) => sub.on('drain', resolve))

    const total = await sub.get(getKey(many))
    t.notEqual(total, null, 'index was written to')
  })
})
