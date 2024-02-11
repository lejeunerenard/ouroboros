import SubEncoder from 'sub-encoder'
import { RangeWatcher } from '@lejeunerenard/hyperbee-range-watcher-autobase'

export const wrap = (base) => {
  base.put = async (key, value) => base.append({ type: 'put', key, value })
  base.del = async (key) => base.append({ type: 'del', key })
  base.get = async (key) => base.view.get(key)
  return base
}

export const apply = async (batch, bee, base) => {
  const debug = false
  debug && console.log('-- in apply --')
  const b = bee.batch({ update: false })

  for (const node of batch) {
    debug && console.log('node', node)
    const hyperbeeOp = 'type' in node.value
    if (!hyperbeeOp || (node.value.type !== 'put' && node.value.type !== 'del')) continue

    const op = node.value
    if (op.type === 'put') {
      debug && console.log('put', op.key, op.value)
      await b.put(op.key, op.value)
    } else if (op.type === 'del') {
      debug && console.log('del', op.key)
      await b.del(op.key)
    }
  }

  await b.flush()
  debug && console.log('-- end apply --')
}

export const createIndex = (name, base, range, cb) => {
  const enc = new SubEncoder()
  const subEnc = enc.sub(name)
  const sub = {
    enc: subEnc,
    async put (key, value) {
      return base.put(subEnc.encode(key), value)
    },
    async del (key) {
      return base.del(subEnc.encode(key))
    },
    async get (key) {
      // TODO Support other options
      return base.get(subEnc.encode(key))
    }
  }

  const watcher = new RangeWatcher(base.view, range, 0, (node) => cb(node, sub))

  return [sub, watcher]
}
