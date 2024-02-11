import SubEncoder from 'sub-encoder'
import { RangeWatcher } from '@lejeunerenard/hyperbee-range-watcher-autobase'

export const wrap = (base) => {
  base.put = async (key, value, opts) => base.append({ type: 'put', key, value, opts })
  base.del = async (key, opts) => base.append({ type: 'del', key, opts })
  base.get = async (key, opts) => base.view.get(key, opts)
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
      debug && console.log('put', op.key, op.value, op.opts)
      await b.put(op.key, op.value, op.opts)
    } else if (op.type === 'del') {
      debug && console.log('del', op.key)
      await b.del(op.key, op.opts)
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
      return base.put(key, value, { keyEncoding: subEnc })
    },
    async del (key) {
      return base.del(key, { keyEncoding: subEnc })
    },
    async get (key) {
      // TODO Support other options
      return base.get(key, { keyEncoding: subEnc })
    }
  }

  const watcher = new RangeWatcher(base.view, range, 0, (node) => cb(node, sub))

  return [sub, watcher]
}
