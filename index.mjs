import SubEncoder from 'sub-encoder'
import { RangeWatcher } from '@lejeunerenard/hyperbee-range-watcher-autobase'
import { setTimeout } from 'timers/promises'

export const wrap = (base) => {
  base.put = async (key, value, opts) => base.append({
    type: 'put',
    key,
    value,
    opts
  }).then((...args) => setTimeout(0, ...args))

  base.del = async (key, opts) => base.append({ type: 'del', key, opts })
    .then((...args) => setTimeout(0, ...args))

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

export const INDEX_META_NS = 'indexes-meta'
const indexMetaEnc = new SubEncoder()
export const indexMetaSubEnc = indexMetaEnc.sub(INDEX_META_NS)

export const createIndex = async (name, base, range, cb, opts = { version: 1 }) => {
  const debug = false
  const version = opts.version

  const enc = new SubEncoder()
  const subEnc = enc.sub(name)
  const sub = {
    enc: subEnc,
    version,
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

  const prevVersion = await base.get(name, { keyEncoding: indexMetaSubEnc })
  debug && console.log('prevVersion', prevVersion, 'version', version)
  if (prevVersion && prevVersion.value < version) {
    await base.put(name, version, { keyEncoding: indexMetaSubEnc })
    if (prevVersion) {
      const proms = []
      for await (const node of base.view.createReadStream({ keyEncoding: subEnc })) {
        proms.push(base.del(node.key, { keyEncoding: subEnc }))
      }

      await Promise.all(proms)
    }
  } else {
    await base.put(name, version, { keyEncoding: indexMetaSubEnc })
  }

  // TODO Consider implementing a version that walks through the history of the
  // view
  const watcher = new RangeWatcher(base.view, range, 0, (node) => cb(node, sub))

  return [sub, watcher]
}
