import SubEncoder from 'sub-encoder'
import { RangeWatcher } from '@lejeunerenard/hyperbee-range-watcher-autobase'
import { EventEmitter } from 'events'

export const wrap = (base) => {
  base.put = (key, value, opts) => base.append({
    type: 'put',
    key,
    value,
    opts
  })

  base.del = (key, opts) => base.append({ type: 'del', key, opts })

  base.get = (key, opts) => base.view.get(key, opts)

  base.peek = (opts) => base.view.peek(opts)

  base.createReadStream = (range, opts) => base.view.createReadStream(range, opts)

  return base
}

export const apply = async (batch, bee, base) => {
  const debug = false
  debug && console.log('-- in apply --')
  const b = bee.batch({ update: false })

  for (const node of batch) {
    debug && console.log('-> node.value', node.value)
    const hyperbeeOp = 'type' in node.value
    if (
      !hyperbeeOp ||
      (node.value.type !== 'put' && node.value.type !== 'del')
    ) {
      continue
    }

    const op = node.value
    if (op.type === 'put') {
      debug && console.log('-> put', op.key, op.value, op.opts)
      await b.put(op.key, op.value, op.opts)
    } else if (op.type === 'del') {
      debug && console.log('-> del', op.key, op.opts)
      await b.del(op.key, op.opts)
    }
  }

  await b.flush()
  debug && console.log('-- end apply --')
}

export const INDEX_META_NS = 'indexes-meta'
const indexMetaEnc = new SubEncoder()
export const indexMetaSubEnc = indexMetaEnc.sub(INDEX_META_NS)

class SubIndex extends EventEmitter {
  constructor (name, base, version) {
    super()

    this.name = name
    this.base = base
    this.version = version

    const enc = new SubEncoder()
    const subEnc = enc.sub(name)
    this.enc = subEnc
    this._updateTimeout = null
    this._emitUpdate = () => {
      clearTimeout(this._updateTimeout)
      this._updateTimeout = setTimeout(() => {
        this.emit('update')
      })
    }
  }

  update () {
    return new Promise((resolve) => this.once('update', resolve))
  }

  put (key, value) {
    return this.base.put(key, value, { keyEncoding: this.enc })
  }

  del (key) {
    return this.base.del(key, { keyEncoding: this.enc })
  }

  get (key) {
    // TODO Support other options
    return this.base.get(key, { keyEncoding: this.enc })
  }

  createReadStream (range, opts) {
    return this.base.createReadStream(range, { ...opts, keyEncoding: this.enc })
  }

  peek (opts) {
    return this.base.peek({ ...opts, keyEncoding: this.enc })
  }
}

export const createIndex =
  async (name, base, ranges, cb, opts = { version: 1 }) => {
    const debug = false
    const version = opts.version

    if (!Array.isArray(ranges)) ranges = [ranges]

    const sub = new SubIndex(name, base, version)

    const prevVersion = await base.get(name, { keyEncoding: indexMetaSubEnc })
    debug && console.log('prevVersion', prevVersion, 'version', version)
    if (prevVersion && prevVersion.value < version) {
      // TODO implement updating index as a batch append
      await base.put(name, version, { keyEncoding: indexMetaSubEnc })
      if (prevVersion) {
        const proms = []
        for await (const node of base.view.createReadStream({ keyEncoding: sub.enc })) {
          proms.push(base.del(node.key, { keyEncoding: sub.enc }))
        }

        await Promise.all(proms)
      }
    } else {
      await base.put(name, version, { keyEncoding: indexMetaSubEnc })
    }

    // TODO Consider implementing a version that walks through the history of the
    // view
    const watchers = ranges.map((range) => new RangeWatcher(
      base.view, range, 0, async (node) => {
        await cb(node, sub)
        sub._emitUpdate()
      }))

    return [sub, watchers]
  }
