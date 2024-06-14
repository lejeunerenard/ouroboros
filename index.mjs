import SubEncoder from 'sub-encoder'
import { RangeWatcher } from '@lejeunerenard/hyperbee-range-watcher-autobase'
import { EventEmitter } from 'events'
import b4a from 'b4a'

export const wrap = (base) => {
  base.put = (key, value, opts) => {
    const encKey = opts && opts.keyEncoding ? opts.keyEncoding.encode(key) : key
    if (opts && opts.keyEncoding) {
      delete opts.keyEncoding
    }

    return base.append({
      type: 'put',
      key: encKey,
      value,
      opts
    })
  }

  base.del = (key, opts) => {
    const encKey = opts && opts.keyEncoding ? opts.keyEncoding.encode(key) : key
    if (opts && opts.keyEncoding) {
      delete opts.keyEncoding
    }
    return base.append({ type: 'del', key: encKey, opts })
  }

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
      await b.put(b4a.from(op.key), op.value, op.opts)
    } else if (op.type === 'del') {
      debug && console.log('-> del', op.key, op.opts)
      await b.del(b4a.from(op.key), op.opts)
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

    this._watchers = []
    this._updateTimeout = null
    this._emitUpdate = () => {
      clearTimeout(this._updateTimeout)
      this._updateTimeout = setTimeout(() => {
        this.emit('update')
      })
    }
  }

  async update () {
    const oldestWatcher = this._watchers.reduce((oldest, w) =>
      w.latest.version <= oldest.latest.version ? w : oldest, {
      latest: {
        version: Infinity
      }
    })

    if (this.base.view.version > oldestWatcher.latest.version) {
      return Promise.any([
        oldestWatcher.update(),
        // drop out early for  immediate updates
        new Promise((resolve) => this.once('update', resolve))
      ])
    }
  }

  async drained () {
    // Wait for watchers to process current input
    await Promise.all(this._watchers.map((w) => w.update()))
    // ensure base is update to date
    return this.update()
  }

  put (key, value) {
    const encKey = this.enc.encode(key)
    return this.base.put(encKey, value)
  }

  del (key) {
    const encKey = this.enc.encode(key)
    return this.base.del(encKey)
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

    await base.ready()

    // Default to only watching since db version when index is created
    let dbVersionBefore = base.view.snapshot()

    const prevVersion = await base.get(name, { keyEncoding: indexMetaSubEnc })
    debug && console.log('prevVersion', prevVersion, 'version', version)
    if (prevVersion && prevVersion.value < version) {
      // TODO implement updating index as a batch append
      await base.put(name, version, { keyEncoding: indexMetaSubEnc })

      const proms = []
      for await (const { key } of base.view.createReadStream({ keyEncoding: sub.enc })) {
        proms.push(base.del(key, { keyEncoding: sub.enc }))
      }

      await Promise.all(proms)
      dbVersionBefore = base.view.checkout(1)
    } else if (!prevVersion) {
      await base.put(name, version, { keyEncoding: indexMetaSubEnc })
    } else if (prevVersion.value > version) {
      console.warn(`The current index version [${prevVersion.value}] is newer than the declared version [${version}]. Upgrade needed. No indexing will happen.`)
      return [sub]
    }

    // TODO Consider implementing a version that walks through the history of the
    // view
    const watchers = ranges.map((range) => {
      const watcher = new RangeWatcher(
        base.view, range, dbVersionBefore, async (node) => {
          await cb(node, sub)
          sub._emitUpdate()
        })
      sub._watchers.push(watcher)
      return watcher
    })

    const indexVersionWatcher = await base.view.getAndWatch(name, { keyEncoding: indexMetaSubEnc })
    indexVersionWatcher.on('update', () => {
      if (indexVersionWatcher.node.value > version) {
        watchers.map((w) => w.close())
      }
    })

    return [sub, watchers]
  }
