module.exports = Locket

// In case you forgot, Alan. You've finished a rewrite. You no longer have many
// different staging trees, you only have one staging tree and it is not a
// tree, it is simply a log, a b-tree with only one leaf. When it is time to
// merge, it is renamed and then it is merged. Thus there is the primary tree,
// the staging log and possibly a merging log. There are, therefore, only two
// extra cursors in addition to the primary tree, and they can always be read
// pretty much directly using an Advance iterator, instead of having to traverse
// them as if they where actual trees.
//
// We have no way of vacuuming the primary tree at this point.
//
// *Note:*
//
// Please curb your compulsion to refactor the upstream libraries any further.
//
// You thought long and hard about this. You are not getting smarter.

// Return the first value that is not `null` nor `undefined`.
const coalesce = require('extant')

// Modules for storage and concurrency.
const Strata            = require('b-tree')
const Cache = require('b-tree/cache')
const AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
const AbstractIterator  = require('abstract-leveldown').AbstractIterator

const callbackify = require('prospective/callbackify')

const packet = require('./packet')

// Inheritence.
const util = require('util')

// Invariants.
const assert = require('assert')

// Modules for file operations. We use `strftime` to create date stamped file
// names.
const fs = require('fs').promises
const path = require('path')

const constrain = require('constrain')

const rescue = require('rescue')
const ascension = require('ascension')

const mvcc = {
    designate: require('designate'),
    dilute: require('dilute'),
    homogenize: require('homogenize'),
    riffle: require('riffle'),
    twiddle: require('twiddle'),
    splice: require('splice')
}

// TODO Let's see if we can get throught his without having to worry about
// encodings.
function encode (buffer) {
    return Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer)
}

class Paginator {
    constructor (iterator, stages, constraint) {
        this._iterator = iterator
        this._stages = stages
        this._constraint = constraint
        this._keyAsBuffer = constraint.options.keyAsBuffer
        this._valueAsBuffer = constraint.options.valueAsBuffer
        this._keys = constraint.options.keys
        this._values = constraint.options.values
        this._items = []
        this._index = 0
    }

    async next () {
        for (;;) {
            if (this._items.length != this._index) {
                const item = this._items[this._index++]
                if (this._constraint.included(item)) {
                    const result = new Array(2)
                    if (this._keys) {
                        result[0] = this._keyAsBuffer ? item.parts[1] : item.parts[1].toString()
                    }
                    if (this._values) {
                        result[1] = this._valueAsBuffer ? item.parts[2] : item.parts[2].toString()
                    }
                    return result
                }
            } else {
                const next = await this._iterator.next()
                if (next.done) {
                    return []
                } else {
                    this._items = next.value
                    this._index = 0
                }
            }
        }
    }

    release () {
        this._stages.forEach(stage => stage.readers--)
    }
}

// An implementation of the LevelDOWN `Iterator` object.
//
// The LevelUP interface allows you to specify encodings at both when you create
// the database and when you invoke one of it's functions, so we need to pass
// two different sets of options to all the functions that consider the
// encoding.

// TODO No call to super!
function Iterator (db, options) {
    AbstractIterator.call(this, db)
    this._constraint = constrain(Buffer.compare, encode, options)
    this._db = db
    this._versions = this._db._snapshot()
    this._paginator = this._db._paginator(this._constraint, this._versions)
}
util.inherits(Iterator, AbstractIterator)

// Our LevelUP `Iterator` implementation is a wrapper around the internal
// iterator that merges our Strata b-tree with one or two logs where we are
// gathering new batch operations. See the `Locket._internalIterator` method for
// a description of how we compose an amalgamated MVCC iterator from the MVCC
// iterator modules.

//
Iterator.prototype._next = callbackify(function () {
    return this._paginator.next()
})

Iterator.prototype._seek = function (target) {
    const paginator = this._paginator
    this._paginator = this.db._paginator(target, this._options)
    paginator.release()
}

Iterator.prototype._end = function (callback) {
    this._paginator.release()
    callback()
}

function Locket (destructible, location, options = {}) {
    if (!(this instanceof Locket)) return new Locket(destructible, location, options)
    AbstractLevelDOWN.call(this)
    this.location = location
    this._destructible = { root: destructible, strata: null, locket: null }
    this._cache = new Cache
    this._primary = null
    this._stages = []
    const primary = coalesce(options.primary, {})
    const stage = coalesce(options.stage, {})
    const leaf = { stage: coalesce(stage.leaf, {}), primary: coalesce(primary.leaf, {}) }
    const branch = { stage: coalesce(stage.branch, {}), primary: coalesce(primary.branch, {}) }
    this._maxStageCount = coalesce(stage.max, 1024)
    this._strata = {
        stage: {
            leaf: {
                split: coalesce(leaf.stage.split, 4096),
                merge: coalesce(leaf.stage.merge, 2048)
            },
            branch: {
                split: coalesce(branch.stage.split, 4096),
                merge: coalesce(branch.stage.merge, 2048)
            }
        },
        primary: {
            leaf: {
                split: coalesce(leaf.primary.split, 4096),
                merge: coalesce(leaf.primary.merge, 2048)
            },
            branch: {
                split: coalesce(branch.primary.split, 4096),
                merge: coalesce(branch.primary.merge, 2048)
            }
        }
    }
}
util.inherits(Locket, AbstractLevelDOWN)

Locket.prototype._serializeKey = encode

Locket.prototype._serializeValue = encode

const comparator = ascension([ Buffer.compare, BigInt, Number ], function (object) {
    return [ object.value, object.version, object.index ]
})

Locket.prototype._newStage = function (directory, options = {}) {
    const leaf = coalesce(options.leaf, 4096)
    const branch = coalesce(options.leaf, 4096)
    const strata = new Strata(this._destructible.strata.ephemeral([ 'stage', options.directory ]), {
        directory: directory,
        branch: this._strata.stage.branch,
        leaf: this._strata.stage.leaf,
        cache: this._cache,
        serializer: {
            key: {
                serialize: function ({ value, version, index }) {
                    const header = { version: version.toString(), index }
                    const buffer = Buffer.from(JSON.stringify(header))
                    return [ buffer, value ]
                },
                deserialize: function (parts) {
                    const { version, index } = JSON.parse(parts[0].toString())
                    return { value: parts[1], version: BigInt(version), index }
                }
            },
            parts: {
                serialize: function (parts) {
                    const header = {
                        header: {
                            method: parts[0].header.method,
                            index: parts[0].header.index
                        },
                        count: parts[0].count,
                        version: parts[0].version.toString()
                    }
                    const buffer = Buffer.from(JSON.stringify(header))
                    return [ buffer ].concat(parts.slice(1))
                },
                deserialize: function (parts) {
                    const header = JSON.parse(parts[0].toString())
                    header.version = BigInt(header.version)
                    return [ header ].concat(parts.slice(1))
                }
            }
        },
        _serializer: {
            key: {
                serialize: function ({ value, version, index }) {
                    const header = { version, index }
                    const buffer = Buffer.alloc(packet.key.sizeof(header))
                    packet.key.serialize(header, buffer, 0)
                    return [ buffer, value ]
                },
                deserialize: function (parts) {
                    const { version, index } = packet.key.parse(parts[0], 0)
                    return { value: parts[1], version, index }
                }
            },
            parts: {
                serialize: function (parts) {
                    const buffer = Buffer.alloc(packet.meta.sizeof(parts[0]))
                    packet.meta.serialize(parts[0], buffer, 0)
                    return [ buffer ].concat(parts.slice(1))
                },
                deserialize: function (parts) {
                    return [ packet.meta.parse(parts[0], 0) ].concat(parts.slice(1))
                }
            }
        },
        extractor: function (parts) {
            return { value: parts[1], version: parts[0].version, index: parts[0].header.index }
        },
        comparator: comparator
    })
    return { strata, path: directory, versions: { 0: true }, writers: 0, readers: 0, count: 0 }
}

Locket.prototype._filestamp = function () {
    return String(Date.now())
}

Locket.prototype._open = callbackify(async function (options) {
    // TODO What is the behavior if you close while opening, or open while
    // closing?
    this._isOpen = true
    this._destructible.locket = this._destructible.root.ephemeral('locket')
    this._destructible.strata = this._destructible.root.ephemeral('strata')
    // TODO Hoist.
    let exists = true
    this._options = options
    this._versions = { 0: true }
    // Must be one, version zero must only come out of the primary tree.
    this._version = 1n
    const files = await (async () => {
        for (;;) {
            try {
                return await fs.readdir(this.location)
            } catch (error) {
                await rescue(error, [{ code: 'ENOENT' }])
                if (!options.createIfMissing) {
                    throw new Error('Locket database does not exist')
                }
                await fs.mkdir(this.location, { recursive: true })
            }
        }
    }) ()
    const subdirs = [ 'primary', 'staging' ]
    if (exists) {
        const sorted = files.filter(file => file[0] != '.').sort()
        if (!sorted.length) {
            exists = false
        // TODO Not a very clever recover, something might be in the midst
        // of a rotation.
        } else if (!subdirs.every(file => sorted.shift() == file) || sorted.length) {
            throw new Error('not a Locket datastore')
        }
    }
    if (exists && options.errorIfExists) {
        throw new Error('Locket database already exists')
    }
    if (!exists) {
        for (const dir of subdirs) {
            await fs.mkdir(path.join(this.location, dir), { recursive: true })
        }
    }
    this._primary = new Strata(this._destructible.strata.durable('primary'), {
        directory: path.join(this.location, 'primary'),
        cache: this._cache,
        comparator: Buffer.compare,
        serializer: 'buffer',
        branch: this._strata.primary.branch,
        leaf: this._strata.primary.leaf
    })
    if ((await fs.readdir(path.join(this.location, 'primary'))).length != 0) {
        await this._primary.open()
    } else {
        await this._primary.create()
    }
    const staging = path.join(this.location, 'staging')
    for (const file of await fs.readdir(staging)) {
        console.log(file)
        const stage = this._newStage(path.join(staging, file)), counts = {}
        await stage.strata.open()
        for await (const items of mvcc.riffle.forward(stage.strata, Strata.MIN)) {
            for (const item of items) {
                const version = item.parts[0].version
                if (counts[version] == null) {
                    assert(item.parts[0].count != null)
                    // console.log(version, item.parts[0].count)
                    counts[version] = item.parts[0].count
                }
                if (item.parts[0].version == 129n) {
                    // console.log(item.parts[0].version, item.parts[0].header.method, item.parts[1].toString())
                }
                if (0 == --counts[version]) {
                    stage.versions[version] = true
                }
            }
        }
        //console.log(stage.versions, counts)
        console.log('done')
        this._stages.push(stage)
        await this._amalgamate()
        await this._unstage()
    }
    const directory = path.join(staging, this._filestamp())
    await fs.mkdir(directory, { recursive: true })
    const stage = this._newStage(directory, {})
    await stage.strata.create()
    this._stages.push(stage)
    return []
})

Locket.prototype._snapshot = function () {
    return JSON.parse(JSON.stringify(this._versions))
}

// Iteration of the database requires merging the results from the deep storage
// b-tree and the one or two staging logs.
//
// We do this by creating a merged Homogonize iterator across the b-tree and the
// logs. This is an iteartor that takes one or more iterators and advances
// through. It will advance each iterator and then when it is advanced, it
// returns the least value of each of the three iterators (or greatest value if
// iteration is reversed.)
//
// We then use the versioned iterator from the Designate module which will
// select the key/value pair for a key that has the greatest committed version.
//
// Finally we need to respect the properties given a user when creating an
// external iterator; start, stop, greater than, less than, greater than or
// equal to, less than or equal to. We use a Dilute iterator to select out only
// records that have not been deleted and that match the user's range critera.

//
Locket.prototype._paginator = function (constraint, versions) {
    this._stages.forEach(stage => stage.readers++)

    const { direction, inclusive } = constraint

    // If we are exclusive we will use a maximum version going forward and a
    // minimum version going backward, puts us where we'd expect to be if we
    // where doing exclusive with the external key only.
    const version = direction == 'forward'
        ? inclusive ? 0n : 0xffffffffffffffffn
        : inclusive ? 0xffffffffffffffffn : 0n
    // TODO Not sure what no key plus exclusive means.
    const versioned = constraint.key != null
        ? { value: constraint.key, version: version }
        : direction == 'forward'
            ? Strata.MIN
            : Strata.MAX
    const key = typeof versioned == 'symbol' ? versioned : versioned.value

    const riffle = mvcc.riffle[direction](this._primary, key, 32, inclusive)
    const primary = mvcc.twiddle(riffle, item => {
        // TODO Looks like I'm in the habit of adding extra stuff, meta stuff,
        // so the records, so go back and ensure that I'm allowing this,
        // forwarding the meta information.
        return {
            key: { value: item.parts[0], version: 0n, index: 0 },
            parts: [{
                header: { method: 'put' },
                version: 0n
            }, item.parts[0], item.parts[1]]
        }
    })

    const stages = this._stages.map(stage => {
        return mvcc.riffle[direction](stage.strata, versioned, 32, inclusive)
    })
    const homogenize = mvcc.homogenize[direction](comparator, stages.concat(primary))
    const designate = mvcc.designate[direction](Buffer.compare, homogenize, versions)
    const dilute = mvcc.dilute(designate, item => {
        if (item.parts[0].header.method == 'del') {
            return -1
        }
        return constraint.included(item.key.value) ? 0 : -1
    })

    return new Paginator(dilute[Symbol.asyncIterator](), this._stages.slice(), constraint)
}

Locket.prototype._iterator = function (options) {
    return new Iterator(this, options)
}

// TODO Maybe just leave this?
Locket.prototype._get = callbackify(async function (key, options) {
    const constraint = constrain(Buffer.compare, encode, {
        gte: key, keys: true, values: true, keyAsBuffer: true, valueAsBuffer: true
    })
    const paginator = this._paginator(constraint, this._snapshot())
    // TODO How do I reuse Cursor.found out of Riffle et. al.? Eh, no good way
    // since we have to advance, merge, dilute, etc. anyway.
    const next = await paginator.next()
    paginator.release()
    if (next.length != 0 && Buffer.compare(next[0], key) == 0) {
        return [ options.asBuffer ? next[1] : next[1].toString() ]
    }
    throw new Error('NotFoundError: not found')
})

Locket.prototype._unstage = async function () {
    const stage = this._stages.pop()
    await stage.strata.close()
    // TODO Implement Strata.options.directory.
    console.log('unstage')
    await fs.rmdir(stage.path, { recursive: true })
    this._maybeUnstage()
}

Locket.prototype._maybeUnstage = function () {
    if (this._isOpen && this._stages.length > 1) {
        const stage = this._stages[this._stages.length - 1]
        if (stage.amalgamated && stage.readers == 0) {
            this._destructible.locket.ephemeral([ 'unstage', stage.path ], this._unstage())
        }
    }
}

// TODO What makes me think that all of these entries are any good? In fact, if
// we've failed while writing a log, then loading the leaf is going to start to
// play the entries of the failed transaction. We need a player that is going to
// save up the entries, and then play them as batches, if the batch has a
// comment record attached to it. Then we know that our log here is indeed the
// latest and greatest.
//
// Another problem is that the code below will insert the records with their
// logged version, instead of converting those verisons to zero.
Locket.prototype._amalgamate = async function () {
    const stage = this._stages[this._stages.length - 1]
    assert.equal(stage.writers, 0)
    let iterator = null
    const riffle = mvcc.riffle.forward(stage.strata, Strata.MIN)
    // TODO Track versions in stage.
    const designate = mvcc.designate.forward(Buffer.compare, riffle, stage.versions)
    await mvcc.splice(item => {
        return {
            key: item.key.value,
            parts: item.parts[0].header.method == 'put' ? item.parts.slice(1) : null
        }
    }, this._primary, designate)
    stage.amalgamated = true
    this._maybeUnstage()
}

Locket.prototype._put = function (key, value, options, callback) {
    this._batch([{ type: 'put', key: key, value: value }], options, callback)
}

Locket.prototype._del = function (key, options, callback) {
    this._batch([{ type: 'del', key: key }], options, callback)
}

// Could use a header record. It would sort out to be less than all the user
// records, so it wouldn't get in the way of a search, and we wouldn't have to
// filter it. It does however mean at least two writes for every `put` or `del`
// and I suspect that common usage is ingle `put` or `del`, so going to include
// the count in ever record, it is only 32-bits.
Locket.prototype._batch = callbackify(async function (batch, options) {
    const stage = this._stages[0]
    stage.writers++
    const version = ++this._version
    const count = batch.length
    const writes = {}
    let cursor = Strata.nullCursor(), index = 0, i = 0
    for (const operation of batch) {
        const { type: method, key: value } = operation, count = batch.length
        const key = { value: encode(value), version, index: i }
        index = cursor.indexOf(key, cursor.page.ghosts)
        if (index == null) {
            console.log('re-descend', version, cursor.page.id)
            const entry = await stage.strata._journalist.load('0.0')
            console.log('!', entry.value.items)
            entry.release()
            if (cursor.page.items != null) {
                for (const item of cursor.page.items) {
                    console.log('>', item.key.value.toString(), item.key.version, item.key.index, item.parts[0].header.method)
                }
            }
            cursor.release()
            cursor = (await stage.strata.search(key)).get()
            index = cursor.index
            assert(!cursor.found)
        } else {
            assert(index < 0)
            index = ~index
        }
        const header = { header: { method, index: i }, count, version }
        if (version == 124n) {
            console.log(cursor.page.items[0].key.value.toString(), cursor.page.id, operation.type, operation.key.toString(), index, i)
        }
        i++
        if (method == 'put') {
            cursor.insert(index, [ header, operation.key, operation.value ], writes)
        } else {
            cursor.insert(index, [ header, operation.key ], writes)
        }
        stage.count++
    }
    let count_ = 0
    for (const item of cursor.page.items) {
        if (item.key.version == 129n) {
            count_++
            console.log(item.key.version, item.parts[0].header.method, item.key.value.toString())
        }
    }
    console.log(count_)
    cursor.release()
    await Strata.flush(writes)
    stage.versions[version] = this._versions[version] = true
        const counts = {}, versions = {}
        for await (const items of mvcc.riffle.forward(stage.strata, Strata.MIN)) {
            for (const item of items) {
                const version = item.parts[0].version
                if (counts[version] == null) {
                    assert(item.parts[0].count != null)
                    counts[version] = item.parts[0].count
                }
                if (item.parts[0].version == 129n) {
                    console.log(item.parts[0].version, item.parts[0].header.method, item.parts[1].toString())
                }
                if (0 == --counts[version]) {
                    versions[version] = true
                }
            }
        }
        console.log('>>>', version, counts, versions)
    stage.writers--
    // A race to create the next stage, but the loser will merely create a stage
    // taht will be unused or little used.
    if (this._stages[0].count > this._maxStageCount) {
        const directory = path.join(this.location, 'staging', this._filestamp())
        await fs.mkdir(directory, { recursive: true })
        const next = this._newStage(directory, {})
        await next.strata.create()
        this._stages.unshift(next)
    }
    this._maybeAmalgamate()
    return []
})

Locket.prototype._maybeAmalgamate = function () {
    if (this._isOpen && this._stages.length != 1 && this._stages[this._stages.length - 1].writers == 0) {
        // Enqueue the amalgamation or else fire and forget.
        this._destructible.locket.ephemeral('amalgamate', async () => {
            await this._amalgamate()
            this._maybeUnstage()
            this._maybeAmalgamate()
        })
    }
}

Locket.prototype._approximateSize = callbackify(async function (from, to) {
    const constraint = constrain(Buffer.compare, encode, { gte: from, lte: to })
    let approximateSize = 0
    for (const items of this._whatever()) {
        for (const item of items) {
            approximateSize += item.heft
        }
    }
    return approximateSize
})

// TODO Countdown through the write queue.
Locket.prototype._close = callbackify(async function () {
    if (this._isOpen) {
        this._isOpen = false
        this._destructible.locket.destroy()
        await this._destructible.locket.destructed
        await this._primary.close()
        while (this._stages.length != 0) {
            await this._stages.shift().strata.close()
        }
        await this._destructible.strata.destructed
        this._cache.purge(0)
    }
    return []
})
