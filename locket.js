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

// Define the pluggable serialization components of our Strata trees.
function compare (left, right) {
    return Buffer.compare(left, right)
}

function keyComparator (a, b) { return comparator(a.key, b.key) }

function encode (buffer) {
    return Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer)
}

function callbackify (f) {
    return function (...vargs) {
        const callback = vargs.pop()
        f.apply(this, vargs)
            .then(vargs => {
                if (vargs.length == 0) {
                    callback()
                } else {
                    callback.apply(null, [ null ].concat(vargs))
                }
            })
            .catch(error => callback(error))
    }
}

class Paginator {
    constructor (iterator, stages, constraint) {
        this._iterator = iterator
        this._stages = stages
        this._constraint = constraint
        this._keyAsBuffer = constraint.options.keyAsBuffer
        this._valueAsBuffer = constraint.options.valueAsBuffer
        this._items = []
        this._index = 0
    }

    async next () {
        for (;;) {
            if (this._items.length != this._index) {
                const item = this._items[this._index++]
                if (this._constraint.included(item)) {
                    return [
                        this._keyAsBuffer ? item.parts[1] : item.parts[1].toString(),
                        this._valueAsBuffer ? item.parts[2] : item.parts[2].toString()
                    ]
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
function Iterator (db, iterator, stages, options) {
    AbstractIterator.call(this, db)
    this._stages = stages
    this._iterator = iterator
    this._release = release
    this._constraint = constrain(options)
    this._db = db
    this._versions = this._db._snapshot()
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
    this._paginator.release()
    this._paginator = this.db._paginator(target, this._options)
}

Iterator.prototype._end = function (callback) {
    this._paginator.release()
    callback()
}

function Locket (destructible, location, options = {}) {
    if (!(this instanceof Locket)) return new Locket(location)
    AbstractLevelDOWN.call(this)
    this.location = location
    this._destructible = destructible
    this._cache = new Cache
    this._primary = null
    this._stages = []
    this._maxStageCount = 1024
    const leaf = coalesce(options.leaf, {})
    this._leaf = {
        split: coalesce(leaf.split, 4096),
        merge: coalesce(leaf.merge, 2048)
    }
    const branch = coalesce(options.branch, {})
    this._branch = {
        split: coalesce(leaf.split, 4096),
        merge: coalesce(leaf.merge, 2048)
    }
}
util.inherits(Locket, AbstractLevelDOWN)

Locket.prototype._serializeKey = encode

Locket.prototype._serializeValue = encode

Locket.prototype._newStage = function (directory, options = {}) {
    const leaf = coalesce(options.leaf, 4096)
    const branch = coalesce(options.leaf, 4096)
    const strata = new Strata(this._destructible.ephemeral([ 'stage', options.directory ]), {
        directory: directory,
        branch: { split: branch, merge: 0 },
        leaf: { split: leaf, merge: 0 },
        cache: this._cache,
        serializer: {
            key: {
                serialize: function ({ version, value }) {
                    const header = { version: version }
                    const buffer = Buffer.alloc(key.sizeof(header))
                    key.serialize(header, buffer, 0)
                    return [ buffer, value ]
                },
                deserialize: function (parts) {
                    const { version } = parse(parts[0], 0)
                    return { version: version, value: parts[1] }
                }
            },
            parts: {
                serialize: function (parts) {
                    const { version, method, count } = parts[0]
                    const buffer = Buffer.alloc(packet.meta.sizeof(parts[0]))
                    packet.meta.serialize(parts[0], buffer, 0)
                    return [ buffer, parts[1], parts[2] ]
                },
                deserialize: function (parts) {
                    return [ packet.meta.parse(parts[0], 0), parts[1], parts[2] ]
                }
            }
        },
        extractor: function (parts) {
            return { value: parts[1], version: parts[0].version }
        },
        comparator: ascension([ Buffer.compare, BigInt ], function (object) {
            return [ object.value, object.version ]
        })
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
    // TODO Hoist.
    let exists = true
    this._options = options
    this._versions = { 0: true }
    this._version = 0n
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
    if (exists && options.errorIfExists) {
        throw new Error('Locket database already exists')
    }
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
    if (!exists) {
        for (const dir of subdirs) {
            await fs.mkdir(path.join(this.location, dir), { recursive: true })
        }
    }
    this._primary = new Strata(this._destructible, {
        directory: path.join(this.location, 'primary'),
        cache: this._cache,
        comparator: Buffer.compare,
        serializer: 'buffer',
        leaf: { merge: this._leaf.merge, split: this._leaf.split },
        branch: { merge: this._branch.merge, split: this._branch.split }
    })
    if ((await fs.readdir(path.join(this.location, 'primary'))).length != 0) {
        await this._primary.open()
    } else {
        await this._primary.create()
    }
    const staging = path.join(this.location, 'staging')
    for (const file of await fs.readdir(staging)) {
        const stage = this._newStage(path.join(staging, file)), counts = {}
        await stage.strata.open()
        for await (const items of mvcc.riffle.forward(stage.strata, Strata.MIN)) {
            for (const item of items) {
                const version = item.parts[0].version
                if (counts[version] == null) {
                    assert(item.parts[0].header.count != null)
                    counts[version] = item.parts[0].header.count
                }
                if (0 == --counts[version]) {
                    stage.versions[version] = true
                }
            }
        }
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
Locket.prototype._paginator = function (constraint) {
    this._stages.forEach(stage => stage.readers++)

    const versions = this._snapshot()
    const version = constraint.direction == 'forward' ? 0n : 0xffffffffffffffffn
    const key = constraint.key ? { value: constraint.key, version: version } : null

    const { direction, inclusive } = constraint

    const riffle = mvcc.riffle[direction](this._primary, key.value, 32, inclusive)
    const primary = mvcc.twiddle(riffle, item => {
        // TODO Looks like I'm in the habit of adding extra stuff, meta stuff,
        // so the records, so go back and ensure that I'm allowing this,
        // forwarding the meta information.
        return {
            key: { version: 0n, value: item.parts[0] },
            parts: [{
                header: { method: 'insert', count: 0 },
                version: 0n
            }, item.parts[0], item.parts[1]]
        }
    })

    const stages = this._stages.map(stage => {
        return mvcc.riffle[direction](stage.strata, key, 32, inclusive)
    })
    const homogenize = mvcc.homogenize[direction](Buffer.compare, stages.concat(primary))
    const designate = mvcc.designate[direction](Buffer.compare, homogenize, versions)
    const dilute = mvcc.dilute(designate, item => {
        if (item.parts[0].method == 'remove') {
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
    const constraint = constrain(key, encode, {
        gte: key, keyAsBuffer: true, valueAsBuffer: true
    })
    const paginator = this._paginator(constraint)
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
    await fs.rmdir(stage.path, { recursive: true })
    this._maybeUnstage()
}

Locket.prototype._maybeUnstage = function () {
    if (this._stages.length > 1) {
        const stage = this._stages[this._stages.length - 1]
        if (stage.amalgamated && stage.readers == 0) {
            this._destructible.ephemeral([ 'unstage', stage.path ], this._unstage())
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
        return item.parts[0].header.method == 'put' ? item.parts.slice(1) : null
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
    let cursor = Strata.nullCursor(), index = 0
    for (const operation of batch) {
        const { type: method, key: value } = operation, count = batch.length
        const key = { value: encode(value), version }
        index = cursor.indexOf(key, cursor.page.ghosts)
        if (index == null) {
            cursor.release()
            cursor = (await stage.strata.search(key)).get()
            index = cursor.index
            assert(!cursor.found)
        } else {
            assert(index < 0)
            index = ~index
        }
        const header = { header: { method, count }, version }
        if (method == 'put') {
            cursor.insert(index, [ header, operation.key, operation.value ], writes)
        } else {
            cursor.insert(index, [ header, operation.key ], writes)
        }
        stage.count++
    }
    cursor.release()
    await Strata.flush(writes)
    stage.versions[version] = this._versions[version] = true
    stage.writers--
    // A race to create the next stage, but the loser will merely create a stage
    // taht will be unused or little used.
    if (this._stages[0].count > this._maxStageCount) {
        const next = this._createStage()
        await next.create()
        this._stages.unshift(next)
    }
    this._maybeAmalgamate()
    return []
})

Locket.prototype._maybeAmalgamate = function () {
    if (this._stages.length != 1 && this._stages[this._stages.length - 1].writing == 0) {
        const stage = this._stages.pop()
        // Enqueue the amalgamation or else fire and forget.
        this._destructible.ephemeral('amalgamate', async () => {
            await this._amalgamate()
            this._maybeUnstage()
            this._maybeAmalgamate()
        })
    }
}

Locket.prototype._dequeue = async function ({ body }) {
    switch (body.method) {
    case 'batch': {
            const version = ++this._version
            const count = body.operations.length
            const stage = this._stages[0]
            let cursor = Strata.nullCursor(), index
            for (const operation of body.operations) {
                const key = {
                    method: operations.type == 'put' ? 'insert' : 'remove',
                    value: operation.key,
                    count: body.operations.length,
                    version: version
                }
                index = cursor.indexOf(key, cursor.page.ghosts)
                if (index == null) {
                    cursor.release()
                    cursor = await (stage.strata.search(key)).get()
                    index = cursor.index
                    assert(!cursor.found)
                } else {
                    assert(index < 0)
                    index = ~index
                }
                if (operation.type == 'put') {
                    cursor.insert(key, operation.value)
                } else {
                    cursor.insert(key, Buffer.alloc(0))
                }
                stage.count++
            }
            cursor.release()
            this._versions[version] = true
            this._maybeAmalgamate()
        }
        break
    // TODO Need some sort of promise on Destructible that is an all is good
    // promise. Gets difficult again, doesn't it. When Destructible exits, it
    // changes the promise from one that doesn't resolve to anything to one that
    // resolves to the error set by Destructible.
    // TODO No more switching back and forth. Use a timestamp for the file name
    // and create a new file amalgamate the old file. What are you supposed to
    // do about any outstanding iterations?
    case 'amalgamate': {
            // Can now go down to zero when all iterators are released. Oops, no
            // wait for `_amalgamate` to finish, duh.
            this._stages[0].holds--
            // Enqueue the amalgamation or else fire and forget.
            this._destructible.ephemeral('amalgamate', async () => {
                await this._amalgamate(this._stages[0])
                this._maybeAmalgamate()
            })
            // Create a new timestamped staging tree.
            await fs.abunch()
            // Add to stages.
            this._stages.unshift({
            })
        }
        break
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
        await this._primary.close()
        while (this._stages.length != 0) {
            await this._stages.shift().strata.close()
        }
        this._cache.purge(0)
    }
    return []
})
