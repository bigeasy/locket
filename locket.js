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

const Trampoline = require('reciprocate')

const Destructible = require('destructible')

const Amalgamator = require('amalgamate')
const Locker = require('amalgamate/locker')

const callbackify = require('prospective/callbackify')

const cadence = require('cadence')
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
    riffle: require('riffle')
}

// TODO Let's see if we can get throught his without having to worry about
// encodings.
function encode (buffer) {
    return Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer)
}

class Paginator {
    constructor (iterator, constraint) {
        this._iterator = iterator
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
                } else {
                    return []
                }
            } else {
                let items = null
                const trampoline = new Trampoline
                this._iterator.next(trampoline, $items => items = $items)
                while (trampoline.seek()) {
                    await trampoline.shift()
                }
                if (this._iterator.done) {
                    return []
                } else {
                    this._items = items
                    this._index = 0
                }
            }
        }
    }

    release () {
        this._iterator['return']()
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
    this._transaction = this._db._locker.snapshot()
    this._paginator = this._db._paginator(this._constraint, this._transaction)
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
    this._db._locker.release(this._transaction)
    callback()
}

function Locket (destructible, location, options = {}) {
    if (!(this instanceof Locket)) return new Locket(destructible, location, options)
    AbstractLevelDOWN.call(this)
    this.location = location
    this._cache = new Cache
    this._versions = {}
    this._options = options
    this._version = 1n
    this._amalgamator = null
}
util.inherits(Locket, AbstractLevelDOWN)

Locket.prototype._serializeKey = encode

Locket.prototype._serializeValue = encode

Locket.prototype._open = callbackify(async function (options) {
    this._locker = new Locker({ heft: coalesce(options.heft, 1024 * 1024) })
    // TODO What is the behavior if you close while opening, or open while
    // closing?
    this._amalgamator = new Amalgamator(new Destructible('locket'), {
        locker: this._locker,
        directory: this.location,
        cache: new Cache,
        comparator: Buffer.compare,
        parts: {
            serialize: function (parts) { return parts },
            deserialize: function (parts) { return parts }
        },
        key: {
            compare: Buffer.compare,
            extract: function (parts)  {
                return parts[0]
            },
            serialize: function (key) {
                return [ key ]
            },
            deserialize: function (parts) {
                return parts[0]
            }
        },
        transformer: function (operation) {
            if (operation.type == 'put') {
                return {
                    method: 'insert',
                    key: operation.key,
                    parts: [ operation.key, operation.value ]
                }
            }
            return {
                method: 'remove',
                key: operation.key
            }
        },
        ...this._options,
        ...options
    })
    await this._amalgamator.ready
    await this._amalgamator.count()
    await this._amalgamator.locker.rotate()
    return []
})

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
Locket.prototype._paginator = function (constraint, transaction) {
    const { key, direction, inclusive } = constraint
    const iterator = this._amalgamator.iterator(transaction, direction, key, inclusive)
    return new Paginator(iterator, constraint)
}

Locket.prototype._iterator = function (options) {
    return new Iterator(this, options)
}

// TODO Maybe just leave this?
Locket.prototype._get = cadence(function (step, key, options) {
    const constraint = constrain(Buffer.compare, encode, {
        gte: key, keys: true, values: true, keyAsBuffer: true, valueAsBuffer: true
    })
    const snapshot = this._locker.snapshot()
    const paginator = this._paginator(constraint, snapshot)
    step(function () {
        return paginator.next()
    }, function (next) {
        this._locker.release(snapshot)
        if (next.length != 0 && Buffer.compare(next[0], key) == 0) {
            return [ options.asBuffer ? next[1] : next[1].toString() ]
        }
        throw new Error('NotFoundError: not found')
    })
})

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
    const version = ++this._version
    this._versions[version] = true
    const mutator = this._locker.mutator()
    await this._amalgamator.merge(mutator, batch, true)
    this._locker.commit(mutator)
    return []
})

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
    await this._amalgamator.destructible.destroy().rejected
    return []
})
