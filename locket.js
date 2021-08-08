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

// Node.js API.
const util              = require('util')
const assert            = require('assert')

// Return the first value that is not `null` nor `undefined`.
const { coalesce }      = require('extant')

// Modules for storage and concurrency.
const Strata            = require('b-tree')

// A fiber-constrained `async`/`await` work queue.
const Turnstile         = require('turnstile')

// Write-ahead log.
const WriteAhead        = require('writeahead')

// LevelUp adaptors.
const AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
const AbstractIterator  = require('abstract-leveldown').AbstractIterator

// An `async`/`await` trampoline.
const Trampoline        = require('reciprocate')

// Structured concurrency.
const Destructible      = require('destructible')

// A Swiss Army asynchronous control-flow function generator for JavaScript.
const cadence           = require('cadence')

// A LRU cache for memory paging and content caching.
const Magazine          = require('magazine')

// Handle-based `async`/`await` file operations.
const Operation         = require('operation')

// WAL merge tree.
const Amalgamator       = require('amalgamate')
const Rotator           = require('amalgamate/rotator')

const Fracture          = require('fracture')


// Modules for file operations. We use `strftime` to create date stamped file
// names.
const fs                = require('fs').promises
const path              = require('path')

const constrain         = require('constrain')

// Conditionally catch JavaScript exceptions based on type and properties.
const rescue            = require('rescue')

// A comparator function builder.
const ascension         = require('ascension')

const mvcc = {
    satiate: require('satiate'),
    constrain: require('constrain/iterator')
}

// TODO Let's see if we can get throught his without having to worry about
// encodings.
function encode (buffer) {
    return Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer)
}

class Paginator {
    constructor (iterator, constraints, options) {
        const constrained = constraints == null
            ? iterator
            : mvcc.constrain(iterator, constraints)
        this._iterator = mvcc.satiate(constrained, 1)
        this._constraints = constraints
        this._keyAsBuffer = options.keyAsBuffer
        this._valueAsBuffer = options.valueAsBuffer
        this._keys = options.keys
        this._values = options.values
        this._items = []
        this._index = 0
    }

    next = cadence(function (step) {
        if (this._items.length != this._index) {
            const item = this._items[this._index++], result = new Array(2)
            if (this._keys) {
                result[0] = this._keyAsBuffer ? item.parts[1] : item.parts[1].toString()
            }
            if (this._values) {
                result[1] = this._valueAsBuffer ? item.parts[2] : item.parts[2].toString()
            }
            return result
        }
        let items = null
        step(function () {
            const trampoline = new Trampoline
            this._iterator.next(trampoline, $items => items = $items)
            trampoline.callback(step())
        }, function () {
            if (this._iterator.done) {
                return []
            } else {
                this._items = items
                this._index = 0
                this.next(step())
            }
        })
    })
}

const duplicated = ascension([ Buffer.compare, [ Number, -1 ], [ Number, -1 ] ])

function createConstraint (options) {
    const start = coalesce(options.gt, options.start, options.gte, null)
    const end = coalesce(options.lt, options.end, options.lte, null)
    const limit = coalesce(options.limit, -1)
    const reverse = coalesce(options.reverse, false)
    const direction = reverse ? 'reverse' : 'forward'
    const keys = [{
        comparator: duplicated,
        key: [ start, Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER ],
        inclusive: options.lt == null,
        limit: limit,
        direction: direction,
        reverse: reverse
    }, {
        comparator: duplicated,
        key: [ end, 0, 0 ],
        inclusive: options.gt == null,
        limit: limit,
        direction: direction,
        reverse: reverse
    }]
    if (reverse) {
        keys.reverse()
    }
    if (keys[1].limit == -1 && keys[1].key[0] == null) {
        keys[1] = null
    }
    keys[0].key = keys[0].key[0]
    return keys
}

// An implementation of the LevelDOWN `Iterator` object.
//
// The LevelUP interface allows you to specify encodings at both when you create
// the database and when you invoke one of it's functions, so we need to pass
// two different sets of options to all the functions that consider the
// encoding.

// TODO No call to super!
class Iterator extends AbstractIterator {
    constructor (db, options) {
        super(db)
        this._constraint = createConstraint(options)
        this._options = options
        this._db = db
        this._transaction = this._db._rotator.locker.snapshot()
        this._paginator = this._db._paginator(this._constraint, this._transaction, this._options)
    }

    // Our LevelUP `Iterator` implementation is a wrapper around the internal
    // iterator that merges our Strata b-tree with one or two logs where we are
    // gathering new batch operations. See the `Locket._internalIterator` method
    // for a description of how we compose an amalgamated MVCC iterator from the
    // MVCC iterator modules.

    //
    _next (callback) {
        this._paginator.next(callback)
    }

    _seek (target) {
        const paginator = this._paginator
        this._paginator = this.db._paginator(target, this._options)
        paginator.release()
    }

    _end (callback) {
        this._db._rotator.locker.release(this._transaction)
        callback()
    }
}

class Locket extends AbstractLevelDOWN {
    constructor (location, options = {}) {
        super()
        this.location = location
        this._cache = coalesce(options.cache, new Magazine),
        // TODO Allow common operation handle cache.
        this._versions = {}
        this._options = options
        this._version = 1n
        this._amalgamator = null
    }

    _serializeKey = encode

    _serializeValue = encode

    _open = cadence(function (step, options) {
        const destructible = this._destructible = new Destructible('locket')
        // TODO Only use the old callback `fs`.
        const fs = require('fs')
        // TODO What is the behavior if you close while opening, or open while closing?
        step(function () {
            step(function () {
                fs.readdir(this.location, step())
            }, function (files) {
                const exists = ([ 'wal', 'tree' ]).filter(file => ~files.indexOf(file))
                if (exists.length == 0) {
                    return false
                }
                if (exists.length == 2) {
                    return true
                }
                // TODO Interrupt or LevelUp specific error.
                throw new Error('partial extant database')
            }, function (exists) {
                if (! exists) {
                    step(function () {
                        fs.mkdir(path.resolve(this.location, 'wal'), { recursive: true }, step())
                    }, function () {
                        fs.mkdir(path.resolve(this.location, 'tree'), { recursive: true }, step())
                    }, function () {
                        return false
                    })
                } else {
                    return exists
                }
            })
        }, async function (exists) {
            const turnstile = new Turnstile(destructible.durable($ => $(), { isolated: true }, 'turnstile'))
            const writeahead = new WriteAhead(destructible.durable($ => $(), 'writeahead'), turnstile, await WriteAhead.open({
                directory: path.resolve(this.location, 'wal')
            }))
            this._rotator = new Rotator(destructible.durable($ => $(), 'rotator'), await Rotator.open(writeahead), { size: 1024 * 1024  })
            // TODO Make strands user optional.
            return this._rotator.open(Fracture.stack(), 'locket', {
                handles: new Operation.Cache(new Magazine),
                directory: path.resolve(this.location, 'tree'),
                create: ! exists,
                cache: this._cache,
                key: 'tree',
                // TODO Use CRC32 or FNV.
                checksum: () => '0',
                extractor: parts => parts[0],
                serializer: {
                    key: {
                        serialize: key => [ key ],
                        deserialize: parts => parts[0]
                    },
                    parts: {
                        serialize: parts => parts,
                        deserialize: parts => parts
                    }
                },
            }, {
                pages: new Magazine,
                turnstile: turnstile,
                comparator: Buffer.compare,
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
                primary: options.primary || {
                    leaf: { split: 1024 * 32, merge: 32 },
                    branch: { split: 1024 * 32, merge: 32 },
                },
                stage: options.stage || {
                    leaf: { split: 1024 * 1024 * 1024, merge: 32 },
                    branch: { split: 1024 * 1024 * 1024, merge: 32 },
                }
            })
        }, function (amalgamator) {
            this._amalgamator = amalgamator
        }, function () {
            return []
        })
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
    _paginator = function (constraint, transaction, options) {
        const [{ key, inclusive, direction }, constraints ] = constraint
        const iterator = this._amalgamator.iterator(transaction, direction, key, inclusive)
        return new Paginator(iterator, constraints, options)
    }

    _iterator = function (options) {
        return new Iterator(this, options)
    }

    // TODO Maybe just leave this?
    _get = cadence(function (step, key, options) {
        const constraint = createConstraint({ gte: key, lte: key })
        const snapshot = this._rotator.locker.snapshot()
        const paginator = this._paginator(constraint, snapshot, {
            keys: true, values: true, keyAsBuffer: true, valueAsBuffer: true
        })
        step(function () {
            paginator.next(step())
        }, [], function (next) {
            this._rotator.locker.release(snapshot)
            if (next.length != 0 && Buffer.compare(next[0], key) == 0) {
                return [ options.asBuffer ? next[1] : next[1].toString() ]
            }
            throw new Error('NotFoundError: not found')
        })
    })

    _put = function (key, value, options, callback) {
        this._batch([{ type: 'put', key: key, value: value }], options, callback)
    }

    _del = function (key, options, callback) {
        this._batch([{ type: 'del', key: key }], options, callback)
    }

    // Could use a header record. It would sort out to be less than all the user
    // records, so it wouldn't get in the way of a search, and we wouldn't have to
    // filter it. It does however mean at least two writes for every `put` or `del`
    // and I suspect that common usage is ingle `put` or `del`, so going to include
    // the count in ever record, it is only 32-bits.
    _batch = cadence(function (step, batch, options) {
        const mutator = this._rotator.locker.mutator()
        step(function () {
            return this._amalgamator.merge(Fracture.stack(), mutator, batch)
        }, function () {
            return this._rotator.commit(Fracture.stack(), mutator.mutation.version)
        }, function () {
            this._rotator.locker.commit(mutator)
            return []
        })
    })

    _approximateSize = cadence(function (from, to) {
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
    _close = cadence(function (step) {
        step(function () {
            return this._amalgamator.drain()
        }, function () {
            return this._destructible.destroy().promise
        }, function () {
            return []
        })
    })
}

module.exports = Locket
