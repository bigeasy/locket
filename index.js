module.exports = Locket

var Strata            = require('b-tree')
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
var AbstractIterator  = require('abstract-leveldown').AbstractIterator

var util = require('util')
var fs   = require('fs')
var path = require('path')

var cadence = require('cadence')
var mkdirp  = require('mkdirp')

function Iterator (db, options) {
    this._db = db
    this._start = options.start
    this._limit = options.limit
}
util.inherits(Iterator, AbstractIterator)

Iterator.prototype._getLatestGoingForward = cadence(function (step, collection) {
    var cursor = this._cursors[collection].cursor
    var index = this._cursors[collection].index
    delete this._cursors[collection].record
    if (cursor) step(function () {
        if (index < cursor.length) return true
        else step(function () {
            cursor.next(step())
        }, function (more) {
            index = cursor.index
            return more
        })
    }, function (more) {
        if (more) {step(function () {
            cursor.get(index, step())
        }, function (record) {
            step(function () {
                if (++index < cursor.length) return true
                else step(function () {
                    cursor.next(step())
                }, function (more) {
                    if (more)  {
                        index = cursor.index
                    } else {
                        delete this._cursors[collection].cursor
                        cursor.unlock()
                    }
                    return more
                })
            }, function (more) {
                if (more) cursor.get(index, step())
                else step(null, record)
            }, function (next) {
                if (next.key == record.key) record = next
                else step(null, record)
            })()
        })} else {
            delete this._cursors[collection].cursor
            cursor.unlock()
            return null
        }
    }, function (record) {
        this._cursors[collection].index = index
        if (record) {
            record.transactionId = record.transactionId || 0
            this._cursors[collection].record = record
        } else {
            delete this._cursors[collection].record
        }
        return record
    })
})

Iterator.prototype._next = cadence(function (step) {
    var db = this._db
    step(function () {
        if (!this._cursors) step(function () {
            this._cursors = {}
            step(function (collection) {
                var tree = db['_' + collection]
                step(function () {
                    tree.iterator({ key: this._start, transactionId: 0 }, step())
                }, function (cursor) {
                    var index = cursor.index < 0 ? ~ cursor.index : cursor.index
                    this._cursors[collection] = {
                        name: collection,
                        cursor: cursor,
                        index: index
                    }
                })
            })([ 'primary', 'secondary', 'tertiary' ])
        }, function () {
            ; [ 'primary', 'secondary', 'tertiary' ].forEach(step([], function (collection) {
                this._getLatestGoingForward(collection, step())
            }))
        })
    }, function () {
        var active = [ 'primary', 'secondary', 'tertiary' ].filter(function (collection) {
            return ('record' in this._cursors[collection])
        }.bind(this))

        if (active.length) {
            var candidates = active.map(function (collection) {
                return this._cursors[collection]
            }.bind(this))

            var key = candidates.reduce(function (previous, current) {
                return previous.record.key < current.record.key ? previous : current
            }).record.key

            candidates = candidates.filter(function (candidate) {
                return key == candidate.record.key
            })

            var winner = candidates.reduce(function (previous, current) {
                return previous.record.transactionId > current.record.transactionId
                     ? previous : current
            }).record

            step(function () {
                candidates.forEach(step([], function (candidate) {
                    this._getLatestGoingForward(candidate.name, step())
                }));
            }, function () {
                if (winner.type == 'del') this._next(step())
                else step()(null, winner.key, winner.value)
            })
        } else {
            step()(new Error('not found'))
        }
    })
})


function Locket (location) {
    if (!(this instanceof Locket)) return new Locket(location)
    AbstractLevelDOWN.call(this, location)
}

util.inherits(Locket, AbstractLevelDOWN)

function extractKey (record) {
    return { key: record.key }
}

function compareKey (leaf, right) {
    if (left.key < right.key) return -1
    else if (left.key > right.key) return 1
    return 0
}

function extractKeyAndTransaction (record) {
    return { key: record.key, transactionId: record.transactionId }
}

function compareKeyAndTransaction (left, right) {
    if (left.key < right.key) return -1
    else if (left.key > right.key) return 1
    return left.transactionId - right.transactionId
}

Locket.prototype._open = cadence(function (step, options) {
    var exists = true
    step(function () {
        var readdir = step([function () {
            fs.readdir(this.location, step())
        }, 'ENOENT', function (_, error) {
            if (options.createIfMissing) {
                exists = false
                mkdirp(this.location, step(readdir, 0))
            } else {
                throw new Error('does not exist')
            }
        }])(1)
    }, function (listing) {
        var subdirs = [ 'primary', 'secondary', 'tertiary', 'transactions' ]
        if (exists) {
          listing = listing.filter(function (file) { return file[0] != '.' }).sort()
          if (listing.length && !listing.every(function (file, index) { return subdirs[index] == file })) {
              throw new Error('not a Locket datastore')
          }
        } else {
          subdirs.forEach(step([], function (dir) {
              fs.mkdir(path.join(this.location, dir), step())
          }))
        }
    }, function () {
        this._primary = new Strata({
            directory: path.join(this.location, 'primary'),
            extractor: extractKey,
            comparator: compareKey,
            leafSize: 1024,
            branchSize: 1024
        })
        if (!exists) this._primary.create(step())
        this._secondary = new Strata({
            directory: path.join(this.location, 'secondary'),
            extractor: extractKeyAndTransaction,
            comparator: compareKeyAndTransaction,
            leafSize: 1024,
            branchSize: 1024
        })
        if (!exists) this._secondary.create(step())
        this._tertiary = new Strata({
            directory: path.join(this.location, 'tertiary'),
            extractor: extractKeyAndTransaction,
            comparator: compareKeyAndTransaction,
            leafSize: 1024,
            branchSize: 1024
        })
        if (!exists) this._tertiary.create(step())
        this._transactions = new Strata({
            directory: path.join(this.location, 'transactions'),
            leafSize: 1024,
            branchSize: 1024
        })
        if (!exists) this._transactions.create(step())
    }, function () {
        this._primary.open(step())
        this._secondary.open(step())
        this._tertiary.open(step())
        this._transactions.open(step())
    }, function () {
        this._isOpened = true
        this._operations = 0
        this._successfulTransactions = {}
        this._leastTransactionId = Number.MAX_VALUE
        this._transactionIds = {}
        this._nextTransactionId = 1
        this._staging = this._secondary
        this._transactions.iterator(step())
    }, function (transactions) {
        step(function (more) {
            if (!more) {
                transactions.unlock()
                step(null)
            }
        }, function () {
            var offset = transactions.offset
            var length = transactions.length
            step(function (i) {
                cursor.get(i + offset, step())
            }, function (transactionId) {
                this._successfulTransactions[transactionId] = true
                this._nextTransactionId = Math.max(this._nextTransactionId, transactionId + 1)
                this._leastTransactionId = Math.min(this._leastTransactionId, transactionId - 1)
            })(length - offset)
        }, function () {
            transactions.next(step())
        })(null, true)
    }, function () {
        this._leastTransactionId = Math.min(this._leastTransactionId, this._nextTransactionId - 1)
    })
})

Locket.prototype._get = cadence(function (step, key, options) {
    var iterator = new Iterator(this, { start: key, limit: 1 })
    step(function () {
        iterator.next(step())
    }, function ($key, value) {
        step(function () {
            iterator.end(step())
        }, function () {
            if ($key == key) return step()(null, value)
            else step()(new Error('not found'))
        })
    })
})

Locket.prototype._put = function (key, value, options, callback) {
    this._batch([{ type: 'put', key: key, value: value }], options, callback)
}

Locket.prototype._del = function (key, options, callback) {
    this._batch([{ type: 'del', key: key }], options, callback)
}

Locket.prototype._batch = cadence(function (step, array, options) {
    var transaction = { id: this._nextTransactionId++ }
    var staging = this._staging
    step(function () {
        step(function (operation) {
            var record = {
                type: operation.type,
                transactionId: transaction.id,
                key: operation.key,
                value: operation.value
            }
            step(function () {
                if (transaction.cursor) {
                    return transaction.cursor
                } else {
                    staging.mutator(record, step())
                }
            }, function (cursor) {
                step(function () {
                    if (transaction.cursor) {
                        cursor.indexOf(record, step())
                    } else {
                        transaction.cursor = cursor
                        return cursor.index
                    }
                }, function (index) {
                    if (index < 0) return index
                    else step(function () {
                        cursor.remove(index, step())
                    }, function () {
                        return ~ index
                    })
                }, function (index) {
                    step(function () {
                        cursor.insert(record, record, ~ index, step())
                    }, function (insert) {
                        if (insert != 0) {
                            delete transaction.cursor
                            cursor.unlock()
                            operations.put(call, this, transaction, options, operation)
                        } else {
                            this._operations++
                        }
                    })
                })
            })
        })(array)
    }, function () {
        if (transaction.cursor) transaction.cursor.unlock()
        step(function () {
            this._transactions.mutator(transaction.id, step())
        }, function (cursor) {
            step(function () {
                cursor.insert(transaction.id, transaction.id, ~ cursor.index, step())
            }, function () {
                cursor.unlock()
            })
        })
    })
})

Locket.prototype._close = cadence(function (step, operations) {
    if (this._isOpened) step(function () {
        this._primary.close(step())
    }, function () {
        this._isOpened = false
    })
})

// vim: sw=4 ts=4:
