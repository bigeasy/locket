module.exports = Locket

var Sequester         = require('sequester')
var Strata            = require('b-tree')
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
var AbstractIterator  = require('abstract-leveldown').AbstractIterator

var ok   = require('assert')
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
    this._sequester = new Sequester
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
        // actually, we can open really fast, if...
        // ...if we allow that iterator up there to merge any number of trees
        // ...if we just create an empty log and go, but consult all trees in the iterator.
        // ...then when we merge, we simply pop a tree off the end.
        // we open really fast by simply creating a new empty tree at the head,
        // then merging everything at our liesure.
        this._staging = this._secondary // TODO: Gotta merge now.
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

function Merge (db) {
    this._db = db
}

Merge.prototype.update = cadence(function (step) {
    var insert = step(function () {
        if (!this._primary) {this._db._primary.mutator(candidate.key, step(step, function ($) {
            this._primary = $
            return this._primary.index
        }))} else {
            this._primary.indexOf(candidate.key, step())
        }
    }, function (index) {
        if (index < 0) return ~ index
        else step(function () {
            this._primary.remove(index, step())
        }, function () {
            return index
        })
    }, function (index) {
        if (candidate.type == 'put') step(function () {
            this._primary.insert(candiate.key, { key: candidate.key, value: candidate.value }, index, step())
        }, function (result) {
            if (result != 0) {
                ok(result > 0, 'went backwards')
                this._primary.unlock()
                delete this._primary
                step(insert)
            }
        })
    })(1)
})

Merge.prototype.merge = cadence(function (step) {
    var unmerged
    step([function () {
        if (this._primary) this._primary.unlock()
    }], function () {
        this._db._sequester.exclude(step())
    }, function () {
        this._db._sequester.share(step(step, [function () { this._db._sequester.unlock() }]))
        unmerged = this._db._staging
        this._db._staging = unmerged == 'secondary' ? 'tertiary' : 'secondary'
        this._db._sequester.unlock()
    }, function () {
        var tree = this['_' + unmerged]
        tree.iterator(step())
    }, function (stage) {
        var candidate = { transactionId: 0 }
        // use a mutator on the stage, the nice thing is that so long as we hold
        // the mutator on the stage, the Locket iterator will block until we've
        // moved the key, we're moving through the leaf pages, deleting
        // everything, or, hey, uh, why don't we have a quadiary tree?
        //
        // okay, also, let's just queue of this shifting, have a counter, when
        // the user calls merge, just flip a counter? yes.
        //
        // mrph, we could move the merged stage into an archive folder, with a
        // date stamped name, and then rm -rf the directory, or, optionally, not
        // rm -rf the directory, preserving all logs for every body, forever.
        step(function (more) {
            if (!more) {
                step(null)
            } else {
                step(function (index) {
                    index += stage.offset
                    step(function () {
                        stage.get(index, step())
                    }, function (record) {
                        if (this._db._transactions[record.transactionId]) {
                            if (candidate.key != record.key) {
                                this._greatestTransactionId = Math.max(candidate.transactionId, this._greatestTransactionId)
                                delete this._db._transactions[candidate.transactionId]
                                this._update(candidate, step())
                            }
                            record = candidate
                        }
                    })
                })(stage.length - stage.offset)
            }
        }, function () {
            stage.next(step())
        })(null, true)
    }, function () {
        if (this._primary) this._primary.unlock()
        delete this._primary
    }, function () {
        this._db._sequester.exclude(step())
    }, function () {
        var stages = this._db.stages
    })
})

Locket.prototype._merge = cadence(function (step) {
    this._mergeRequests++
    if (this._mergeRequests == 0) {
        new Merge(this).merge(step())
    }
})

Locket.prototype._batch = cadence(function (step, array, options) {
    var transaction = { id: this._nextTransactionId++ }
    var staging = this._staging
    step(function () {
        this._sequester.share(step(step, [function () { this._sequester.unlock() }]))
    }, function () {
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
