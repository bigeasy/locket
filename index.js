module.exports = Locket

var alphabet = '\
    able baker charlie dog easy fox george how item \
    jig king love mike nan oboe peter queen roger \
    sugar tare uncle victor william x-ray yoke zebra \
'.trim().split(/\s+/)

var Sequester         = require('sequester')
var Strata            = require('b-tree')
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
var AbstractIterator  = require('abstract-leveldown').AbstractIterator

var tz   = require('timezone')
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

Iterator.prototype._forward = cadence(function (step, name) {
    var cursor = this._cursors[name].cursor
    var index = this._cursors[name].index
    delete this._cursors[name].record
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
        // todo: check that id is in succuessful transaction.
            step(function () {
                if (++index < cursor.length) return true
                else step(function () {
                    cursor.next(step())
                }, function (more) {
                    if (more)  {
                        index = cursor.index
                    } else {
                        delete this._cursors[name].cursor
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
            delete this._cursors[name].cursor
            cursor.unlock()
            return null
        }
    }, function (record) {
        this._cursors[name].index = index
        if (record) {
            record.transactionId = record.transactionId || 0
            this._cursors[name].record = record
        } else {
            delete this._cursors[name].record
        }
        return record
    })
})

Iterator.prototype._next = cadence(function (step) {
    var db = this._db
    step(function () {
        if (!this._cursors) step(function () {
            this._cursors = {}
            step(function (stage) {
                step(function () {
                    console.log({ key: this._start, transactionId: 0 })
                    stage.tree.iterator(stage.name == 'primary'
                                       ? this._start
                                       : { key: this._start, transactionId: 0 }, step())
                }, function (cursor) {
                    var index = cursor.index < 0 ? ~ cursor.index : cursor.index
                    this._cursors[stage.name] = {
                        name: stage.name,
                        cursor: cursor,
                        index: index
                    }
                    this._forward(stage.name, step())
                })
            })([ { name: 'primary', tree: this._db._primary } ].concat(this._db._stages))
        })
    }, function () {
        var active = Object.keys(this._cursors).filter(function (name) {
            return ('record' in this._cursors[name])
        }.bind(this))

        if (active.length) {
            var candidates = active.map(function (name) {
                return this._cursors[name]
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
                    this._forward(candidate.name, step())
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
    return record.key
}

function compareKey (left, right) {
    if (left < right) return -1
    if (left > right) return 1
    return 0
}

function extractKeyAndTransaction (record) {
    return { key: record.key, transactionId: record.transactionId }
}

function compareKeyAndTransaction (left, right) {
    if (left.key < right.key) return -1
    if (left.key > right.key) return 1
    return left.transactionId - right.transactionId
}

function createStageStrata (letter) {
    return new Strata({
        directory: path.join(this.location, 'stages', letter),
        extractor: extractKeyAndTransaction,
        comparator: compareKeyAndTransaction,
        leafSize: 1024,
        branchSize: 1024
    })
}

var createStage = cadence(function (step, letter) {
    var strata = createStageStrata.call(this, letter)
    var stage = { name: letter, tree: strata }

    step (function () {
        mkdirp(path.join(this.location, 'stages', letter), step())
    }, function () {
        strata.create(step())
    }, function () {
        strata.open(step())
    }, function () {
        return stage
    })
})

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
        var subdirs = [ 'archive', 'primary', 'stages', 'transactions' ]
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
        this._transactions = new Strata({
            directory: path.join(this.location, 'transactions'),
            leafSize: 1024,
            branchSize: 1024
        })
        if (!exists) this._transactions.create(step())
    }, function () {
        this._primary.open(step())
        this._transactions.open(step())
    }, function () {
        fs.readdir(path.join(this.location, 'stages'), step())
    }, function (letters) {
        step(function () {
            letters.forEach(step([], function (letter) {
                var strata = createStageStrata.call(this, letter)
                step(function () {
                    strata.open(step())
                }, function () {
                    return { name: letter, tree: strata }
                })
            }))
        }, function (stages) {
            this._stages = stages
            var letter = alphabet.filter(function (letter) { return !~letters.indexOf(letter) }).shift()
            createStage.call(this, letter, step())
        }, function (stage) {
            this._stages.unshift(stage)
        })
    }, function () {
        this._isOpened = true
        this._operations = 0
        this._mergeRequests = 0
        this._successfulTransactions = {}
        this._nextTransactionId = 1
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
            })(length - offset)
        }, function () {
            transactions.next(step())
        })(null, true)
    })
})

Locket.prototype._get = cadence(function (step, key, options) {
    var iterator = new Iterator(this, { start: key, limit: 1 })
    step(function () {
        iterator.next(step())
    }, function ($key, value) {
        console.log($key, value)
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
    this._greatest = 0
}

Merge.prototype.update = cadence(function (step, record) {
    this._greatest = Math.max(record.transactionId, this._greatest)
    var insert = step(function () {
        if (!this._primary) {this._db._primary.mutator(record.key, step(step, function ($) {
            this._primary = $
            return this._primary.index
        }))} else {
            this._primary.indexOf(record.key, step())
        }
    }, function (index) {
        if (index < 0) return ~ index
        else step(function () {
            throw new Error // we don't wanna go ehre.
            this._primary.remove(index, step())
        }, function () {
            return index
        })
    }, function (index) {
        if (record.type == 'put') step(function () {
            // todo: probably re-extract?
            this._primary.insert({ key: record.key, value: record.value }, record.key, index, step())
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
    // todo: track both greatest and least transaction id.
    var candidate = { transactionId: 0 }
    var shared = 0
    var unmerged
    step([function () { // gah! cleanup! couldn't see it. ack! ack! ack!
        if (this._primary) this._primary.unlock()
        if (shared) this._db._sequester.unlock()
    }], function () {
        this._db._sequester.exclude(step())
    }, function () {
        unmerged = this._db._stages[this._db._stages.length - 1]
        this._db._sequester.share(step())
        this._db._sequester.unlock()
    }, function () {
        shared++
        unmerged.tree.iterator(step())
    }, function (stage) {
        step(function (more) {
            if (!more) {
                stage.unlock()
                // do you need to unlock when you hit the end?
                step(null)
            } else {
                step(function (index) {
                    index += stage.offset
                    step(function () {
                        stage.get(index, step())
                    }, function (record) {
                        if (this._db._successfulTransactions[record.transactionId]) {
                            if (candidate.transactionId && candidate.key != record.key) {
                                // what? nooooo. what? whatcha doin' there champ?
                                //delete this._db._successfulTransactions[candidate.transactionId]
                                this.update(candidate, step())
                            }
                            candidate = record
                        }
                    })
                })(stage.length - stage.offset)
            }
        }, function () {
            stage.next(step())
        })(null, true)
    }, function () {
        if (candidate.transactionId) {
            this.update(candidate, step())
        }
    }, function () {
        if (this._primary) this._primary.unlock()
        delete this._primary
    }, function () {
        this._db._sequester.exclude(step())
        shared--
        this._db._sequester.unlock()
    }, function () {
        this._db._stages.pop()
        this._db._sequester.share(step())
        this._db._sequester.unlock()
    }, function () {
        // todo: purge transactions
        shared++
        var from = path.join(this._db.location, 'stages', unmerged.name)
        var filename = tz(Date.now(), '%F-%H-%M-%S-%3N')
        var to = path.join(this._db.location, 'archive', filename)
        // todo: note that archive and stage need to be on same file system.
        step(function () {
            fs.rename(from, to, step())
        }, function () {
            // todo: rimraf the archive file if we're not preserving the archive
        })
    })
})

Locket.prototype._merge = cadence(function (step) {
    if (this._mergeRequests++ == 0) {
        step(function () {
            // first merge any extras down to one.
            step(function () {
                if (this._stages.length == 1) step(null)
                else new Merge(this).merge(step())
            })()
        }, function () {
            // add a new stage.
            var letter = alphabet[(alphabet.indexOf(this._stages[0].name) + 1) % alphabet.length]
            // we need to stop the world just long enough to unshift the new
            // stage, it will happen in one tick, super quick.
            step(function () {
                createStage.call(this, letter, step())
            }, function (stage) {
                step(function () {
                    this._sequester.exclude(step())
                }, function () {
                    this._stages.unshift(stage)
                    this._sequester.unlock()
                })
            })
        }, function () {
            // once again, first merge any extras down to one.
            step(function () {
                if (this._stages.length == 1) step(null)
                else new Merge(this).merge(step())
            })()
        })
    }
})

Locket.prototype._batch = cadence(function (step, array, options) {
    var transaction = { id: this._nextTransactionId++ }
    var tree = this._stages[0].tree
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
                    tree.mutator(record, step())
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
                this._successfulTransactions[transaction.id] = true
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
