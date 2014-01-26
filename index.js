module.exports = Locket

var sequester         = require('sequester')
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

var pair = require('pair')
var constrain = require('constrain')

function echo (object) { return object }

var mvcc = {
    revise: require('revise'),
    riffle: require('riffle'),
    advance: require('advance'),
    skip: require('skip'),
    splice: require('splice'),
    designate: require('designate'),
    amalgamate: require('amalgamate'),
    dilute: require('dilute')
}

function isTrue (options, property, defaultValue) {
    return !!((property in options) ? options[property] : defaultValue)
}

function Iterator (db, options) {
    var preferences = [ options, db._options ]
    this._db = db
    this._range = constrain(pair.compare, function (key) {
        return Buffer.isBuffer(key) ? key : pair.encoder.key(preferences).encode(key)
    }, options)
    this._versions = this._db._snapshot()
    this._decoders = {
        key: isTrue(options, 'keyAsBuffer', true) ? echo : pair.encoder.key(preferences).decode,
        value: isTrue(options, 'valueAsBuffer', true) ? echo : pair.encoder.value(preferences).decode
    }
}
util.inherits(Iterator, AbstractIterator)

Iterator.prototype._next = cadence(function (step) {
    step(function () {
        if (this._iterator) return this._iterator
        this._db._dilution(this._range, this._versions, step('_iterator'))
    }, function (iterator) {
        iterator.next(step())
    }, function (record, key) {
        if (record) {
            step(null, this._decoders.key(record.key), this._decoders.value(record.value))
        }
    })
})

Iterator.prototype._end = function (callback) {
    this._iterator.unlock()
    callback()
}

function Locket (location) {
    if (!(this instanceof Locket)) return new Locket(location)
    AbstractLevelDOWN.call(this, location)
    this._sequester = sequester.createLock()
    this._merging = sequester.createLock()
}
util.inherits(Locket, AbstractLevelDOWN)

Locket.prototype._snapshot = function () {
    var versions = {}
    for (var key in this._versions) {
        versions[key] = true
    }
    return versions
}

Locket.prototype._dilution = cadence(function (step, range, versions) {
    step(function () {
        var iterators = []
        step(function () {
            step(function (stage) {
                mvcc.skip[range.direction](
                    stage.tree, pair.compare, versions, {}, range.key, step()
                )
            }, function (iterator) {
                iterators.push(iterator)
            })([ { tree: this._primary } ].concat(this._stages))
        }, function () {
            return iterators
        })
    }, function (iterators) {
        mvcc.designate[range.direction](pair.compare, function (record) {
            return record.operation == 'del'
        }, iterators, step())
    }, function (iterator) {
        return mvcc.dilute(iterator, function (key) {
            return range.valid(key.value)
        })
    })
})

var extractor = mvcc.revise.extractor(pair.extract)
function createStageStrata (name) {
    return new Strata({
        directory: path.join(this.location, 'stages', name),
        extractor: extractor,
        comparator: mvcc.revise.comparator(pair.compare),
        serialize: pair.serializer,
        deserialize: pair.deserializer,
        leafSize: 1024,
        branchSize: 1024
    })
}

var createStage = cadence(function (step, name) {
    name = String(name)

    var strata = createStageStrata.call(this, name)
    var stage = { name: name, tree: strata }

    step (function () {
        mkdirp(path.join(this.location, 'stages', name), step())
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
    this._options = options
    step(function () {
        var readdir = step([function () {
            fs.readdir(this.location, step())
        }, /^ENOENT$/, function (_, error) {
            if (options.createIfMissing == null || options.createIfMissing) {
                exists = false
                mkdirp(this.location, step(readdir, 0))
            } else {
                throw new Error('does not exist')
            }
        }])(1)
    }, function (listing) {
        if (exists && options.errorIfExists) {
            throw new Error('Locket database already exists')
        }
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
            extractor: mvcc.revise.extractor(pair.extract),
            comparator: mvcc.revise.comparator(pair.compare),
            serialize: pair.serializer,
            deserialize: pair.deserializer,
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
    }, function (files) {
        // todo: replication is probably going to mean that no stages is okay.
        if (exists && !files.length) {
            throw new Error('no stages, what happened?')
        }
        files.sort().reverse()
        step(function () {
            files.forEach(step([], function (letter) {
                var strata = createStageStrata.call(this, letter)
                step(function () {
                    strata.open(step())
                }, function () {
                    return { name: letter, tree: strata }
                })
            }))
        }, function (stages) {
            this._stages = stages
            createStage.call(this, files.length ? +(files[0]) + 1 : 1, step())
        }, function (stage) {
            this._stages.unshift(stage)
        })
    }, function () {
        this._isOpened = true
        this._operations = 0
        this._mergeRequests = 0
        this._versions = { 0: true }
        this._version = 1
        mvcc.riffle.forward(this._transactions, step())
    }, function (transactions) {
        step([function () {
            transactions.unlock()
        }], function () {
            step(function () {
                transactions.next(step(false))
            }, function (version) {
                this._versions[version] = true
                this._version = Math.max(this._version, version)
            })()
        })
    })
})

Locket.prototype._get = cadence(function (step, key, options) {
    if (!Buffer.isBuffer(key)) {
        key = pair.encoder.key([ options, this._options ]).encode(key)
    }
    var iterator = this._iterator({ start: key, limit: 1 })
    step(function () {
        iterator.next(step())
    }, function ($key, value) {
        step(function () {
            iterator.end(step())
        }, function () {
            if ($key && value && pair.compare($key, key) == 0) {
                if (!isTrue(options, 'asBuffer', true)) {
                    value = pair.encoder.value([ options, this._options ]).decode(value)
                }
                step(null, value)
            } else {
                step(new Error('NotFoundError: not found'))
            }
        })
    })
})

Locket.prototype._put = function (key, value, options, callback) {
    this._batch([{ type: 'put', key: key, value: value }], options, callback)
}

Locket.prototype._del = function (key, options, callback) {
    this._batch([{ type: 'del', key: key }], options, callback)
}

Locket.prototype._iterator = function (options) {
    return new Iterator(this, options)
}

Locket.prototype._stageTrees = function (index) {
    return this._stages.slice(index).map(function (stage) { return stage.tree })
}

Locket.prototype._merge = cadence(function (step) {
    var merged = {}
    step(function () {
        this._merging.exclude(step(step, [function () { this._merging.unlock() }]))
    }, function () {
        // add a new stage.
        //
        // we need to stop the world just long enough to unshift the new
        // stage, it will happen in one tick, super quick.
        step(function () {
            // todo: rename name to count.
            // todo: need to put next "name" in memory and increment, this
            // duplicates.
            createStage.call(this, +(this._stages[0].name) + 1, step())
        }, function (stage) {
            step(function () {
                this._sequester.exclude(step())
            }, function () {
                this._stages.unshift(stage)
                this._sequester.unlock()
            })
        })
    }, function () {
        this._sequester.share(step(step, [function () { this._sequester.unlock() }]))
    }, function () {
        // make serial
        this._stageTrees(1).forEach(step([], function (tree) {
            mvcc.skip.forward(tree, pair.compare, this._versions, merged, this._start, step())
        }))
    }, function (iterators) {
        mvcc.designate.forward(pair.compare, function (record) {
            return false
        }, iterators, step())
    }, function (iterator) {
        step(function () {
            // todo: amalgamate is going to set the version, which is wrong, it
            // should assert the version, and the version should be correct.
            mvcc.amalgamate(function (record) {
                return record.operation == 'del'
            }, 0, this._primary, iterator, step())
        }, function () {
            iterator.unlock()
        })
    }, function () {
        // no need to lock exclusive, anyone using these trees at the end
        // gets the same result as not using them.
        var iterator = mvcc.advance(Object.keys(merged).sort(), function (element, callback) {
            callback(null, element, element)
        })
        // todo: maybe passing a string makes a function for you.
        // todo: any case where a transaction is in an inbetween state? A state
        // where it has been removed from the transactions tree, but it exists
        // in the other trees? Or worse, an earlier version remains in the
        // transaction tree, so that when we replay it causes an earlier version
        // to win? Yes, we're writing in order. How much do we trust a Strata
        // write? Quite a bit I think. I'm imagining that we need to trust a
        // write to some degree, that it will only be a half bitten append,
        // therefore, we would have written out deletes in order, which means
        // that only the latest versions are in the tree. Would you rather I
        // write out version numbers as file names in a directory? Atomic
        // according to everything else you believe.
        mvcc.splice(function () { return 'delete' }, this._transactions, iterator, step())
    }, function () {
        this._stages.splice(1).forEach(step([], function (stage) {
            var from = path.join(this.location, 'stages', stage.name)
            var filename = tz(Date.now(), '%F-%H-%M-%S-%3N-' + stage)
            var to = path.join(this.location, 'archive', filename)
            // todo: note that archive and stage need to be on same file system.
            step(function () {
                fs.rename(from, to, step())
            }, function () {
                // todo: rimraf the archive file if we're not preserving the archive
            })
        }))
    })
})

Locket.prototype._batch = cadence(function (step, array, options) {
    var version = ++this._version
    step(function () {
        this._sequester.share(step(step, [function () { this._sequester.unlock() }]))
    }, function () {
        var properties = [ options, this._options ]
        var batch = mvcc.advance(array, function (entry, callback) {
            var record = pair.record(entry.key, entry.value, entry.type, version, properties)
            var key = extractor(record)
            callback(null, record, key)
        })
        mvcc.amalgamate(function () {
            return false
        }, version, this._stages[0].tree, batch, step())
    }, function () {
        step(function () {
            this._transactions.mutator(version, step())
        }, function (cursor) {
            step(function () {
                cursor.insert(version, version, ~ cursor.index, step())
            }, function () {
                this._versions[version] = true
                cursor.unlock()
            })
        })
    })
})

Locket.prototype._approximateSize = cadence(function (step, from, to) {
    step(function () {
        var range = constrain(pair.compare, function (key) {
            return Buffer.isBuffer(key) ? key : pair.encoder.key([]).encode(key)
        }, { gte: from, lte: to })
        this._dilution(range, this._snapshot(), step())
    }, function (iterator) {
        var approximateSize = 0
        step([function () {
            iterator.unlock()
        }], function () {
            step(function () {
                iterator.next(step())
            }, function (record, key, size) {
                if (record) approximateSize += size
                else step(null, approximateSize)
            })()
        })
    })
})

Locket.prototype._close = cadence(function (step, operations) {
    if (this._isOpened) step(function () {
        step(function (tree) {
            tree.close(step())
        })([ this._primary, this._transactions ].concat(this._stages.map(function (stage) {
            return stage.tree
        })))
    }, function () {
        this._isOpened = false
    })
})
