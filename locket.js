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

var cadence = require('cadence/redux')
var mkdirp  = require('mkdirp')

var pair = require('pair')
var constrain = require('constrain')

require('cadence/loops')

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

function Options (options, defaults) {
    for (var key in options) {
        this[key] = options[key]
    }
    for (var key in defaults) {
        if (!(key in options)) {
            this[key] = defaults[key]
        }
    }
}

var extractor = mvcc.revise.extractor(pair.extract);
function Stage (db, number, status) {
    this.number = number
    this.location = db.location
    this.leafSize = db.leafSize
    this.branchSize = db.branchSize
    this.status = status
    this.count = 0
    this.directory = path.join(db.location, 'stages', String(number))
    this.tree = new Strata({
        directory: this.directory,
        extractor: extractor,
        comparator: mvcc.revise.comparator(pair.compare),
        serialize: pair.serializer,
        deserialize: pair.deserializer,
        leafSize: this.leafSize,
        branchSize: this.branchSize,
        writeStage: 'leaf'
    })
}

Stage.prototype.create = cadence(function (async, number) {
    async (function () {
        mkdirp(path.join(this.location, 'stages', String(this.number)), async())
    }, function () {
        this.tree.create(async())
    }, function () {
        this.tree.open(async())
    })
})

// There are two ways to sort out options here, because there are great many
// ways to specify encodings and its an amalgamation of iterator options and
// database options. The Pair module makes this determination for us from an
// array of options. Then we have options specific to the iterator, not related
// to encodings.

function Iterator (db, options) {
    var preferences = [ options, db._options ]
    options = new Options(options, { keyAsBuffer: true, valueAsBuffer: true })

    this._db = db
    this._range = constrain(pair.compare, function (key) {
        return Buffer.isBuffer(key) ? key : pair.encoder.key(preferences).encode(key)
    }, options)
    this._versions = this._db._snapshot()
    this._decoders = {
        key: options.keyAsBuffer ? echo : pair.encoder.key(preferences).decode,
        value: options.valueAsBuffer ? echo : pair.encoder.value(preferences).decode
    }
}
util.inherits(Iterator, AbstractIterator)

Iterator.prototype._next = cadence(function (async) {
    async(function () {
        if (this._iterator) {
            return this._iterator
        }
        async(function () {
            this._db._dilution(this._range, this._versions, async())
        }, function (iterator) {
            return [ this._iterator = iterator ]
        })
    }, function (iterator) {
        iterator.next(async())
    }, function (record, key) {
        if (record) {
            return [ this._decoders.key(record.key), this._decoders.value(record.value) ]
        }
    })
})

Iterator.prototype._end = function (callback) {
    this._iterator.unlock(callback)
}

function Locket (location) {
    if (!(this instanceof Locket)) return new Locket(location)
    AbstractLevelDOWN.call(this, location)
    this._merging = sequester.createLock()
    this._primaryLeafSize = 1024
    this._primaryBranchSize = 1024
    this._stageLeafSize = 1024
    this._stageBranchSize = 1024
}
util.inherits(Locket, AbstractLevelDOWN)

Locket.prototype._snapshot = function () {
    var versions = {}
    for (var key in this._versions) {
        versions[key] = true
    }
    return versions
}

Locket.prototype._dilution = cadence(function (async, range, versions) {
    async(function () {
        async.map(function (stage) {
            mvcc.skip[range.direction](stage.tree, pair.compare, versions, {}, range.key, async())
        })([ { tree: this._primary } ].concat(this._stages))
    }, function (iterators) {
        mvcc.designate[range.direction](pair.compare, function (record) {
            return record.operation == 'del'
        }, iterators, async())
    }, function (iterator) {
        return mvcc.dilute(iterator, function (key) {
            return range.valid(key.value)
        })
    })
})

Locket.prototype._open = cadence(function (async, options) {
    var exists = true
    this._options = options
    async(function () {
        var readdir = async([function () {
            fs.readdir(this.location, async())
        }, function (error) {
            if (error.code === 'ENOENT') {
                if (options.createIfMissing == null || options.createIfMissing) {
                    exists = false
                    async(function () {
                        mkdirp(this.location, async())
                    }, function () {
                        return [ readdir() ]
                    })
                } else {
                    throw new Error('does not exist')
                }
            } else {
                throw error
            }
        }], function (files) {
            return [ readdir, files ]
        })()
    }, function (files) {
        if (exists && options.errorIfExists) {
            throw new Error('Locket database already exists')
        }
        var subdirs = [ 'archive', 'primary', 'stages', 'transactions' ]
        if (exists) {
            files = files.filter(function (file) { return file[0] != '.' }).sort()
            if (!files.length) {
                exists = false
            } else if (!subdirs.every(function (file) { return files.shift() == file }) || files.length) {
                throw new Error('not a Locket datastore')
            }
        }
        if (!exists) {
            async.forEach(function (dir) {
                fs.mkdir(path.join(this.location, dir), async())
            })(subdirs)
        }
    }, function () {
        this._primary = new Strata({
            directory: path.join(this.location, 'primary'),
            extractor: extractor,
            comparator: mvcc.revise.comparator(pair.compare),
            serialize: pair.serializer,
            deserialize: pair.deserializer,
            leafSize: this._primaryLeafSize,
            branchSize: this._primaryBranchSize,
            writeStage: 'leaf'
        })
    }, function () {
        if (!exists) this._primary.create(async())
        this._transactions = new Strata({
            directory: path.join(this.location, 'transactions'),
            leafSize: this._stageLeafSize,
            branchSize: this._stageBranchSize,
            writeStage: 'leaf'
        })
        if (!exists) this._transactions.create(async())
    }, function () {
        this._primary.open(async())
    }, function () {
        this._transactions.open(async())
    }, function () {
        fs.readdir(path.join(this.location, 'stages'), async())
    }, function (files) {
        files = files.filter(function (file) { return file[0] != '.' })
                     .map(function (file) { return +file })
                     .sort(function (a, b) { return a - b }).reverse()
        this._maxStageNumber = Math.max.apply(Math, files.concat(0))
        async(function () {
            async.map(function (number) {
                var stage = new Stage(this, number, 'full')
                async(function () {
                    stage.tree.open(async())
                }, function () {
                    return stage
                })
            })(files)
        }, function (stages) {
            this._stages = stages
        })
    }, function () {
        this._isOpened = true
        this._operations = 0
        this._mergeRequests = 0
        this._versions = { 0: true }
        this._version = 1
        mvcc.riffle.forward(this._transactions, async())
    }, function (transactions) {
        async([function () {
            transactions.unlock(async())
        }], function () {
            var loop = async(function () {
                transactions.next(async())
            }, function (version) {
                if(version) {
                    this._versions[version] = true
                    this._version = Math.max(this._version, version)
                } else {
                    return [ loop ]
                }
            })()
        })
    })
})

Locket.prototype._get = cadence(function (async, key, options) {
    options = new Options(options, { asBuffer: true })
    if (!Buffer.isBuffer(key)) {
        key = pair.encoder.key([ options, this._options ]).encode(key)
    }
    var iterator = this._iterator({ start: key, limit: 1 })
    async(function () {
        iterator.next(async())
    }, function ($key, value) {
        async(function () {
            iterator.end(async())
        }, function () {
            if ($key && value && pair.compare($key, key) == 0) {
                if (!options.asBuffer) {
                    value = pair.encoder.value([ options, this._options ]).decode(value)
                }
                return [ value ]
            } else {
                throw new Error('NotFoundError: not found')
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

Locket.prototype._merge = cadence(function (async) {
    var merged = {}
    async(function () {
        async (function () {
            this._merging.exclude(async())
        }, [function () {
            this._merging.unlock()
        }])
    }, function () {
        var stages = this._stages.filter(function (stage) {
            return (stage.status == 'idle' && stage.count > 1024 * 0.74)
                || (stage.status == 'full')
        })
        stages.forEach(function (stage) {
            stage.status = 'merge'
        })
        async(function () {
            async.map(function (tree) {
                mvcc.skip.forward(tree, pair.compare, this._versions, merged, this._start, async())
            })(stages.map(function (stage) { return stage.tree }))
        }, function (iterators) {
            mvcc.designate.forward(pair.compare, function (record) {
                return false
            }, iterators, async())
        }, function (iterator) {
            // todo: amalgamate is going to set the version, which is wrong, it
            // should assert the version, and the version should be correct.
            async(function () {
                mvcc.amalgamate(function (record) {
                    return record.operation == 'del'
                }, 0, this._primary, iterator, async())
            }, function () {
                iterator.unlock(async())
            })
        }, function () {
            // no need to lock exclusive, anyone using these trees at the end
            // gets the same result as not using them.
            var iterator = mvcc.advance(Object.keys(merged).sort(), function (element, callback) {
                callback(null, element, element)
            })
            // todo: maybe passing a string makes a function for you.
            // todo: any case where a transaction is in an inbetween state? A
            // state where it has been removed from the transactions tree, but
            // it exists in the other trees? Or worse, an earlier version
            // remains in the transaction tree, so that when we replay it causes
            // an earlier version to win? Yes, we're writing in order. How much
            // do we trust a Strata write? Quite a bit I think. I'm imagining
            // that we need to trust a write to some degree, that it will only
            // be a half bitten append, therefore, we would have written out
            // deletes in order, which means that only the latest versions are
            // in the tree. Would you rather I write out version numbers as file
            // names in a directory? Atomic according to everything else you
            // believe.
            mvcc.splice(function () { return 'delete' }, this._transactions, iterator, async())
        }, function () {
            var loop = async(function () {
                this._primary.balance(async())
            }, function () {
                if (this._primary.balanced) return loop
            })()
        }, function () {
            async.forEach(function (stage) {
                var from = path.join(this.location, 'stages', String(stage.number))
                var filename = tz(Date.now(), '%F-%H-%M-%S-%3N-' + stage.number)
                var to = path.join(this.location, 'archive', filename)
                // todo: note that archive and stage need to be on same file system.
                async(function () {
                    fs.rename(from, to, async())
                }, function () {
                    // todo: rimraf the archive file if we're not preserving the archive
                })
            })(this._stages.filter(function (stage) { return stage.status == 'merge' }))
        }, function () {
            this._stages = this._stages.filter(function (stage) {
                return stage.status != 'merge'
            })
        })
    })
})

Locket.prototype._batch = cadence(function (async, array, options) {
    var version = ++this._version
    async(function () {
        var stage = this._stages.filter(function (stage) {
            return stage.status == 'idle'
        }).pop();
        if (stage) return stage
        stage = new Stage(this, ++this._maxStageNumber, 'active')
        this._stages.unshift(stage)
        async(function () {
            stage.create(async())
        }, function () {
            return stage
        })
    }, function (stage) {
        async(function () {
            stage.status = 'active'
            stage.count += array.length
            // Array does not need to be sorted because it is being inserted into a
            // tree with only one leaf.
            var properties = [ options, this._options ]
            var batch = mvcc.advance(array, function (entry, callback) {
                var record = pair.record(entry.key, entry.value, entry.type, version, properties)
                var key = extractor(record)
                callback(null, record, key)
            })
            mvcc.amalgamate(function () {
                return false
            }, version, stage.tree, batch, async())
        }, function () {
            async(function () {
                this._transactions.mutator(version, async())
            }, function (cursor) {
                async(function () {
                    cursor.insert(version, version, ~ cursor.index, async())
                }, function () {
                    cursor.unlock(async())
                })
            })
        }, function () {
            stage.status = 'idle'
            this._versions[version] = true
        })
    })
})

Locket.prototype._approximateSize = cadence(function (async, from, to) {
    async(function () {
        var range = constrain(pair.compare, function (key) {
            return Buffer.isBuffer(key) ? key : pair.encoder.key([]).encode(key)
        }, { gte: from, lte: to })
        this._dilution(range, this._snapshot(), async())
    }, function (iterator) {
        var approximateSize = 0
        async([function () {
            iterator.unlock(async())
        }], function () {
            var loop = async(function () {
                iterator.next(async())
            }, function (record, key, size) {
                if (record) approximateSize += size
                else return [ loop, approximateSize ]
            })()
        })
    })
})

Locket.prototype._close = cadence(function (async, operations) {
    if (this._isOpened) {
        async(function () {
            async.forEach(function (tree) {
                tree.close(async())
            })([ this._primary, this._transactions ].concat(this._stages.map(function (stage) {
                return stage.tree
            })))
        }, function () {
            this._isOpened = false
        })
    }
})
