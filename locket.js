module.exports = Locket

var sequester         = require('sequester')
var Strata            = require('b-tree')
var Framer            = require('b-tree/frame/binary')
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

var framer = new Framer

require('cadence/loops')

function echo (object) { return object }

var mvcc = {
    revise: require('revise'),
    riffle: require('riffle'),
    advance: require('advance'),
    splice: require('splice'),
    homogenize: require('homogenize'),
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
var comparator = mvcc.revise.comparator(pair.compare)

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
            async(function () {
                iterator.next(async())
            }, function (more) {
                this._done = !more
                return this._iterator = iterator
            })
        })
    }, function (iterator) {
        var loop = async(function () {
            if (this._done) {
                return [ loop ]
            }
            var item = this._iterator.get()
            if (item) {
                return [ loop, this._decoders.key(item.record.key),
                               this._decoders.value(item.record.value) ]
            }
            this._iterator.next(async())
        }, function (more) {
            this._done = !more
        })()
    })
})

Iterator.prototype._end = function (callback) {
    this._iterator.unlock(callback)
}

function Locket (location) {
    if (!(this instanceof Locket)) return new Locket(location)
    AbstractLevelDOWN.call(this, location)
    this._merging = sequester.createLock()
    this._append = sequester.createLock()
    this._appending = []
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
        mvcc.riffle[range.direction](this._primary, range.key, async())
    }, function (iterator) {
        var advance
        if (range.key) {
            var sheaf = this._staging.sheaf, page = this._cursor._page
            var version = range.direction == 'forward' ? 0 : Math.MAX_VALUE
            var index = sheaf.find(this._cursor._page, { value: range.key, version: version }, 0)
            if (index < 0) {
                index = range.direction == 'forward' ? ~index : ~index - 1
            } /* else if (!range.inclusive) {
                index += range.direction == 'forward' ? 1 : -1
            } */
            advance = mvcc.advance[range.direction](this._cursor._page.items, index)
        } else {
            advance = mvcc.advance[range.direction](this._cursor._page.items)
        }
        var homogenize = mvcc.homogenize[range.direction](comparator, [ iterator, advance ])
        var designate = mvcc.designate[range.direction](pair.compare, versions, {}, homogenize)
        var dilute = mvcc.dilute(designate, function (item) {
            if (item.record.operation == 'del') {
                return -1
            }
            return range.valid(item.key.value)
        })
        return dilute
    })
})

Locket.prototype._open = cadence(function (async, options) {
    var exists = true
    this._options = options
    this._versions = { 0: true }
    this._version = 0
    // todo: not only marked, but also merely seen. do I want the ability to
    // scan all the records as they go by?
    var markVersion = function (entry) {
        this._versions[entry.header[0]] = !! entry.header[1]
        this._version = Math.max(+entry.header[0], this._version)
    }.bind(this)
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
        var subdirs = [ 'archive', 'merging', 'primary', 'staging' ]
        if (exists) {
            files = files.filter(function (file) { return file[0] != '.' }).sort()
            if (!files.length) {
                exists = false
            // todo: not a very clever recover, something might be in the midst
            // of a rotation.
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
            framer: framer,
            extractor: extractor,
            comparator: comparator,
            serializers: pair.serializers,
            deserializers: pair.deserializers,
            serialize: pair.serializer,
            deserialize: pair.deserializer,
            leafSize: this._primaryLeafSize,
            branchSize: this._primaryBranchSize,
            writeStage: 'leaf'
        })
        if (!exists) {
            this._primary.create(async())
        }
    }, function () {
        this._staging = new Strata({
            directory: path.join(this.location, 'staging'),
            framer: framer,
            serializers: pair.serializers,
            deserializers: pair.deserializers,
            extractor: extractor,
            comparator: comparator,
            writeStage: 'leaf',
            userRecordHandler: markVersion
        })
        if (!exists) {
            this._staging.create(async())
        }
    }, function () {
        this._primary.open(async())
    }, function () {
        this._staging.open(async())
    }, function () {
        // force load of the one page to load transcations.
        this._staging.iterator(this._staging.left, async())
    }, function (cursor) {
        this._cursor = cursor
    }, function () {
        this._isOpened = true
        this._operations = 0
        this._mergeRequests = 0
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

Locket.prototype._merge = function (callback) { callback() }

Locket.prototype.__merge = cadence(function (async) {
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

// todo: give up on finalizers here, or else make them fire per cadence.
Locket.prototype._write = cadence(function (async, array, options) {
    var version = ++this._version
    async(function () {
        this._append.share(async())
    }, [function () {
        this._append.unlock()
    }], function () {
        var properties = [ options, this._options ]
        var sheaf = this._staging.sheaf, page = this._cursor._page
        var appender = this._appender
        if (!this._appender) {
            appender = this._appender = this._staging.logger.createAppender(page)
        }
        this._appending.push(version) // todo: convince yourself there's no race condition
                          // todo: add a count of locks?
        appender.writeUserRecord([ version ])
        for (var i = 0, I = array.length; i < I; i++) {
            var entry = array[i]
            var record = pair.record(entry.key, entry.value, entry.type, version, properties)
            var key = extractor(record)
            var index = sheaf.find(page, key, 0)
            var replace = 0
            if (index < 0) {
                index = ~index
            } else {
                replace = 1
                appender.writeDelete(index)
            }
            var heft = appender.writeInsert(index, record).heft
            page.splice(index, replace, { key: key, record: record, heft: heft })
        }
        appender.writeUserRecord([ version, 1 ])
    })
})

Locket.prototype._batch = cadence(function (async, array, options) {
    async(function (stage) {
        this._write(array, options, async())
    }, function () {
        if (!this._flushing) {
            async(function () {
                this._flushing = true
            }, function () {
                this._append.exclude(async())
            }, [function () {
                this._flushing = false
                this._append.unlock()
            }], function () {
                this._appender.close(async())
            }, function () {
                this._appender = null
                this._appending.splice(0, this._appending.length).forEach(function (version) {
                    this._versions[version] = true
                }, this)
            })
        }
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
            }, function (more) {
                if (more) {
                    var item
                    while (item = iterator.get()) {
                        approximateSize += item.heft
                    }
                } else {
                    return [ loop, approximateSize ]
                }
            })()
        })
    })
})

Locket.prototype._close = cadence(function (async, operations) {
    if (this._isOpened) {
        async(function () {
            this._primary.close(async())
        }, function () {
            this._cursor.unlock(async())
        }, function () {
            this._staging.close(async())
        }, function () {
            this._isOpened = false
        })
    }
})
