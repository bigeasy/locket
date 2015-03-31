module.exports = Locket

// Modules for storage and concurrency.
var sequester         = require('sequester')
var Strata            = require('b-tree')
var BinaryFramer      = require('b-tree/frame/binary')
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
var AbstractIterator  = require('abstract-leveldown').AbstractIterator

// Inheritence.
var util = require('util')

// Invariants.
var ok   = require('assert')

// Modules for file operations. We use `strftime` to create date stamped file
// names.
var tz      = require('timezone')
var fs      = require('fs')
var path    = require('path')
var mkdirp  = require('mkdirp')

// Cadence for asynchornous control flow.
var cadence = require('cadence/redux')
require('cadence/loops')

// TODO: Move into `mvcc` hash.
var pair = require('pair')
var constrain = require('constrain')

// Modules for implementation of MVCC on top of Strata.
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

// A `Buffer` conversion function for values that are already `Buffer`s.
function echo (object) {
    return object
}

// Create a map of options resolving the defaults and whatnot. Kind of a mess.
// todo: Need to pull some of this options navigation out of Pair, and here into
// Locket.
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

// Define the pluggable serialization components of our Strata trees.
var framer = new BinaryFramer
var extractor = mvcc.revise.extractor(pair.extract);
var comparator = mvcc.revise.comparator(pair.compare)

// An implementation of the LevelDOWN `Iterator` object.
//
// The LevelUP interface allows you to specify encodings at both when you create
// the database and when you invoke one of it's functions, so we need to pass
// two different sets of options to all the functions that consider the
// encoding.
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

// Our LevelUP `Iterator` implementation is a wrapper around the internal
// iterator that merges our Strata b-tree with one or two logs where we are
// gathering new batch operations. See the `Locket._internalIterator` method for
// a description of how we compose an amalgamated MVCC iterator from the MVCC
// iterator modules.

//
Iterator.prototype._next = cadence(function (async) {
    async(function () {
        // Create an iteterator using `Locket._internalIterator` if one does not
        // already exist.
        if (this._iterator) {
            return this._iterator
        }
        async(function () {
            this._db._internalIterator(this._range, this._versions, async())
        }, function (iterator) {
            async(function () {
                iterator.next(async())
            }, function (more) {
                this._done = !more
                return this._iterator = iterator
            })
        })
    }, function (iterator) {
        // We use the asynchornous `next` method to move from page to page and
        // the system `get` method to move from item to item within the page. We
        // loop until `get` returns an item or `next` returns `false` indicating
        // that there are no more items.
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
    this._rotating = sequester.createLock()
    this._append = sequester.createLock()
    this._cursors = []
    this._appending = []
    this._primaryLeafSize = 1024
    this._primaryBranchSize = 1024
    this._stageLeafSize = 1024
    this._stageBranchSize = 1024
}
util.inherits(Locket, AbstractLevelDOWN)

Locket.prototype._versionMarker = function (entry) {
    this._versions[entry.header[0]] = !! entry.header[1]
    this._version = Math.max(+entry.header[0], this._version)
}

Locket.prototype._openStrataWithCursor = cadence(function (async, name, create, versionMarker) {
    async(function () {
        this._openStrata(name, create, versionMarker, async())
    }, function (strata) {
        if (strata) {
            // force load of the one page to load transcations.
            strata.iterator(strata.left, async())
        } else {
            return []
        }
    })
})

Locket.prototype._openStrata = cadence(function (async, name, create, versionMarker) {
    var strata
    async(function () {
        fs.readdir(path.join(this.location, name), async())
    }, function (files) {
        if (files.length || create) {
            async(function () {
                strata = this['_' + name] = new Strata({
                    directory: path.join(this.location, name),
                    framer: framer,
                    serializers: pair.serializers,
                    deserializers: pair.deserializers,
                    extractor: extractor,
                    comparator: comparator,
                    writeStage: 'leaf',
                    userRecordHandler: versionMarker || null
                })
                if (files.length) {
                    strata.open(async())
                } else {
                    strata.create(async())
                }
            }, function () {
                return [ strata ]
            })
        } else {
            return []
        }
    })
})

Locket.prototype._open = cadence(function (async, options) {
    var exists = true
    this._options = options
    this._versions = { 0: true }
    this._version = 0
    var marker = this._versionMarker.bind(this)
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
        this._openStrata('primary', true, null, async())
    }, function () {
        this._openStrataWithCursor('staging', true, this._versionMarker.bind(this), async())
    }, function (cursor) {
        this._cursors[0] = cursor
    }, function () {
        this._isOpened = true
        this._operations = 0
        this._mergeRequests = 0
    }, function () {
        async(function () {
            this._openStrataWithCursor('merging', false, this._versionMarker.bind(this), async())
        }, function (cursor) {
            if (cursor) {
                this._cursors[1] = cursor
                this._amalgamate(async())
            }
        })
    })
})

Locket.prototype._snapshot = function () {
    var versions = {}
    for (var key in this._versions) {
        versions[key] = true
    }
    return versions
}

// Iteration of the database requires merging the results from the deep storage
// b-tree and the one or two staging logs.
//
// We do this by creating a merged iterator across the b-tree and the logs. This
// is an iteartor that takes one or more iterators and advances through. It will
// advance each iterator and then when it is advanced, it returns the least
// value of each of the three iterators (or greatest value if iteration is
// reversed.)
//
// We then use the versioned iterator from the Designate module which will
// select the key/value pair for a key that has the greatest committed version.
//
// Finally we need to respect the properties given a user when creating an
// external iterator; start, stop, greater than, less than, greater than or
// equal to, less than or equal to. We use a Dilute iterator to select out only
// records that have not been deleted and that match the user's range critera.
Locket.prototype._internalIterator = cadence(function (async, range, versions) {
    var version = range.direction == 'forward' ? 0 : Math.MAX_VALUE
    var key = range.key ? { value: range.key, version: version } : null
    async(function () {
        mvcc.riffle[range.direction](this._primary, key, async())
    }, function (iterator) {
        var sheaf = this._staging.sheaf
        var advances = this._cursors.map(function (cursor) {
            if (range.key) {
                var index = sheaf.find(cursor._page, key, 0)
                if (index < 0) {
                    index = range.direction == 'forward' ? ~index : ~index - 1
                } /* else if (!range.inclusive) {
                    index += range.direction == 'forward' ? 1 : -1
                } */
                return mvcc.advance[range.direction](pair.extract, comparator, cursor._page.items, index)
            } else {
                // todo: why do I use the full extractor with amalgamate?
                return mvcc.advance[range.direction](pair.extract, comparator, cursor._page.items)
            }
        })
        var homogenize = mvcc.homogenize[range.direction](comparator, advances.concat(iterator))
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

Locket.prototype._iterator = function (options) {
    return new Iterator(this, options)
}

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

Locket.prototype._rotate = cadence(function (async) {
    async(function () {
        this._rotating.exclude(async())
    }, [function () {
        this._rotating.unlock()
    }], function () {
        async(function () {
            this._cursors[0].unlock(async())
        }, function () {
            this._staging.close(async())
        }, function () {
            fs.rename(path.join(this.location, 'staging'),
                      path.join(this.location, 'merging'), async())
        }, function () {
            this._openStrataWithCursor('merging', false, this._versionMarker.bind(this), async())
        }, function (merging) {
            async(function () {
                fs.mkdir(path.join(this.location, 'staging'), async())
            }, function () {
                this._openStrataWithCursor('staging', true, this._versionMarker.bind(this), async())
            }, function (staging) {
                this._cursors = [ staging, merging ]
            })
        })
    })
})

// todo: What makes me think that all of these entries are any good? In fact, if
// we've failed while writing a log, then loading the leaf is going to start to
// play the entries of the failed transaction. We need a player that is going to
// save up the entries, and then play them as batches, if the batch has a
// comment record attached to it. Then we know that our log here is indeed the
// latest and greatest.
//
// Another problem is that the code below will insert the records with their
// logged version, instead of converting those verisons to zero.
Locket.prototype._amalgamate = cadence(function (async) {
    async(function () {
        mvcc.splice(function (incoming, existing) {
            return incoming.record.operation == 'put' ? 'insert' : 'delete'
        }, this._primary, mvcc.advance.forward(extractor, comparator, this._cursors[1]._page.items), async())
    }, function () {
        var merging = this._cursors.pop()
        async(function () {
            merging.unlock(async())
        }, function () {
            this._merging.close(async())
        }, function () {
            this._merging = null
        })
    }, function () {
        var from = path.join(this.location, 'merging')
        var to = path.join(this.location, 'archive', tz(Date.now(), '%F-%H-%M-%S-%3N'))
        // todo: note that archive and stage need to be on same file system.
        async(function () {
            fs.rename(from, to, async())
        }, function () {
            // todo: rimraf the archive file if we're not preserving the archive
        })
    })
})

Locket.prototype._merge = cadence(function (async) {
    var merged = {}
    async(function () {
        this._rotate(async())
    }, function () {
        this._amalgamate(async())
    })
})

Locket.prototype._put = function (key, value, options, callback) {
    this._batch([{ type: 'put', key: key, value: value }], options, callback)
}

Locket.prototype._del = function (key, options, callback) {
    this._batch([{ type: 'del', key: key }], options, callback)
}

// todo: give up on finalizers here, or else make them fire per cadence.
Locket.prototype._write = cadence(function (async, array, options) {
    var version = ++this._version
    async(function () {
        this._append.share(async())
    }, [function () {
        this._append.unlock()
    }], function () {
        var properties = [ options, this._options ]
        var sheaf = this._staging.sheaf, page = this._cursors[0]._page
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
    async(function () {
        this._rotating.share(async())
    }, [function () {
        this._rotating.unlock()
    }], function (stage) {
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
        this._internalIterator(range, this._snapshot(), async())
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
            async.forEach(function (cursor) {
                cursor.unlock(async())
            })(this._cursors)
        }, function () {
            this._staging.close(async())
        }, function () {
            this._isOpened = false
        })
    }
})
