module.exports = Locket

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

var pair = require('pair')

var mvcc = {
    revise: require('revise'),
    riffle: require('riffle'),
    advance: require('advance'),
    skip: require('skip'),
    designate: require('designate'),
    amalgamate: require('amalgamate')
}

function isTrue (options, property, defaultValue) {
    return !!((property in options) ? options[property] : defaultValue)
}

function Iterator (db, options) {
    var versions = {}

    for (var key in db._versions) {
        versions[key] = true
    }

    this._db = db
    this._start = options.start
    this._limit = options.limit
    this._versions = versions
    this._direction = isTrue(options, 'reverse', false) ? 'reverse' : 'forward'
    this._keyAsBuffer = isTrue(options, 'keyAsBuffer', true)
    this._valueAsBuffer = isTrue(options, 'valueAsBuffer', true)
}
util.inherits(Iterator, AbstractIterator)

Iterator.prototype._next = cadence(function (step) {
    step(function () {
        if (this._iterator) return this._iterator
        step(function () {
            var iterators = []
            step(function () {
                step(function (stage) {
                    mvcc.skip[this._direction](
                        stage.tree, pair.compare, this._versions, {}, this._start, step()
                    )
                }, function (iterator) {
                    iterators.push(iterator)
                })([ { tree: this._db._primary } ].concat(this._db._stages))
            }, function () {
                return iterators
            })
        }, function (iterators) {
            mvcc.designate.forward(pair.compare, function (record) {
                return record.operation == 'del'
            }, iterators, step('_iterator'))
        })
    }, function (iterator) {
        iterator.next(step())
    }, function (record) {
        if (record) step(null, record.key, record.value)
    })
})

Iterator.prototype._end = function (callback) {
    this._iterator.unlock()
    callback()
}

function Locket (location) {
    if (!(this instanceof Locket)) return new Locket(location)
    AbstractLevelDOWN.call(this, location)
    this._sequester = new Sequester
}
util.inherits(Locket, AbstractLevelDOWN)

var extractor =  mvcc.revise.extractor(pair.extract)
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
        key = pair.encoder.key([ options || {}, this._options ]).encode(key)
    }
    var iterator = this._iterator({ start: key, limit: 1 })
    step(function () {
        iterator.next(step())
    }, function ($key, value) {
        step(function () {
            iterator.end(step())
        }, function () {
            if ($key && value && pair.compare($key, key) == 0) {
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

function Merge (db) {
    this._db = db
    this._greatest = 0
}

Merge.prototype.update = cadence(function (step, record) {
    var key = { key: record.key, version: 0 }
    this._greatest = Math.max(record.version, this._greatest)
    var insert = step(function () {
        if (!this._primary) {this._db._primary.mutator(key, step(step, function ($) {
            this._primary = $
            return this._primary.index
        }))} else {
            this._primary.indexOf(key, step())
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
            this._primary.insert({
                version: 0, // todo: different object type?
                key: record.key,
                value: record.value
            }, key, index, step())
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
    var candidate = { version: 0 }
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
        unmerged.tree.iterator(unmerged.tree.left, step())
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
                        if (this._db._versions[record.version]) {
                            if (candidate.version && candidate.key != record.key) {
                                // what? nooooo. what? whatcha doin' there champ?
                                //delete this._db._versions[candidate.transactionId]
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
        if (candidate.version) {
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
            //
            // we need to stop the world just long enough to unshift the new
            // stage, it will happen in one tick, super quick.
            step(function () {
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
            // once again, first merge any extras down to one.
            step(function () {
                if (this._stages.length == 1) step(null)
                else new Merge(this).merge(step())
            })()
        })
    }
})

Locket.prototype._batch = cadence(function (step, array, options) {
    var version = ++this._version
    var tree = this._stages[0].tree
    step(function () {
        this._sequester.share(step(step, [function () { this._sequester.unlock() }]))
    }, function () {
        var properties = [ options, this._options ]
        var batch = mvcc.advance.forward(array, function (entry, callback) {
            var record = pair.record(entry.key, entry.value, entry.type, version, properties)
            var key = extractor(record)
            callback(null, record, key)
        })
        mvcc.amalgamate.amalgamate(function () {
            return false
        }, version, tree, batch, step())
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
