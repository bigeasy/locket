#!/usr/bin/env node

/*
  ___ usage: en_US ___
  usage: node load.js

    All around tests for benchmarking Locket.

  options:

    -d, --leveldown
        use leveldown instead of Locket.
  ___ usage ___
*/

var Locket = require('../')
var cadence = require('cadence')
var path = require('path')
var crypto = require('crypto')
var seedrandom = require('seedrandom')
var levelup = require('levelup')
var rimraf = require('rimraf')

var mkdirp = require('mkdirp')


var random = (function () {
    var random = seedrandom(0)
    return function (max) {
        return Math.floor(random() * max)
    }
})()

var runner = cadence(function (async, options) {
    var file = path.join(__dirname, 'tmp', 'put'), db, count = 0
    var o = { createIfMissing: true }
    if (!options.params.leveldown) {
        o.db = require('..')
    }
    async(function () {
        rimraf(file, async())
    }, function () {
        //mkdirp(file, async())
    }, function () {
        levelup(file, o, async())
    }, function (db) {
        async(function () {
            async(function () {
                var entries = []
                var type, sha, buffer, value
                for (var i = 0; i < 1024; i++) {
                    var value = random(10000)
                    sha = crypto.createHash('sha1')
                    buffer = new Buffer(4)
                    buffer.writeUInt32BE(value, 0)
                    sha.update(buffer)
                    entries.push({
                        key: sha.digest(),
                        value: buffer,
                        type: !! random(2) ? 'put' : 'del'
                    })
                }
                db.batch(entries, async())
            })(7)
        }, function () {
            db.close(async())
        })
    }, function () {
        levelup(file, o, async())
    }, function (db) {
        async(function () {
            db.createReadStream()
                .on('data', function (data) {
                    count++
                })
                .on('error', async(Error))
                .on('end', async(null))
        }, function () {
            console.log('count', count)
            db.close(async())
        })
    })
})

require('arguable/executable')(module, cadence(function (async, options) {
    function run () {
        runner(options, function (error) { if (error) throw error })
    }
    run()
}))
