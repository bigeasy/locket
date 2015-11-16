#!/usr/bin/env node

/*
  ___ usage ___ en_US ___
  usage: node load.js

    All around tests for benchmarking Locket.

  options:

    -d, --leveldown
        use leveldown instead of Locket.
  ___ . ___
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

var runner = cadence(function (async, program) {
    var start, insert, gather
    var file = path.join(__dirname, 'tmp', 'put'), db, records = []
    var o = { createIfMissing: true }
    if (!program.param.leveldown) {
        o.db = require('..')
    }
    var batches = []
    while (batches.length != 7) {
        var entries = []
        var type, sha, buffer, value
        for (var i = 0; i < 1024; i++) {
            var value = random(1024)
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
        batches.push(entries)
    }
    async(function () {
        rimraf(file, async())
    }, function () {
        //mkdirp(file, async())
    }, function () {
        start = Date.now()
        levelup(file, o, async())
    }, function (db) {
        async(function () {
            var batch = 0, loop = async(function () {
                if (batch == 7) return [ loop.break ]
                db.batch(batches[batch], async())
                batch++
            })()
        }, function () {
            db.close(async())
        })
    }, function () {
        insert = Date.now() - start
        start = Date.now()
        levelup(file, o, async())
    }, function (db) {
        async(function () {
            async.ee(db.createReadStream())
                 .on('data', function (data) { records.push(data) })
                 .end('end')
                 .error()
        }, function () {
            db.close(async())
        }, function () {
            gather = Date.now() - start
            console.log('insert: ' + insert + ', gather: ' + gather)
        })
    })
})

require('arguable')(module, cadence(function (async, program) {
    runner(program, async())
}))
