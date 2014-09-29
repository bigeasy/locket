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

require('arguable/executable')(module, cadence(function (step, options) {
    var file = path.join(__dirname, 'tmp', 'put')
    step(function () {
        rimraf(file, step())
    }, function () {
        //mkdirp(file, step())
    }, function () {
        var o = { createIfMissing: true }
        if (!options.params.leveldown) {
            o.db = require('..')
        }
        var locket = levelup(file, o)
        step(function () {
            var entries = []
            var type, sha, buffer, value
            for (var i = 0; i < 1024; i++) {
                var value = random(10000)
                sha = crypto.createHash('sha1')
                buffer = new Buffer(4)
                buffer.writeUInt32BE(value, 0)
                sha.update(buffer)
                entries.push({
                    key: sha.digest('binary'),
                    value: value,
                    type: !! random(1)
                })
            }
            console.log('here', entries.length)
            locket.batch(entries, step())
        })(7)
    })
}))
