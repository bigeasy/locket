#!/usr/bin/env node

var Locket = require('../')
var cadence = require('cadence')
var path = require('path')
var crypto = require('crypto')
var seedrandom = require('seedrandom')


function pseudo (max) {
    var random = seedrandom()()
    while (random > max) {
        random = seedrandom()()
    }
    return random
}

cadence(function (step) {
    var locket = new Locket(path.join(path.join(__dirname, '../tmp'), 'put'))
    step(function () {
        locket.open({ createIfMissing: true }, step())
    }, function () {
        var entries = []
        var max = 10000
        var type, sha, entry, val = seedrandom(0)()
        for (var i = 0; i < 1024; i++) {
            type = (pseudo(2) % 2 == 0)
            sha = crypto.createHash('sha1')
            entry = new Buffer(4)
            entry.writeFloatLE(val, 0)
            sha.update(entry)

            entries.push({
                type: type,
                key: sha.digest('binary'),
                value: val
            })

            val = pseudo(max)
        }
        locket.batch(entries, step())
    }, function () {
        sha = crypto.createHash('sha1')
        first_key = new Buffer(4)
        first_key.writeFloatLE(seedrandom(0)(), 0)
        sha.update(first_key)
        locket.get(sha.digest('binary'), function (_, value) {
            console.log(value)
        })
        // ^^^ no idea what's going on here.
    })
})(function (error) {
    if (error) throw error
})
