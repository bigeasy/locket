#!/usr/bin/env node

var Locket = require('../')
var cadence = require('cadence')
var path = require('path')
var crypto = require('crypto')
var seedrandom = require('seedrandom')


var random = (function () {
    var random = seedrandom(0)
    return function (max) {
        return Math.floor(random() * max)
    }
})()

cadence(function (step) {
    var locket = new Locket(path.join(path.join(__dirname, '../tmp'), 'put'))
    step(function () {
        locket.open({ createIfMissing: true }, step())
    }, function () {
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
})(function (error) {
    if (error) throw error
})
