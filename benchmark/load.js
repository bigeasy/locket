#!/usr/bin/env node

var Locket = require('../')
var cadence = require('cadence')
var path = require('path')
var crypto = require('crypto')

var locket = new Locket(path.join(path.join(__dirname, '../tmp'), 'put'))

function pseudo (max) {
    var random = Math.random()
    while (random > max) {
        random = Math.random()
    }
    return random
}
// ^^^ see:
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Math/random

locket.open({createIfMissing: true}, function () {
    var entries = []
    var max = 10000
    var type, sha, val

    for (var i=0; i<1024; i++) { 
        val = pseudo(max)
        sha = crypto.createHash('sha1')
        sha.update(new Buffer(val)) // <- wrong invocation, that creates a
                                    //      buffer of length `val`.
        type = (val % 2 == 0)       // <- need a new random number or you will
                                    //      only ever insert odds and delete
                                    //      evens, or whatever.
        entries.push({
            type: type,
            key: sha.digest('binary'),
            value: val
        })
    }
    locket.batch(entries, function() {
        sha = crypto.createHash('sha1')
        sha.update(new Buffer(val))
        locket.get(sha.digest('binary'), function () {
            console.log(arguments)
        })
    })
})
