#!/usr/bin/env node

require('proof')(2, prove)

function prove (step, assert) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')
    var levelup = require('levelup')
    var concat = require('concat-stream')

    var tmp = path.join(__dirname, '../tmp')

    step(function () {
        var location = path.join(tmp, 'put')
        var locket
        step(function () {
            rimraf(location, step())
        }, function () {
            locket = levelup(location, { db: Locket })
            locket.open(step())
        }, function () {
            locket.put('a', 1, step())
        }, function () {
            var read = locket.createReadStream(),
                consume = concat(function(rows) {
                    var record = rows.shift()
                    assert(record.key, 'a', 'key')
                    assert(record.value, '1', 'value')
                })
            read.pipe(consume)
            consume.once('finish', step(-1))
        }, function () {
            locket.close(step())
        })
    })
}
