#!/usr/bin/env node

require('proof')(2, require('cadence')(prove))

function prove (async, assert) {
    var path = require('path')
    var fs = require('fs')
    var Delta = require('delta')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')
    var levelup = require('levelup')
    var concat = require('concat-stream')

    var tmp = path.join(__dirname, '../tmp')

    async(function () {
        var location = path.join(tmp, 'put')
        var locket
        async(function () {
            rimraf(location, async())
        }, function () {
            locket = levelup(location, { db: Locket })
            locket.open(async())
        }, function () {
            locket.put('a', 1, async())
        }, function () {
            var read = locket.createReadStream(),
                consume = concat(function(rows) {
                    var record = rows.shift()
                    assert(record.key, 'a', 'key')
                    assert(record.value, '1', 'value')
                })
            read.pipe(consume)
            var delta = new Delta(async()).ee(consume).on('finish')
        }, function () {
            locket.close(async())
        })
    })
}
