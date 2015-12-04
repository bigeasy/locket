require('proof')(3, require('cadence')(prove))

function prove (async, assert) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')

    var tmp = path.join(__dirname, '../tmp')

//    Error.stackTraceLimit = Infinity
    async(function () {
        var location = path.join(tmp, 'put')
        var locket
        async(function () {
            rimraf(location, async())
        }, function () {
            locket = new Locket(location)
            locket.open({ createIfMissing: true }, async())
        }, function () {
            locket.batch([], async())
        }, function () {
            for (var i = 0; i < 3; i++) {
                locket.put(i, JSON.stringify({ value: i }), async())
            }
        }, function () {
            locket.close(async())
        }, function () {
            locket = new Locket(location)
            locket.open({ createIfMissing: true }, async())
        }, function () {
            console.log('foo')
            locket.get(0, async())
        }, function () {
            console.log('foo')
            locket._merge(async())
        }, function () {
            console.log('foo')
            locket.get(0, async())
        }, function (a) {
            console.log('foo')
            assert(JSON.parse(a), { value: 0 }, 'merged a')
            locket.get(1, async())
        }, function (b) {
            assert(JSON.parse(b), { value: 1 }, 'merged b')
            locket.get(2, async())
        }, function (c) {
            assert(JSON.parse(c), { value: 2 }, 'merged c')
        })
    })
}
