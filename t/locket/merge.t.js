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
            locket._primaryBranchSize = 16
            locket._primaryLeafSize = 16
            locket.open({ createIfMissing: true }, async())
        }, function () {
            locket._check()
            locket._doubleCheck(async())
        }, function () {
            locket.batch([], async())
        }, function () {
            var batch = []
            for (var i = 0; i < 1024; i++) {
                batch.push({ type: 'put', key: i, value: JSON.stringify({ value: i }) })
            }
            locket.batch(batch, async())
            locket.merged.enter(async())
        }, function () {
            locket.get(0, async())
        }, function (a) {
            assert(JSON.parse(a), { value: 0 }, 'merged a')
            locket.get(1, async())
        }, function (b) {
            assert(JSON.parse(b), { value: 1 }, 'merged b')
            locket.get(2, async())
        }, function (c) {
            assert(JSON.parse(c), { value: 2 }, 'merged c')
            var batch = []
            for (var i = 0; i < 1024; i++) {
                batch.push({ type: 'del', key: i })
            }
            locket.batch(batch, async())
        }, function () {
            locket._merge(async())
        })
    })
}
