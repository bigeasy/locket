#!/usr/bin/env node

require('proof')(4, require('cadence')(prove))

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
            locket.batch([], async())
        }, function () {
            var batch = []
            for (var i = 0; i < 1024; i++) {
                batch.push({ type: 'put', key: i, value: JSON.stringify({ value: i }) })
            }
            locket.batch(batch, async())
        }, function () {
            locket.get(0, async())
        }, function (got) {
            assert(JSON.parse(got), { value: 0 }, 'unmerged')
        }, function () {
            locket._merge(async())
        }, function () {
            locket.get(0, async())
            locket.get(1, async())
            locket.get(2, async())
        }, function (a, b, c) {
            assert(JSON.parse(a), { value: 0 }, 'merged a')
            assert(JSON.parse(b), { value: 1 }, 'merged b')
            assert(JSON.parse(c), { value: 2 }, 'merged c')
        }, function () {
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
