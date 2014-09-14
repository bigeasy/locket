#!/usr/bin/env node

require('proof')(4, function (step, assert) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')

    var tmp = path.join(__dirname, '../tmp')

//    Error.stackTraceLimit = Infinity
    step(function () {
        var location = path.join(tmp, 'put')
        var locket
        step(function () {
            rimraf(location, step())
        }, function () {
            locket = new Locket(location)
            locket._primaryBranchSize = 16
            locket._primaryLeafSize = 16
            locket.open({ createIfMissing: true }, step())
        }, function () {
            locket.batch([], step())
        }, function () {
            var batch = []
            for (var i = 0; i < 1024; i++) {
                batch.push({ type: 'put', key: i, value: JSON.stringify({ value: i }) })
            }
            locket.batch(batch, step())
        }, function () {
            locket.get(0, step())
        }, function (got) {
            assert(JSON.parse(got), { value: 0 }, 'unmerged')
        }, function () {
            locket._merge(step())
        }, function () {
            locket.get(0, step())
            locket.get(1, step())
            locket.get(2, step())
        }, function (a, b, c) {
            assert(JSON.parse(a), { value: 0 }, 'merged a')
            assert(JSON.parse(b), { value: 1 }, 'merged b')
            assert(JSON.parse(c), { value: 2 }, 'merged c')
        }, function () {
            var batch = []
            for (var i = 0; i < 1024; i++) {
                batch.push({ type: 'del', key: i })
            }
            locket.batch(batch, step())
        }, function () {
            locket._merge(step())
        })
    })
})
