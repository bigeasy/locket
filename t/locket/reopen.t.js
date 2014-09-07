#!/usr/bin/env node

require('proof')(3, function (step, equal, deepEqual) {
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
            locket.open({ createIfMissing: true }, step())
        }, function () {
            locket.batch([], step())
        }, function () {
            for (var i = 0; i < 3; i++) {
                locket.put(i, JSON.stringify({ value: i }), step())
            }
        }, function () {
            locket.close(step())
        }, function () {
            locket = new Locket(location)
            locket.open({ createIfMissing: true }, step())
        }, function () {
            locket._merge(step())
        }, function () {
            locket.get(0, step())
            locket.get(1, step())
            locket.get(2, step())
        }, function (a, b, c) {
            deepEqual(JSON.parse(a), { value: 0 }, 'merged a')
            deepEqual(JSON.parse(b), { value: 1 }, 'merged b')
            deepEqual(JSON.parse(c), { value: 2 }, 'merged c')
        })
    })
})
