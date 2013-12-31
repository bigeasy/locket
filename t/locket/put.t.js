#!/usr/bin/env node

require('proof')(3, function (step, ok, equal, deepEqual) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')

    var tmp = path.join(__dirname, '../tmp')

    step(function () {
        var location = path.join(tmp, 'put')
        var locket
        step(function () {
            rimraf(location, step())
        }, function () {
            locket = new Locket(location)
            locket.open({ createIfMissing: true }, step())
        }, function () {
            locket.put('a', JSON.stringify({ value: 1 }), step())
        }, function () {
            locket.get('a', step())
        }, function (got) {
            deepEqual(JSON.parse(got), { value: 1 }, 'put')
            locket.close(step())
        }, function () {
            locket = new Locket(location)
            locket.open(step())
        }, function () {
            locket.get('a', step())
        }, function (got) {
            ok(Buffer.isBuffer(got), 'is buffer')
            deepEqual(JSON.parse(got), { value: 1 }, 'reopen')
            locket.close(step())
        })
    })
})
