#!/usr/bin/env node

require('proof')(4, function (step, equal, deepEqual) {
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
            locket.batch([], step())
        }, function () {
            locket.batch([
              { type: 'put', key: 'a', value: JSON.stringify({ value: 1 }) },
              { type: 'put', key: 'a', value: JSON.stringify({ value: 0 }) },
              { type: 'put', key: 'c', value: JSON.stringify({ value: 2 }) },
              { type: 'put', key: 'b', value: JSON.stringify({ value: 1 }) }
            ], step())
        }, function () {
            locket.get('a', step())
        }, function (got) {
            deepEqual(JSON.parse(got), { value: 0 }, 'unmerged')
        }, function () {
            locket._merge(step())
        }, function () {
            locket.get('a', step())
            locket.get('b', step())
            locket.get('c', step())
        }, function (a, b, c) {
            deepEqual(JSON.parse(a), { value: 0 }, 'merged a')
            deepEqual(JSON.parse(b), { value: 1 }, 'merged b')
            deepEqual(JSON.parse(c), { value: 2 }, 'merged c')
        })
    })
})
