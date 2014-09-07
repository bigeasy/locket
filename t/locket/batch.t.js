#!/usr/bin/env node

require('proof')(1, function (step, assert) {
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
              { type: 'put', key: 'a', value: JSON.stringify({ value: 0 }) }
            ], step())
        }, function () {
            locket.get('a', step())
        }, function (got) {
            assert(JSON.parse(got), { value: 0 }, 'put')
        })
    })
})
