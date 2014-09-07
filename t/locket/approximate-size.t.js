#!/usr/bin/env node

require('proof')(2, function (step, assert) {
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
              { type: 'put', key: 'a', value: '1' },
              { type: 'put', key: 'b', value: '2' },
              { type: 'put', key: 'c', value: '3' }
            ], step())
        }, function () {
            locket.approximateSize('a', 'c', step())
        }, function (size) {
            assert(size, 183, 'all')
        }, function () {
            locket.approximateSize('a', 'b', step())
        }, function (size) {
            assert(size, 122, 'some')
        }, function () {
            locket.close(step())
        })
    })
})
