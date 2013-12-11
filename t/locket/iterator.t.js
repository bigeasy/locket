#!/usr/bin/env node

require('proof')(1, function (step, equal, deepEqual) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')

    var tmp = path.join(__dirname, '../tmp')

    step(function () {
        var location = path.join(tmp, 'put')
        var locket, iterator, keys = []
        step(function () {
            rimraf(location, step())
        }, function () {
            locket = new Locket(location)
            locket.open({ createIfMissing: true }, step())
        }, function () {
            locket.batch([
              { type: 'put', key: 'a', value: JSON.stringify({ value: 1 }) },
              { type: 'put', key: 'b', value: JSON.stringify({ value: 2 }) },
              { type: 'put', key: 'c', value: JSON.stringify({ value: 3 }) }
            ], step())
        }, function () {
            iterator = locket.iterator()
            // todo: better way to break outer?
            step(function () {
                iterator.next(step())
            }, function (key, value) {
                if (key && value) keys.push(key.toString())
                else step(null)
            })()
        }, function () {
            deepEqual(keys, [ 'a', 'b', 'c' ], 'left most to end')
            iterator.end(step())
        }, function () {
            locket.close(step())
        })
    })
})
