#!/usr/bin/env node

require('proof')(4, function (step, assert) {
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
            locket.batch([
              { type: 'put', key: 'a', value: 'able' },
              { type: 'put', key: 'b', value: 'baker' },
              { type: 'put', key: 'c', value: 'charlie' }
            ], step())
        }, function () {
            var keys = [], iterator = locket.iterator()
            step(function () {
                // todo: better way to break outer?
                step(function () {
                    iterator.next(step())
                }, function (key, value) {
                    if (key && value) keys.push(key.toString())
                    else return [ step ]
                })()
            }, function () {
                assert(keys, [ 'a', 'b', 'c' ], 'left most to end')
                iterator.end(step())
            })
        }, function () {
            var keys = [], values = [], iterator = locket.iterator({
                keyAsBuffer: false,
                valueAsBuffer: false
            })
            step(function () {
                // todo: better way to break outer?
                step(function () {
                    iterator.next(step())
                }, function (key, value) {
                    if (key && value) {
                        keys.push(key)
                        values.push(value)
                    } else {
                        return [ step ]
                    }
                })()
            }, function () {
                assert(keys, [ 'a', 'b', 'c' ], 'keys not as buffer')
                assert(values, [ 'able', 'baker', 'charlie' ], 'values not as buffer')
                iterator.end(step())
            })
        }, function () {
            var keys = [], iterator = locket.iterator({ reverse: true })
            step(function () {
                // todo: better way to break outer?
                step(function () {
                    iterator.next(step())
                }, function (key, value) {
                    if (key && value) keys.push(key.toString())
                    else return [ step ]
                })()
            }, function () {
                assert(keys, [ 'c', 'b', 'a' ], 'reversed left most to end')
                iterator.end(step())
            })
        }, function () {
            locket.close(step())
        })
    })
})
