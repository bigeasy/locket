#!/usr/bin/env node

require('proof')(3, require('cadence')(prove))

function prove (step, assert) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')
    var levelup = require('levelup')

    var tmp = path.join(__dirname, '../tmp')

    step(function () {
        var location = path.join(tmp, 'put')
        var locket
        step(function () {
            rimraf(location, step())
        }, function () {
            locket = levelup(location, { db: Locket })
            locket.open(step())
        }, [function () {
            locket.get('a', step())
        }, function (_, error) {
            assert(error.status, 404, 'get empty')
        }])
    }, function () {
        var location = path.join(tmp, 'put')
        var locket
        step(function () {
            rimraf(location, step())
        }, function () {
            locket = levelup(location, { db: Locket })
            locket.open(step())
        }, function () {
            locket.put('b', JSON.stringify({ value: 1 }), step())
        }, [function () {
            locket.get('a', step())
        }, function (_, error) {
            assert(error.status, 404, 'not found')
        }], function () {
            locket.get(new Buffer('b'), step())
        }, function (value) {
            assert(JSON.parse(value), { value: 1 }, 'got')
        })
    })
}
