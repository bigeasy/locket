#!/usr/bin/env node

require('proof')(1, function (step, ok, equal, deepEqual) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')
    var levelup = require('levelup')
    var concat = require('concat-stream')

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
            locket.put('ÿsÿ01', 'hello', step())
        }, function () {

            // these are a copy of what levelup passes to the
            // leveldown iterator when it does createReadStream
            var iteratorOptions = {
              keys: true,
              values: true,
              createIfMissing: true,
              errorIfExists: false,
              keyEncoding: 'utf8',
              valueEncoding: 'binary',
              compression: true,
              init: true,
              defaults: true,
              writeBufferSize: 16777216,
              start: 'ÿdÿ',
              end: 'ÿdÿÿ',
              limit: -1,
              keyAsBuffer: false,
              valueAsBuffer: true
            }

            var iterator = locket.iterator(iteratorOptions)
            step(function () {
                iterator.next(step())
            }, function(key, val) {
                ok(key == null && val == null, 'nothing')
                iterator.end(step())
            })
        }, function () {
            locket.close(step())
        })
    })
})
