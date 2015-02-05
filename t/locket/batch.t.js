#!/usr/bin/env node

require('proof')(1, require('cadence/redux')(prove))

function prove (async, assert) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')

    var tmp = path.join(__dirname, '../tmp')

    async(function () {
        var location = path.join(tmp, 'put')
        var locket
        async(function () {
            rimraf(location, async())
        }, function () {
            locket = new Locket(location)
            locket.open({ createIfMissing: true }, async())
        }, function () {
            locket.batch([], async())
        }, function () {
            locket.batch([
              { type: 'put', key: 'a', value: JSON.stringify({ value: 1 }) },
              { type: 'put', key: 'a', value: JSON.stringify({ value: 0 }) }
            ], async())
        }, function () {
            locket.get('a', async())
        }, function (got) {
            assert(JSON.parse(got), { value: 0 }, 'put')
        })
    })
}
