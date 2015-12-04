require('proof')(3, require('cadence')(prove))

function prove (async, assert) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')
    var levelup = require('levelup')

    var tmp = path.join(__dirname, '../tmp')

    async(function () {
        var location = path.join(tmp, 'put')
        var locket
        async(function () {
            rimraf(location, async())
        }, function () {
            locket = levelup(location, { db: Locket })
            locket.open(async())
        }, [function () {
            locket.get('a', async())
        }, function (error) {
            assert(error.status, 404, 'get empty')
        }])
    }, function () {
        var location = path.join(tmp, 'put')
        var locket
        async(function () {
            rimraf(location, async())
        }, function () {
            locket = levelup(location, { db: Locket })
            locket.open(async())
        }, function () {
            locket.put('b', JSON.stringify({ value: 1 }), async())
        }, [function () {
            locket.get('a', async())
        }, function (error) {
            assert(error.status, 404, 'not found')
        }], function () {
            locket.get(new Buffer('b'), async())
        }, function (value) {
            assert(JSON.parse(value), { value: 1 }, 'got')
        })
    })
}
