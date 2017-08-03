require('proof')(2, require('cadence')(prove))

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
                { type: 'put', key: 'a', value: '1' },
                { type: 'put', key: 'b', value: '2' },
                { type: 'put', key: 'c', value: '3' }
            ], async())
        }, function () {
            locket.approximateSize('a', 'c', async())
        }, function (size) {
            assert(size, 42, 'all')
        }, function () {
            locket.approximateSize('a', new Buffer('b'), async())
        }, function (size) {
            assert(size, 28, 'some')
        }, function () {
            locket.close(async())
        })
    })
}
