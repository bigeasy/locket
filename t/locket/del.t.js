require('proof')(1, require('cadence')(prove))

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
            locket.put('a', JSON.stringify({ value: 1 }), async())
        }, function () {
            locket.del('a', async())
        }, [function () {
            locket.get('a', async())
        }, function (error) {
            assert(error.message, 'NotFoundError: not found', 'not found')
        }])
    })
}
