require('proof')(1, require('cadence')(prove))

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
            locket = new Locket(location)
            locket.open({ createIfMissing: true }, async())
        }, function () {
            locket.put('ÿsÿ01', 'hello', async())
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
            async(function () {
                iterator.next(async())
            }, function(key, val) {
                assert(key == null && val == null, 'nothing')
                iterator.end(async())
            })
        }, function () {
            locket.close(async())
        })
    })
}
