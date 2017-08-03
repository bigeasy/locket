require('proof')(2, require('cadence')(prove))

function prove (async, assert) {
    var Locket = require('../..'), locket
    var path = require('path')
    var rimraf = require('rimraf')
    var location = path.join(__dirname, '../tmp')
    async(function () {
        rimraf(location, async())
    }, function () {
        locket = new Locket(location)
        locket._tryCatchKeep(function () {
            throw new Error('thrown')
        }, async())
    }, function () {
        assert(locket._error.message, 'thrown', 'caught thrown message')
        try {
            locket._checkError()
        } catch (error) {
            assert(error.cause.message, 'thrown', 'thrown caught message')
        }
    })
}
