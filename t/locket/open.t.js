require('proof')(7, require('cadence')(prove))

function prove (async, assert) {
    var path = require('path')
    var fs = require('fs')
    var tmp = path.join(__dirname, '../tmp')
    var rimraf = require('rimraf')
    var Locket = require('../..')
    var cadence = require('cadence')

    async(function () {
        var invalid = path.join(tmp, 'invalid')
        var locket
        async(function () {
            rimraf(tmp, async())
        }, [function () {
            locket = new Locket(invalid)
            locket.open({ createIfMissing: false }, async())
        }, function (error) {
          assert(error.message, 'does not exist')
        }])
    }, function () {
        var locket
        async(function () {
            rimraf(tmp, async())
        }, [function () {
            locket = new Locket(__dirname)
            locket.open(async())
        }, function (error) {
            assert(error.message, 'not a Locket datastore')
        }])
    }, function () {
        var empty = path.join(tmp, 'empty')
        var locket
        async(function () {
            rimraf(tmp, async())
        }, function () {
            locket = new Locket(empty)
            locket.open({ createIfMissing: true }, async())
        }, function () {
            fs.readdir(empty, async())
        }, function (listing) {
            assert(listing.sort(), [ 'archive', 'merging', 'primary', 'staging' ], 'created')
            fs.readdir(path.join(empty, 'staging'), async())
        }, function (listing) {
            assert(listing.sort(), [ 'drafts', 'pages' ], 'staging created')
        }, function () {
            locket.close(async())
        })
    }, function () {
        var existing = path.join(tmp, 'empty')
        var locket
        async([function () {
            locket = new Locket(existing)
            locket.open({ createIfMissing: false, errorIfExists: true }, async())
        }, function (error) {
            assert(error.message, 'Locket database already exists', 'errorIfExists')
        }])
    }, function () {
        var empty = path.join(tmp, 'empty')
        var locket
        async(function () {
            locket = new Locket(empty)
            locket.open({}, async())
        }, function () {
            fs.readdir(empty, async())
        }, function (listing) {
            assert(listing.sort(), [ 'archive', 'merging', 'primary', 'staging' ], 'reopened')
            fs.readdir(path.join(empty, 'staging'), async())
        }, function (listing) {
            assert(listing.sort(), [ 'drafts', 'pages' ], 'staging reopened')
        }, function () {
            locket.close(async())
        }, function () {
            locket.close(async()) // test double close
        })
    })
}
