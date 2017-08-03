require('proof')(2, require('cadence')(prove))

function prove (async, assert) {
    var path = require('path')
    var fs = require('fs')

    var rimraf = require('rimraf')
    var cadence = require('cadence')

    var Locket = require('../..')

    var tmp = path.join(__dirname, '../tmp')

    var alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('')

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
                { type: 'put', key: 'a', value: JSON.stringify({ value: 0 }) },
                { type: 'put', key: 'a', value: JSON.stringify({ value: 1 }) }
            ], async())
        }, function () {
            locket.get('a', async())
        }, function (got) {
            assert(JSON.parse(got), { value: 1 }, 'put')
        }, function () {
            var batch = alphabet.filter(function (letter, index) {
                return index % 2 == 0
            }).map(function (letter, index) {
                return { type: 'put', key: letter, value: index * 2 }
            })
            locket.batch(batch, async())
        }, function () {
            var batch = alphabet.filter(function (letter, index) {
                return index % 2 == 1
            }).map(function (letter, index) {
                return { type: 'put', key: letter, value: index * 2 + 1 }
            })
            var keys = [], iterator = locket.iterator({ keyAsBuffer: false })
            async(function () {
                var count = 0, loop = async(function () {
                    if (++count == 7) return [ loop.break ]
                    iterator.next(async())
                }, function (key) {
                    keys.push(key)
                })()
            }, function () {
                locket.batch(batch, async())
            }, function () {
                var loop = async(function () {
                    iterator.next(async())
                }, function (key) {
                    if (key == null) return [ loop.break ]
                    keys.push(key)
                })()
            }, function () {
                assert([
                    'a', 'c', 'e', 'g', 'i', 'k', 'm', 'o', 'q', 's', 'u', 'w', 'y'
                ], keys, 'concurrent batch')
            })
        })
    })
}
