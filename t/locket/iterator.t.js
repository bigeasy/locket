require('proof')(4, require('cadence')(prove))

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
            locket.batch([
                { type: 'put', key: 'a', value: 'able' },
                { type: 'put', key: 'b', value: 'baker' },
                { type: 'put', key: 'c', value: 'charlie' }
            ], async())
        }, function () {
            var keys = [], iterator = locket.iterator()
            async(function () {
                var loop = async(function () {
                    iterator.next(async())
                }, function (key, value) {
                    if (key && value) keys.push(key.toString())
                    else return [ loop.break ]
                })()
            }, function () {
                assert(keys, [ 'a', 'b', 'c' ], 'left most to end')
                iterator.end(async())
            })
        }, function () {
            var keys = [], values = [], iterator = locket.iterator({
                keyAsBuffer: false,
                valueAsBuffer: false
            })
            async(function () {
                var loop = async(function () {
                    iterator.next(async())
                }, function (key, value) {
                    if (key && value) {
                        keys.push(key)
                        values.push(value)
                    } else {
                        return [ loop.break ]
                    }
                })()
            }, function () {
                assert(keys, [ 'a', 'b', 'c' ], 'keys not as buffer')
                assert(values, [ 'able', 'baker', 'charlie' ], 'values not as buffer')
                iterator.end(async())
            })
        }, function () {
            var keys = [], iterator = locket.iterator({ reverse: true })
            async(function () {
                var loop = async(function () {
                    iterator.next(async())
                }, function (key, value) {
                    if (key && value) keys.push(key.toString())
                    else return [ loop.break ]
                })()
            }, function () {
                assert(keys, [ 'c', 'b', 'a' ], 'reversed left most to end')
                iterator.end(async())
            })
        }, function () {
            locket.close(async())
        })
    })
}
