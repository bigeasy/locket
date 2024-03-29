require('proof')(6, require('cadence')(function (step, okay) {
    const path = require('path')
    const fs = require('fs')

    const Destructible = require('destructible')
    const destructible = new Destructible('put.t')

    const callback = require('comeuppance')

    const Locket = require('..')

    const location = path.join(__dirname, 'tmp', 'locket')

    step(function () {
        if (/^v12\./.test(process.version)) {
            fs.rmdir(location, { recursive: true }, step())
        } else {
            fs.rm(location, { recursive: true, force: true }, step())
        }
    }, function () {
        fs.mkdir(location, { recursive: true }, step())
    }, function () {
        new Locket(location)

        const locket = new Locket(location, {
            primary: {
                leaf: { split: 64, merge: 32 },
                branch: { split: 64, merge: 32 },
            },
            stage: {
                max: 128,
                leaf: { split: 64, merge: 32 },
                branch: { split: 64, merge: 32 },
            }
        })

        step(function () {
            locket.open({ createIfMissing: true }, step())
        }, function () {
            locket.put('a', 'z', step())
        }, function () {
            step(function () {
                locket.get('a', step())
            }, function (value) {
                okay({
                    isBuffer: Buffer.isBuffer(value),
                    value: value.toString()
                }, {
                    isBuffer: true,
                    value: 'z'
                }, 'put')
            })
        }, function () {
            step(function () {
                locket.get('a', { asBuffer: false }, step())
            }, function (value) {
                okay({
                    type: typeof value,
                    value: value
                }, {
                    type: 'string',
                    value: 'z'
                }, 'get')
            })
        }, function () {
            locket.del('a', step())
        }, function () {
            const test = []
            step([function () {
                locket.get('a', step())
            }, function (error) {
                test.push(error.message)
            }], function () {
                okay(test, [ 'NotFoundError: not found' ], 'get not found')
            })
        }, function () {
            locket.put(Buffer.from('a'), Buffer.from('z'), step())
        }, function () {
            locket.close(step())
        }, function () {
            locket.open({ createIfMissing: true }, step())
        }, function () {
            step(function () {
                locket.get('a', step())
            }, function (value) {
                okay({
                    isBuffer: Buffer.isBuffer(value),
                    value: value.toString()
                }, {
                    isBuffer: true,
                    value: 'z'
                }, 'reopen')
            })
        }, function () {
            const iterator = locket._iterator({
                keys: true, values: true, keyAsBuffer: false, valueAsBuffer: false
            })
            step(function () {
                iterator.next(step())
            }, function (key, value) {
                okay({ key, value }, { key: 'a', value: 'z' }, 'next')
                iterator.next(step())
            }, [], function (ended) {
                okay(ended, [], 'ended')
                iterator.end(step())
            })
        }, function () {
            const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('')

            const put = alphabet.map(letter => { return { type: 'put', key: letter, value: letter } })
            const del = alphabet.map(letter => { return { type: 'del', key: letter } })

            step.loop([ 0 ], function (i) {
                // **TODO** Double this number and it hangs indefinately.
                if (i == 64) {
                    return [ step.break ]
                }
                step(function () {
                    locket.batch(put.concat(del), step())
                }, function () {
                    return i + 1
                })
            })
        }, function () {
            console.log('closeing')
            locket.close(step())
        }, function () {
            console.log('closed')
            locket.open({ createIfMissing: true }, step())
        }, function () {
            locket.close(step())
        })
    })
}))
