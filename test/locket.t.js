require('proof')(6, async okay =>  {
    const path = require('path')
    const fs = require('fs')

    const Destructible = require('destructible')
    const destructible = new Destructible('put.t')

    const callback = require('prospective/callback')
    const rimraf = require('rimraf')

    const Locket = require('..')

    const tmp = path.join(__dirname, './tmp')
    const location = path.join(tmp, 'locket')

    await callback(callback => rimraf(location, callback))

    const locket = Locket(destructible, location, {
        primary: {
            leaf: { split: 64, merge: 32 },
            branch: { split: 64, merge: 32 },
        },
        stage: {
            max: 128 * 8,
            leaf: { split: 64, merge: 32 },
            branch: { split: 64, merge: 32 },
        }
    })
    await callback(callback => locket.open({ createIfMissing: true }, callback))

    await callback(callback => locket.put('a', 'z', callback))

    {
        const [ value ] = await callback(callback => locket.get('a', callback))
        okay({
            isBuffer: Buffer.isBuffer(value),
            value: value.toString()
        }, {
            isBuffer: true,
            value: 'z'
        }, 'put')
    }

    {
        const [ value ] = await callback(callback => locket.get('a', { asBuffer: false }, callback))
        okay({
            type: typeof value,
            value: value
        }, {
            type: 'string',
            value: 'z'
        }, 'put')
    }

    await callback(callback => locket.del('a', callback))

    {
        const test = []
        try {
            await callback(callback => locket.get('a', callback))
        } catch (error) {
            test.push(error.message)
        }
        okay(test, [ 'NotFoundError: not found' ], 'get not found')
    }

    await callback(callback => locket.put(Buffer.from('a'), Buffer.from('z'), callback))

    await callback(callback => locket.close(callback))

    await callback(callback => locket.open({ createIfMissing: true }, callback))

    {
        const [ value ] = await callback(callback => locket.get('a', callback))
        okay({
            isBuffer: Buffer.isBuffer(value),
            value: value.toString()
        }, {
            isBuffer: true,
            value: 'z'
        }, 'reopen')
    }

    {
        const iterator = locket._iterator({
            keys: true, values: true, keyAsBuffer: false, valueAsBuffer: false
        })
        const [ key, value ] = await callback(callback => iterator.next(callback))
        okay({ key, value }, { key: 'a', value: 'z' }, 'next')
        const ended = await callback(callback => iterator.next(callback))
        okay(ended, [], 'ended')
        await callback(callback => iterator.end(callback))
    }

    const alphabet = 'abcdefghijklmnopqrstuvwxyz'.split('')

    const put = alphabet.map(letter => { return { type: 'put', key: letter, value: letter } })
    const del = alphabet.map(letter => { return { type: 'del', key: letter } })

    for (let i = 0; i < 128; i++) {
        await callback(callback => locket.batch(put.concat(del), callback))
    }

    await callback(callback => locket.close(callback))
})
