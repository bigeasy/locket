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

    const locket = Locket(destructible, location)
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

    {
        const test = []
        try {
            await callback(callback => locket.get('z', callback))
        } catch (error) {
            test.push(error.message)
        }
        okay(test, [ 'NotFoundError: not found' ], 'get not found')
    }

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

    await callback(callback => locket.close(callback))
})
