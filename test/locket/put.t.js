require('proof')(2, async okay =>  {
    const path = require('path')
    const fs = require('fs')

    const Destructible = require('destructible')
    const destructible = new Destructible('put.t')

    const callback = require('prospective/callback')
    const rimraf = require('rimraf')

    const Locket = require('../..')

    const tmp = path.join(__dirname, '../tmp')
    const location = path.join(tmp, 'put')

    await callback(callback => rimraf(location, callback))

    const locket = new Locket(destructible, location)
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

    await callback(callback => locket.close(callback))
})
