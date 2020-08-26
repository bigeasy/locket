require('proof')(2, okay => {
    const path = require('path')
    const AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN

    const Locket = require('../..')

    try {
        const locket = Locket(path.join('t', 'tmp'))
        okay(locket instanceof AbstractLevelDOWN, 'is a leveldown implementation')
    } catch (e) {
        assert(e.message, 'constructor requires at least a location argument')
    }
})
