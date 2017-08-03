require('proof')(2, prove)

function prove (assert) {
    var Locket = require('../..')
    var locket

    try {
        locket = new Locket
    } catch (e) {
        assert(e.message, 'constructor requires at least a location argument')
    }

    var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
    var path = require('path')

    locket = Locket(path.join('t', 'tmp'))

    assert(locket instanceof AbstractLevelDOWN, 'is a leveldown implementation')
}
