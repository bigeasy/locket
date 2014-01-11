module.exports = function (suite, tests) {
    var test        = require('tap').test
    var testCommon  = require('abstract-leveldown/testCommon')
    var Locket      = require('../..')

    function factory (location) { return new Locket(location) }

    var path = 'abstract-leveldown/abstract/' + suite + '-test'

    require(path)[tests](factory, test, testCommon, new Buffer('a'))
}
