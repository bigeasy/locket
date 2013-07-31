var test = require('tap').test
var testCommon = require('abstract-leveldown/testCommon')
var Locket = require('./index')
var factory = function (location) { return new Locket(location) }

require('abstract-leveldown/abstract/leveldown-test').args(factory, test, testCommon)
require('abstract-leveldown/abstract/open-test').all(factory, test, testCommon)
