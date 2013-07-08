#!/usr/bin/env node

require('proof')(2, function (step, ok, equal) {
  var Locket = require('../..')
  var locket

  try {
    locket = new Locket
  } catch (e) {
    equal(e.message, 'constructor requires at least a location argument')
  }

  var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN
  var path = require('path')

  locket = Locket(path.join('t', 'tmp'))

  ok(locket instanceof AbstractLevelDOWN, 'is a leveldown implementation')
})
