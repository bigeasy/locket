#!/usr/bin/env node

require('proof')(1, function (equal) {
  equal(require('../..'), 1, 'require');
});
