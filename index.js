module.exports = Locket

var Strata = require('b-tree')
var AbstractLevelDOWN = require('abstract-leveldown').AbstractLevelDOWN

var util = require('util')
var fs = require('fs')
var path = require('path')

var cadence = require('cadence')
var mkdirp = require('mkdirp')

function Locket (location) {
    if (!(this instanceof Locket)) return new Locket(location)
    AbstractLevelDOWN.call(this, location)
}

util.inherits(Locket, AbstractLevelDOWN)

Locket.prototype._open = cadence(function (step, options) {
    var exists = true
    step(function () {
        step([function () {
            fs.readdir(this.location, step())
        }, 'ENOENT', function (_, error) {
            if (options.createIfMissing) {
                exists = false
                mkdirp(this.location, step())
            } else {
                throw new Error('does not exist')
            }
        }])(2)
    }, function (listing) {
        var subdirs = [ 'primary', 'secondary', 'tertiary', 'transactions' ]
        if (exists) {
          listing = listing.filter(function (file) { return file[0] != '.' }).sort()
          if (listing.length && !listing.every(function (file, index) { return subdirs[index] == file })) {
              throw new Error('not a Locket datastore')
          }
        } else {
          subdirs.forEach(step([], function (dir) {
              fs.mkdir(path.join(this.location, dir), step())
          }))
        }
    }, function () {
        this._primary = new Strata(path.join(this.location, 'primary'), {
            leafSize: 1024,
            branchSize: 1024
        })
        if (!exists) this._primary.create(step())
    }, function () {
        this._primary.open(step())
        this._isOpened = true
    })
})

Locket.prototype._close = cadence(function (step, operations) {
    if (this._isOpened) step(function () {
        this._primary.close(step())
    }, function () {
        this._isOpened = false
    })
})
