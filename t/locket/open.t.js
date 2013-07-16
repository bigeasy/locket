#!/usr/bin/env node

require('proof')(6, function (step, equal, deepEqual) {
    var path = require('path')
    var fs = require('fs')
    var tmp = path.join(__dirname, '../tmp')
    var rimraf = require('rimraf')
    var Locket = require('../..')
    var cadence = require('cadence')

    step(function () {
        var invalid = path.join(tmp, 'invalid')
        var locket
        step(function () {
            rimraf(tmp, step())
        }, [function () {
            locket = new Locket(invalid)
            locket.open(step())
        }, function (_, error) {
          equal(error.message, 'does not exist')
        }])
    }, function () {
        var locket
        step(function () {
            rimraf(tmp, step())
        }, [function () {
            locket = new Locket(__dirname)
            locket.open(step())
        }, function (_, error) {
          equal(error.message, 'not a Locket datastore')
        }])
    }, function () {
        var empty = path.join(tmp, 'empty')
        var locket
        step(function () {
            rimraf(tmp, step())
        }, function () {
            locket = new Locket(empty)
            locket.open({ createIfMissing: true }, step())
        }, function () {
            fs.readdir(empty, step())
        }, function (listing) {
            deepEqual(listing.sort(), [ 'primary', 'stages', 'transactions' ], 'created')
            fs.readdir(path.join(empty, 'stages'), step())
        }, function (listing) {
            deepEqual(listing.sort(), [ 'able' ], 'stages created')
        }, function () {
            locket.close(step())
        })
    }, function () {
        var empty = path.join(tmp, 'empty')
        var locket
        step(function () {
            locket = new Locket(empty)
            locket.open({}, step())
        }, function () {
            fs.readdir(empty, step())
        }, function (listing) {
            deepEqual(listing.sort(), [ 'primary', 'stages', 'transactions' ], 'reopened')
            fs.readdir(path.join(empty, 'stages'), step())
        }, function (listing) {
            deepEqual(listing.sort(), [ 'able', 'baker' ], 'stages reopened')
        }, function () {
            locket.close(step())
        }, function () {
            locket.close(step()) // test double close
        })
    })
})
