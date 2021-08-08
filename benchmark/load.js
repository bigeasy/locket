#!/usr/bin/env node

Error.stackTraceLimit = 88

/*
  ___ usage ___ en_US ___
  usage: node load.js

    All around tests for benchmarking Locket.

  options:

    -d, --leveldown
        use leveldown instead of Locket.
  ___ . ___
*/

const Destructible = require('destructible')
const Locket = require('../')
const cadence = require('cadence')
const path = require('path')
const crypto = require('crypto')
const seedrandom = require('seedrandom')
const levelup = require('levelup')
const leveldown = require('leveldown')

const fs = require('fs').promises

const { callback } = require('comeuppance')

const random = (function () {
    const random = seedrandom(0)
    return function (max) {
        return Math.floor(random() * max)
    }
})()

const runner = cadence(function (step, arguable) {
    let start, insert, gather, level
    const tmp = path.join(__dirname, 'tmp')
    const o = { createIfMissing: true }
    const destructible = new Destructible('benchmark/load')
    const batches = []
    let key = 0
    while (batches.length != 124) {
        const entries = []
        for (let i = 0; i < 1024; i++) {
            if (false) {
            const value = random(1024)
            const sha = crypto.createHash('sha1')
            const buffer = Buffer.alloc(4)
            buffer.writeUInt32BE(value, 0)
            sha.update(buffer)
            entries.push({
                key: sha.digest(),
                value: buffer,
                type: !! random(2) ? 'put' : 'del'
            })
            } else {
            const buffer = Buffer.alloc(4)
            buffer.writeUInt32BE(key++, 0)
            entries.push({
                key: buffer,
                value: buffer,
                type: !! random(2) ? 'put' : 'del'
            })
            }
        }
        batches.push(entries)
    }
    destructible.promise.catch(error => console.log(error.stack))
    destructible.destruct(() => 'destructing')
    step(function () {
        return fs.rm(tmp, { recursive: true, force: true })
    }, function () {
        return fs.mkdir(tmp, { recursive: true })
    }, function () {
        start = Date.now()
        if (arguable.ultimate.leveldb) {
            const file = path.join(tmp, 'put')
            return leveldown(file)
        } else {
            const file = path.join(tmp, 'put')
            step(function () {
                return fs.mkdir(file)
            }, function () {
                return new Locket(file)
            })
        }
    }, function (leveldown) {
        const db = levelup(leveldown)
        step(function () {
            let batch = 0
            const loop = step.loop([ 0 ], function (i) {
                if (i == 124) {
                    return [ loop.break ]
                }
                step(function () {
                    console.log(i)
                    db.batch(batches[i], step())
                }, function () {
                    return i + 1
                })
            })
        }, function () {
            db.close(step())
        })
    }, function () {
        insert = Date.now() - start
        start = Date.now()
        return
        if (arguable.ultimate.leveldb) {
            const file = path.join(tmp, 'put')
            return leveldown(file)
        } else {
            const file = path.join(tmp, 'put')
            return new Locket(file)
        }
    }, function (leveldown) {
        return
        const db = levelup(leveldown)
        step(function () {
            step.ee(db.createReadStream())
                .on('data', function (data) { records.push(data) })
                .end('end')
                .error()
        }, function () {
            db.close(step())
        }, function () {
            gather = Date.now() - start
            console.log('insert: ' + insert + ', gather: ' + gather)
        })
    }, function () {
        console.log('insert: ' + insert)
        return destructible.destroy().promise
    })
})

/*
    ___ usage ___ en_US ___
    usage: prolific <options> <program>

    options:

        -l, --leveldb
            use leveldb

    ___ $ ___ en_US ___

    ___ . ___
*/

require('arguable')(module, async arguable => {
    await callback(callback => runner(arguable, callback))
})
