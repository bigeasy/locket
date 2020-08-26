// Return the first value that is not `null` nor `undefined`.
const coalesce = require('extant')
const ascension = require('ascension')

const Strata = require('b-tree')

function create (destructible, options) {
    const leaf = coalesce(options.leaf, 4096)
    const branch = coalesce(options.leaf, 4096)
    const strata = new Strata(destructible, {
        directory: options.directory,
        serializer: options.serializer,
        comparator: ascension([ options.comparator, BigInt ], function (object) {
            return [ object.value, object.version ]
        }),
        branch: { split: branch, merge: branch - 1 },
        leaf: { split: leaf, merge: leaf - 1 },
        cache: options.cache
    })
    return strata
}

exports.create = create
