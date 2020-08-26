const $lookup = { key: require('./key.lookup'), meta: require('./meta.lookup') }

exports.key = {
    sizeof: require('./key.sizeof'),
    serialize: require('./key.serializer.all')({ $lookup: $lookup.key }),
    parse: require('./key.parser.all')({ $lookup: $lookup.key })
}

exports.meta = {
    sizeof: require('./meta.sizeof').object,
    serialize: require('./meta.serializer.all')({ $lookup: $lookup.meta }).object,
    parse: require('./meta.parser.all')({ $lookup: $lookup.meta }).object
}
