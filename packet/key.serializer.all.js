module.exports = function ({ $lookup }) {
    return {
        object: function () {
            return function (object, $buffer, $start) {
                $buffer[$start++] = Number(object.version >> 56n & 0xffn)
                $buffer[$start++] = Number(object.version >> 48n & 0xffn)
                $buffer[$start++] = Number(object.version >> 40n & 0xffn)
                $buffer[$start++] = Number(object.version >> 32n & 0xffn)
                $buffer[$start++] = Number(object.version >> 24n & 0xffn)
                $buffer[$start++] = Number(object.version >> 16n & 0xffn)
                $buffer[$start++] = Number(object.version >> 8n & 0xffn)
                $buffer[$start++] = Number(object.version & 0xffn)

                $buffer[$start++] = object.index >>> 24 & 0xff
                $buffer[$start++] = object.index >>> 16 & 0xff
                $buffer[$start++] = object.index >>> 8 & 0xff
                $buffer[$start++] = object.index & 0xff

                return { start: $start, serialize: null }
            }
        } ()
    }
}
