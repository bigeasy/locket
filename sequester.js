class Sequester {
    constructor () {
        this._queue = []
    }

    _lock (extra) {
        const lock = { ...extra, promise, resolve }
        lock.promise = new Promise(resolve => lock.resolve = resolve)
        return lock
    }

    async share () {
        for (;;) {
            if (this._queue[0].exclusive) {
                await this._queue[this._queue.length - 1].promise
            } else {
                this._queue[0].shares++
                break
            }
        }
    }

    async write () {
        const exclusive = this._lock({ exclusive: true })
        this._queue.push(exclusive, this._lock({ exclusive: true, shares: 0 }))
        await this.share()
        this.unlock()
        await exclusive.promise
    }

    unlock () {
        if (this._queue[0].exclusive) {
            this._queue.shift()
            this._queue[0].resolve.call()
        } else {
            if (--this._queue[0].shares == 0 && this._queue.length != 1) {
                this._queue.shift()
                this._queue[0].resolve.call()
            }
        }
    }
}
