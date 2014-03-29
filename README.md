# Locket

A pure-JavaScript LevelDB implementation backed by a durable and persistent
evented I/0 b-tree for use with LevelUP.

<a href="http://www.flickr.com/photos/bzedan/2611547954/" title="Salvage Necklace 5: inside locket by B_Zedan, on Flickr"><img src="http://farm4.staticflickr.com/3273/2611547954_23eff61651_o.jpg" width="722" height="481" alt="Salvage Necklace 5: inside locket"></a>

Salvage Necklace 5: Inside Locket by [B Zedan](http://www.flickr.com/people/bzedan/).

# Example

wrap with [levelup](https://github.com/rvagg/node-levelup) to before use

``` js
var locket = require('locket')
var levelup = require('levelup')

var db = levelup('/tmp/my-locket-db', {db: locket})

db.put('foo', 'bar', function (err) {
  if(err) throw err
  db.get('foo', function (err, data) {
    if(err) throw err
    console.error(data)
  })
})

```

now you are compatible with all level-* modules!

# License

MIT

