// # Locket
//
// [![Actions Status](https://github.com/bigeasy/locket/workflows/Node%20CI/badge.svg)](https://github.com/bigeasy/locket/actions)
// [![codecov](https://codecov.io/gh/bigeasy/locket/branch/master/graph/badge.svg)](https://codecov.io/gh/bigeasy/locket)
// [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
//
// <sub>_Salvage Necklace 5: Inside Locket by [B Zedan](http://www.flickr.com/people/bzedan/)._</sub>
//
// <a href="http://www.flickr.com/photos/bzedan/2611547954/" title="Salvage Necklace 5: inside locket by B_Zedan, on Flickr"><img src="http://farm4.staticflickr.com/3273/2611547954_23eff61651_o.jpg" width="722" height="481" alt="Salvage Necklace 5: inside locket"></a>
//
//
// A pure-JavaScript [leveldown](https://github.com/Level/leveldown) implementation
// backed by a persistent and durable evented I/0 b-tree for use with
// [levelup](https://github.com/Level/leveldown) &mdash; i.e. a database.
//
// | What          | Where                                         |
// | --- | --- |
// | Discussion    | https://github.com/bigeasy/locket/issues/1    |
// | Documentation | https://bigeasy.github.io/locket              |
// | Source        | https://github.com/bigeasy/locket             |
// | Issues        | https://github.com/bigeasy/locket/issues      |
// | CI            | https://travis-ci.org/bigeasy/locket          |
// | Coverage:     | https://codecov.io/gh/bigeasy/locket          |
// | License:      | MIT                                           |
//
// Locket installs from NPM.

// ## Living `README.md`
//
// This `README.md` is also a unit test using the
// [Proof](https://github.com/bigeasy/proof) unit test framework. We'll use the
// Proof `okay` function to assert out statements in the readme. A Proof unit test
// generally looks like this.

require('proof')(1, okay => {
    // ## Usage

    okay('okay')
})

// You can run this unit test yourself to see the output from the various
// code sections of the readme.
