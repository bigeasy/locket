At some point, it was decided that all the significant logic be exported to
supporting libraries. The MVCC logic grouped into a collection of libraries that
are linked by the `mvcc` project on NPM. Encoding and the pair record structure
were also extracted into libraries that are separate from the MVCC collection,
they are specific to LevelDB while the MVCC collection can be used to build
other databases.

### Issue by Issue

 * Upgrade Proof to 0.0.46. #141.
 * Upgrade Cadence to 0.0.38 #140.
 * Upgrade Turnstile to 0.0.3. #134.
 * Upgrade Timezone to 0.0.30. #133.
 * Create object to wrap Strata tree. #132.
 * Incorrectly verifying validity of Locket directory. #131.
 * Release version 0.0.1. #130.
 * Create `Options` to manage options. #129.
 * Upgrade Strata to 0.0.18. #128.
 * Upgrade Strata to 0.0.17. #127.
 * Upgrade Proof to 0.0.44. #126.
 * Add Turnstile as a dependency. #125.
 * Upgrade Cadence to 0.0.36. #124.
 * Remove "url" from `package.json`. #120.
 * Upgrade Cadence to 0.0.35. #118.
 * Stop using Cadence boolean handlers. #117.
 * Open iterators using Cadence gathered loop. #116.
 * Upgrade Cadence to 0.0.34. #115.
 * Implement `end` property. #114.
 * Add `development` branch to `.travis.yml`. #113.
 * Upgrade Constrain to 0.0.8. #112.
 * Update `LICENSE` for 2014. #109.
 * Test minimal stream via LevelUP. #108.
 * Upgrade Constrain to 0.0.7. #107.
 * Test get through LevelUP. #106.
 * `LevelUP.createReadStream` with returns empty buffer. #105.
 * Implement `Locket.approximateSize`. #104.
 * Upgrade Skip to 0.0.6. #103.
 * Extract Dilute creation to Locket object. #102.
 * Upgrade Constrain to 0.0.6. #101.
 * Select correct direction for designation iterator. #100.
 * Upgrade Designate to 0.0.3. #99.
 * Upgrade Sequester to 0.0.8. #98.
 * Upgrade Strata to 0.0.16. #97.
 * Upgrade Proof to 0.0.41. #96.
 * Upgrade Advance to 0.0.5. #95.
 * Revert to MIT. #94.
 * Upgrade Strata to 0.0.12. #93.
 * Upgrade Advance to 0.0.4. #92.
 * Add LGPL 3.0. #91.
 * Build only on Node.js 0.10 at Travis CI. #90.
 * Use Dilute. #89.
 * Upgrade Strata to 0.0.11. #88.
 * Ugprade Correlate to 0.0.5. #87.
 * Upgrade Advance to 0.0.3. #86.
 * Upgrade Riffle to 0.0.5. #85.
 * Upgrade Skip to 0.0.5. #84.
 * Upgrade Correlate to 0.0.4. #83.
 * Upgrade Correlate to 0.0.3. #82.
 * Use Correlate `valid` function. #81.
 * Upgrade Correlate to 0.0.2. #80.
 * Use Correlate to interpret iterator range properties. #79.
 * Implement AbstractLevelDOWN constructor tests. #77.
 * Re-implement merge using abstracted libraries. #76.
 * Re-implement iterator using abstracted libraries. #75.
 * Re-implement batch using abstracted libraries. #74.
 * Re-implement delete using abstracted libraries. #73.
 * Re-implement get using extracted libraries. #72.
 * Re-implement put using extracted libraries. #71.
 * Re-implement create using extracted libraries. #69.
 * Change `_lastTransactionId` to `_version`. #68.
 * Re-implement open using extracted libraries. #67.
 * Implement the AbstractLevelDOWN approximate size tests. #58.
 * Implement AbstractLevelDOWN batch tests. #57.
 * Implement AbstractLevelDOWN chained batch tests. #56.
 * Implement AbstractLevelDOWN close tests. #55.
 * Implement AbstractLevelDOWN del tests. #54.
 * Implement AbstractLevelDOWN iterator tests. #53.
 * Implement the AbstractLevelDOWN put/get/del tests. #52.
 * Implement AbstractLevelDOWN put tests. #51.
 * Implement AbstractLevelDOWN ranges tests. #50.
 * Implement AbstractLevelDOWN get tests. #49.
 * Pass all AbstractLevelDOWN open tests. #34.
