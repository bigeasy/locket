At some point, it was decided that all the significant logic be exported to
supporting libraries. The MVCC logic grouped into a collection of libraries that
are linked by the `mvcc` project on NPM. Encoding and the the pair record
structure were also extracted into libraries that are separate from the MVCC
collection, they are specific to LevelDB while the MVCC collection can be used
to build other databases.

### Issue by Issue

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
 * Implement AbstractLevelDOWN batch tests. #57.
 * Implement AbstractLevelDOWN close tests. #55.
 * Implement AbstractLevelDOWN del tests. #54.
 * Implement AbstractLevelDOWN iterator tests. #53.
 * Implement AbstractLevelDOWN put tests. #51.
 * Implement AbstractLevelDOWN get tests. #49.
 * Pass all AbstractLevelDOWN open tests. #34.
