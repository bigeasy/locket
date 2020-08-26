## Thu Aug 20 15:56:56 CDT 2020

Simple enough to ensure that at shutdown and at restart we merge any outstanding
cursors. That settles the problem of keeping track of versions. There is a
recover step to extract the valid versions, then we merge.

Actually, that is all that needs to happen then, recovering the outstanding
versions. Simplifies things. We can then warn if the b-tree hits a corrupted
merge page, not as serious as having the primary tree in a bad state.

If we're very concerned we could keep a log to verify state. Learning to have
some faith in the file system. If the log entries are less than the buffer size
we can append to a log and it will rarely be corrupted.

For now, it will be enough to extract all valid verisons out of the staging
pages.
