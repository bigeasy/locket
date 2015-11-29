# Locket Diary

## Design Discussion

Going to need an iterator that goes to the left in Strata, on that goes to the
key before the given key. `strata.iterator(key, { before: true }, callback)`

## Merge

When merging one of the staging trees, it seems kind of silly to actually delete
the records, which really means writing a delete. Better to simply obliterate
the tree when you're done and recreate it? Yes, but that is a new state. One for
which I don't have a recovery planned. I do have a recover strategy for a tree
that fails in the middle of a delete or balance.

How do I know which transactions succeeded? I'll only write the transaction if
there are actual operations in the `batch`. Then, when I encounter a transaction
id in the `batch`, I know to remove the transaction from the transaction
collection. At the end of the merge I promote the least transaction id using the
in memory transaction hash.

A stage tree must be completely merged before we merge the next tree. We can't
merge a little of one tree, then merge a little of the other. Why? Because we're
counting on this transaction id to always increase. When the iterator performs
the three way merge, it will compare the two stage trees against the primary
tree. We can be certain that the final, committed action our stage tree is the
one that needs to be merged into the primary tree.

Let's say we've allowed the user to toggle back and forth between stage trees.
The user writes transaction 1 to the secondary stage tree. The user swaps trees
telling Locket to merge the secondary stage tree and to log to the tertiary
stage tree. The user writes transaction 2 to the tertiary stage tree. The user
then tells Locket to swap back to secondary stage tree and to merge the tertiary
stage tree. But, Locket never got around to merging transaction 1, concurrency
don't ya know, and transaction 1 is the secondary stage tree. Meanwhile,
transaction 2 is in the tertiary stage tree and is merged into the primary tree.
When transaction 2 is merged into the primary tree it becomes transaction 0.
Any valid transaction is a log takes precedence over the primary tree. The
transaction 1 that was superseded by transaction 2 now vetoes the values that
transaction 2 has merged into the primary tree.

We're always moving forward, merging one tree and then the next. With this
system, we can take our time with with a staging tree merging it into the
primary tree. Then swap back. The user should not be calling merge faster than
the merge operation can merge a stage.

Also, we're going to have to merge when we open a Locket, because we don't have
a way to know which stage was active, which was merging. We can make this less
painful by taking a sip of each stage so we can launch with an empty stage if
one exists, merging the full one.

## Revision Id Forever

I'm not sure why I'm not keeping the revision id in the main table. It costs
nothing and then I can use my able, baker stage names, which I won't do, but it
won't matter what order things are merged, because I'll have a revision id and I
won't overwrite any record.

## Replication

Now that we have an archive, we can transport it to other servers as a log, and
replay it, in pretty much any order. Latest version wins. Simply drop the files
into the staging area, push them onto the end of the queue, and balance. Just as
performant as any merge.

## Extract Iterators / Merge

May as well extract our nice iterator and merge into `strata.merge`.

## In-Memory Merge

One at a time, it would be simple to insert an entry into a tree. When it is a
batch, if the batch is large, say a large append, we're going to block everyone
if we are appending to a staging log that is already part of the collective
tree. Anyone reading is going to wait on the write to the tail.

However, we're only writing in the sense that we're merging the batch, not in
the sense that we're waiting for the write to flush.

I start to worry about when a version is committed, but then I recall that this
is not a concern for LevelDB and is therefore not a concern for Locket.

## Multiple Writers

They would all write to their own log, but the logs would be merged into a
single page, so that loading the page reads in many logs. They records would get
put in the right order and MVCC takes over. Still only ever two outstanding
pages, but many logs for those pages.
