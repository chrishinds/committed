Todo
====

Now
---

writer transactions which are committed immediately and produce a chain dont callback on chain completion. path of least resistance could be to create a class which looks like async.queue, but which commits immediately, and have a special 'Immediately' queue.

Any chain that produces a global lock transaction will not end up correctly setting the _state=locked flag, hence the queue drain it puts in place may never progress.


Soon
----

- mongo 2.6 compatibility (dunder issue #2)
- CI server to sustain 2.4 & 2.6
- fill in pending instruction tests
- findAndModify instruction
- pre-transaction-store hook for each instruction, to be used for id generation (for new docs or new revisions), will save the extra transaction collection write 
- transaction-token feature, to allow optional expiry of transactions based on reads which were in-progress during a global-lock transaction. 
- rollbackProjection needs to respect elemMatch, otherwise there are lots of circumstances where too much state is saved
- rollback info to be removed when state is updated with committed status: possibly a better solution to the state inflation problem than tricksy projection processing, maybe have this on a config flag. 
- update instr to increment revisions already on an object if none are explicitly given?

Later
-----


- infrastructure for easing instruction creation, eg a class structure which supplies support for updating instruction state etc, or a module of helper fns to require.


