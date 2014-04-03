Todo
====

Now
---

- fill in pending instruction tests

Soon
----

- findAndModify instruction
- pre-transaction-store hook for each instruction, to be used for id generation (for new docs or new revisions), will save the extra transaction collection write 
- transaction-token feature, to allow optional expiry of transactions based on reads which were in-progress during a global-lock transaction. 
- rollbackProjection needs to respect elemMatch, otherwise there are lots of circumstances where too much state is saved


Later
-----


- infrastructure for easing instruction creation, eg a class structure which supplies support for updating instruction state etc, or a module of helper fns to require.


