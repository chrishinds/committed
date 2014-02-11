Todo
====


Now
---

- updateManyOp instruction
	- returns # documents updated
- fill in pending instruction tests

Soon
----

- findAndModify instruction
	- implies a change so that transactions and chains can return objects in addition to just (err, status)
- pre-transaction-store hook for each instruction, to be used for id generation (for new docs or new revisions), will save the extra transaction collection write 
- translate mongo $operators into __operator on the way in; having to mix __ and $ is confusing.
- instruction to update every revision in a collection (utilizing config.revisions), could be necessary after a global-lock transaction, could be expensive though, not necessary if all updates are queue-protected

Later
-----

- exports.readWithin can just equal enqueue for the moment, but should ideally result in immediate execution when queue is empty. however, flipping from queued to non-queued execution will add complexity, esp since we already shutdown/restart for globallocks.
- Current instruction set is quite specifically revisioned. Should hence become exports.instructions.revisioned. 
- Create some more generic instructions?
- infrastructure for easing instruction creation, eg a class structure which supplies support for updating instruction state etc, or a module of helper fns to require.


