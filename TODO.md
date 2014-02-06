Todo
====


Now
---

- fill in pending tests
- enqueue functions which might then produce a transaction for execution, or not.
- function execution within queues, eg via exports.enqueue, could be used for reads or writes without transactions. 
- separate exports.enqueueRead to separate functions which just do reads from those which might write too, because when the queue's empty it's possible to do the reads in parallel.

Soon
----

- pre-transaction-store hook for each instruction, to be used for id generation (for new docs or new revisions), will save the extra transaction collection write 
- updateMultiOp instruction
- instruction to update every revision in a collection (utilizing config.revisions), could be necessary after a global-lock transaction, could be expensive though, not necessary if all updates are queue-protected

Later
-----

- exports.readWithin can just equal enqueue for the moment, but should ideally result in immediate execution when queue is empty. however, flipping from queued to non-queued execution will add complexity, esp since we already shutdown/restart for globallocks.
- Current instruction set is quite specifically revisioned. Should hence become exports.instructions.revisioned. 
- Create some more generic instructions?
- infrastructure for easing instruction creation, eg a class structure which supplies support for updating instruction state etc, or a module of helper fns to require.


