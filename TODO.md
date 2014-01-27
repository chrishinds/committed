Todo
====


Now
---

- revisioned instructions and tests of revisioning
- function execution within queues, eg via exports.enqueue, could be used for reads or writes without transactions. 
- separate exports.readWithin to separate functions which just do reads from those which might write too

Soon
----

- when instructions fail they should push info strings explaining why
- updateMultiOp instruction
- pre-transaction-store hook for each instruction, to be used for id generation (for new docs or new revisions), will save the extra transaction collection write 


Later
-----

- exports.readWithin can just equal enqueue for the moment, but should ideally result in immediate execution when queue is empty. however, flipping from queued to non-queued execution will add complexity, esp since we already shutdown/restart for globallocks.
- Current instruction set is quite specifically revisioned. Should hence become exports.instructions.revisioned. 
- Create some more generic instructions
- infrastructure for easing instruction creation, eg a class structure which supplies support for updating instruction state etc.


