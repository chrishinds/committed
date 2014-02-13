Todo
====

have removed the chain retry functionality.

need a test to demonstrate non-blocking nature of chains and repeated queue insert

Now
---

- bug: chains with transactions in the same queue fail
- fill in pending instruction tests

Soon
----

- findAndModify instruction
- pre-transaction-store hook for each instruction, to be used for id generation (for new docs or new revisions), will save the extra transaction collection write 
- translate mongo $operators into __operator on the way in; having to mix __ and $ is confusing.
- instruction to update every revision in a collection (utilizing config.revisions), could be necessary after a global-lock transaction, could be expensive though, not necessary if all updates are queue-protected

Later
-----


- infrastructure for easing instruction creation, eg a class structure which supplies support for updating instruction state etc, or a module of helper fns to require.


