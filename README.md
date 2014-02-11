committed
=========

This library can be used to create mongodb transactions that are Atomic, Isolated and Durable. If you're using mongo you already know it's not ACID compliant. This library doesn't magically change that. However, used carefully it can help you hit some common requirements. Isolation for multi-collection operations is easy to achieve using committed's transaction queues. Atomicity and Durability require more effort, as you must capture db operations as 'instructions' with matching rollbacks. A small set of instructions is supplied. When used in this way, transaction objects are written to their own collection which therefore also acts as a robust audit trail.

We promise to document once our project's hit production, which is in anycase likely to conicide with this library becoming usably mature. 