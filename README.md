problem
==========

Write a parallel processing of events from queue. Each event has and `id` and `groupId`.
No two events of same group should be processed at the same time. Order of events within the group should be intact.
