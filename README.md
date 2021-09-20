# toy_raft
Demo implementation of something resembling the Raft consensus algorithm created by
Diego Ongaro and John Ousterhout in 2016.

Raft is designed to be the simplest possible non-byzantine consensus algorithm.
Raft is Replicated And Fault Tolerant.
Raft is made with logs.
Raft can be used to escape the island of Paxos.

Original raft presentation:
https://www.youtube.com/watch?v=vYp4LYbnnW8

Original raft paper:
https://raft.github.io/raft.pdf

"pytest" in root directory to run tests

--------------------------

The core of the algorithm is all in the Server class, with extensive unit tests.
There are 4x as many lines of tests as there are of code, using a few cool features
of pytest and pytest_describe.

The Server class is designed to be puppeteered in multiples to simulate a cluster.
TODO:
- Add integration tests that run multiple servers at once
- Add support for accepting, forwarding, and responding to client requests
- Create a toy framework to run a cluster of Raft servers in Flask
