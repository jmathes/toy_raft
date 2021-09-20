# toy_raft
Demo implementation of the Raft consensus algorithm by Diego Ongaro and John Ousterhout

A quick, toy implementation of something resembling raft.

Original raft presentation:
https://www.youtube.com/watch?v=vYp4LYbnnW8

Original raft paper:
https://raft.github.io/raft.pdf

"pytest" in root directory to run tests

--------------------------

The core of the algorithm is all in the Server class, with extensive unit tests.
The Server class is designed to be puppeteered in multiples to simulate a cluster, but
I didn't have time to write those tests. I also ran out of time before adding support
for I/O with a client.
