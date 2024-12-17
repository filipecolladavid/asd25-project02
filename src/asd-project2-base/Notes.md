# Questions ABD
How to deal with processes joining the system:
    - How are requests made? As messages, how to quorum without majority ? Assume that at least 3 nodes join the system together ?

How to order operations? Is it a requirement? 


# Implementation
Queue store operations received (Read/Write/Join request).<br>
Operations are only executed on the timer.<br>
Queue is only pop when receive confirmation.<br>


