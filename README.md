# neko-dataflow

## What is neko-dataflow
It is a golang implementation of [timely-dataflow](https://cs.stanford.edu/~matei/courses/2015/6.S897/readings/naiad.pdf). It is a library to program streaming data computing, which supports distributed pipelined and iterative computing.

API documentation is still under construction. For more use cases of this library, please refer to examples.

## TODOs

- Currently only single threaded scheduler version. Need to design and implement distributed version.
- Better API wrapping for vertices, scheduler, etc.
- Epoch is there in timestamp, but we need to implement the mechanism for epoch updates to retire old epoch and therefore drain old epoch message.
- Notify fence needs to be implemented. When a NotifyAt is sent with invalid timestamp, we need to know what to do.
- Batch data processing.