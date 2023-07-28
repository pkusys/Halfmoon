Halfmoon
==================================

Halfmoon is a research FaaS runtime for log-based fault-tolerant stateful serverless computing with exactly-once semantics.
Halfmoon provides a client library with log-optimal logging protocols for stateful serverless functions to access shared state using simple read/write APIs.
Halfmoon is built on top of [Boki](https://github.com/ut-osa/boki), a serverless runtime that features an efficient logging layer based on shared logs.

We choose Halfmoon as the name of our runtime system because it offers a pair of protocols that are log-free on reads or writes, respectively, as opposed to existing solutions that logs all reads and writes.

### Building Halfmoon ###

Under Ubuntu 20.04, first install the dependencies with
~~~
sudo apt install g++ make cmake pkg-config autoconf automake libtool curl unzip
~~~

Once completed, build Halfmoon with:

~~~
CC=clang CXX=clang++ ./build_deps.sh
CXX=clang++ make -j $(nproc)
~~~

### Kernel requirements ###

Halfmoon's underlying runtime, Boki, uses [io_uring](https://en.wikipedia.org/wiki/Io_uring) for asynchronous I/Os.
io_uring is a very new feature in the Linux kernel (introduced in 5.1),
and evolves rapidly with newer Linux kernel version.

In line with Boki, Halfmoon requires Linux kernel 5.10 or later to run.

### Halfmoon library ###

The `src` directory includes Halfmoon's modifications to Boki's logging layer. We introduce a new conditional logging primitive to support our protocols.

The `workers/golang` directory includes client-side wrapper codes that communicates with the logging layer and exports the log APIs to Go programs.

The Halfmoon protocols are implemented in the `workloads/workflow` in [TODO](xxx).

### Running Halfmoon's evaluation workloads ###

The [TODO](xxx) repository includes experiment scripts and detailed instructions on running evaluation workloads presented in our SOSP '23 paper.

### Limitations of the current prototype ###

The Halfmoon protocols are only exported to functions written in Go.

### License ###

Halfmoon is licensed under Apache License 2.0, in accordance with Boki.
