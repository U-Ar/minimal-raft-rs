# minimal-raft-rs
Experimental &amp; minimal raft key-value store implementation tested on jepsen/maelstrom


## Result of maelstrom test on the current version

`tests/maelstrom/maelstrom test -w lin-kv --bin target/release/raft --node-count 3 --concurrency 4n --rate 30 --time-limit 60 --nemesis partition --nemesis-interval 10`

![maelstrom latency](/resources/latency-raft-3.png)


## How to run maelstrom tests

following workloads are supported:

- echo
- broadcast 
- g-set
- pn-counter
- txn-list-append
- lin-kv

example usage:

```bash
cargo build
tests/maelstrom/maelstrom test -w echo --bin target/debug/echo --nodes n1 --time-limit 10
tests/maelstrom/maelstrom test -w broadcast --bin target/debug/broadcast --time-limit 20 --rate 100
tests/maelstrom/maelstrom test -w g-set --bin target/debug/g-set --time-limit 20 --rate 10
tests/maelstrom/maelstrom test -w pn-counter --bin target/debug/g-counter --time-limit 30 --rate 10 --nemesis partition
tests/maelstrom/maelstrom test -w txn-list-append --bin target/debug/datomic --time-limit 10 --node-count 2 --rate 100
tests/maelstrom/maelstrom test -w lin-kv --bin target/debug/raft --node-count 3 --concurrency 4n --rate 30 --time-limit 60 --nemesis partition --nemesis-interval 10
```