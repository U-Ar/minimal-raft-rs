# minimal-raft-rs
Experimental &amp; minimal raft key-value store implementation tested on jepsen/maelstrom


## Result of maelstrom test on the current version

`tests/maelstrom/maelstrom test -w lin-kv --bin target/release/raft --node-count 3 --concurrency 4n --rate 30 --time-limit 60 --nemesis partition --nemesis-interval 10`

![maelstrom latency](/resources/latency-raft-3.png)