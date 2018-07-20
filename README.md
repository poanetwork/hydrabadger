## Hydrabadger

An experimental peer-to-peer client using the [Honey Badger Byzantine Fault
Tolerant consensus algorithm](https://github.com/poanetwork/hbbft).

### Usage

#### Running a test peer

1. `git clone https://github.com/c0gent/hydrabadger`
2. `cd hydrabadger`
3. `./peer0`

#### Additional peers

1. Open a new terminal window.
2. `cd {...}/hydrabadger`
3. `./peer1`

4. (Repeat 1 and 2), `./peer2`, `./peer3`, `./peer4`

Each peer will generate a number of random transactions at regular intervals,
process them accordingly, and output complete batches.

Type `cargo run [--release] -- --help` (or `target/peer_node --help` if
already built) for command line options (more coming soon!).


### Current State

Network initialization (with 5 nodes only), transaction generation, consensus,
and batch outputs are all generally working. Batch outputs for each epoch are
printed to the log.

Overall the client is fragile and doesn't handle deviation from simple usage
very well yet.

#### Unimplemented

* **Command-Line/Config Options:** Variable and/or random (within a range) batch
  size, transaction size (bytes), transaction generation count, transaction
  generation interval, minimum peer count, much more.
* **Observer Nodes:** still in a state of flux (not working at the time of
  writing).
* **Additional Nodes:** Only the first 5 nodes are properly handled (as
  validators). Variable and dynamic nodes are coming soon.
* **Many edge cases and exceptions:** disconnects, reconnects, etc.
  * If too many nodes disconnect, the consensus process halts for all nodes
    (as expected). No means of reconnection or node removal is yet in place.
* **Much, much more...**

#### Other Issues

* `InvalidAckMessage` returned from `SyncKeyGen::handle_ack` seems to cause
  `Node {..} received multiple Readys from {..}` messages which *may* be
  causing occasional halting. Causes unclear.
* `BatchDeserializationFailed` errors are common and appear to halt consensus.
  New issue creation pending investigation.
