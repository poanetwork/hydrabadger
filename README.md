## Hydrabadger

An experimental peer-to-peer client using the [Honey Badger Byzantine Fault
Tolerant consensus algorithm](https://github.com/poanetwork/hbbft).

### Usage

#### Running a test peer

1. `git clone https://github.com/c0gent/hydrabadger`
2. `cd hydrabadger`
3. `./run-node 0`

#### Additional peers

1. Open a new terminal window.
2. `cd {...}/hydrabadger`
3. `./run-node 1`
4. (Repeat 1 and 2), `./run-node 2`, `./run-node 3`, `./run-node 4`
    * Note: If your terminal has tabs, open multiple tabs and use
      ctrl-pgup/pgdown to cycle between tabs quickly.

Each peer will generate a number of random transactions at regular intervals,
process them accordingly, and output complete batches. If your terminal is
spammed with batch outputs, consensus is working.

Type `./run-node 0 --help` or `cargo run -- --help` for command line options.

See the
[`run-node`](https://github.com/c0gent/hydrabadger/blob/master/run-node)
script for additional optional environment variables that can be set.

### Current State

Network initialization node addition, transaction generation, consensus,
and batch outputs are all generally working. Batch outputs for each epoch are
printed to the log.

Overall the client is fragile and doesn't handle deviation from simple usage
very well yet.

#### Unimplemented

* **Observer Nodes:** still in a state of flux (not working at the time of
  writing).
* **Additional Nodes:** Only the first 5 nodes are properly handled (as
  validators). Variable and dynamic nodes are coming soon.
* **Error handling** is atrocious, most errors are simply printed to the log.
* **Many edge cases and exceptions:** disconnects, reconnects, etc.
  * If too many nodes disconnect, the consensus process halts for all nodes
    (as expected). No means of reconnection or node removal is yet in place.
* **Much, much more...**

#### Other Issues

* `InvalidAckMessage` returned from `SyncKeyGen::handle_ack` seems to cause
  `Node {..} received multiple Readys from {..}` messages which *may* be
  causing occasional halting. Causes unclear.
* `BatchDeserializationFailed` errors are common and appear to halt consensus.
  New issue creation pending investigation [FIXED?].


### License

[![License: LGPL v3.0](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)

This project is licensed under the GNU Lesser General Public License v3.0. See the [LICENSE](LICENSE) file for details.