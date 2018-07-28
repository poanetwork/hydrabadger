# Hydrabadger

An experimental peer-to-peer client using the [Honey Badger Byzantine Fault
Tolerant consensus algorithm](https://github.com/poanetwork/hbbft).

## Usage

### Running a test peer

1. `git clone https://github.com/poanetwork/hydrabadger`
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
[`run-node`](https://github.com/poanetwork/hydrabadger/blob/master/run-node)
script for additional optional environment variables that can be set. To turn
on debug log output: `export HYDRABADGER_LOG_ADDTL=debug` and/or `echo "export
HYDRABADGER_LOG_ADDTL=debug" >> ~/.profile`.

### Current State

Network initialization node addition, transaction generation, consensus,
and batch outputs are all generally working. Batch outputs for each epoch are
printed to the log.

Overall the client is fragile and doesn't handle deviation from simple usage
very well yet.

### Unimplemented

* **Many edge cases and exceptions:** disconnects, reconnects, etc.
  * Connecting to a network which is in the process of key generation causes
    the entire network to fail. For now, wait until the network starts
    outputting batches before connecting additional peer nodes.
* **Error handling** is atrocious, most errors are simply printed to the log.
* **Usage as a library** is still a work in progress as the API settles.
* **Much, much more...**

### License

[![License: LGPL v3.0](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)

This project is licensed under the GNU Lesser General Public License v3.0. See the [LICENSE](LICENSE) file for details.