## Hydrabadger

An experimental peer-to-peer client using the [Honey Badger Byzantine Fault
Tolerant consensus algorithm](https://github.com/poanetwork/hbbft).

### Usage

#### Running a test peer

1. `git clone https://github.com/c0gent/hydrabadger`
2. `cd hydrabadger`
3. `./peer0`

#### Additional local peers

1. Open a new terminal window.
2. `cd {...}/hydrabadger`
3. `./peer1`

4. Repeat 1 and 2
5. `./peer2`


Each peer will generate a number of random transactions at regular intervals,
process them accordingly, and output complete batches.