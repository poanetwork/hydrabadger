//! The local message delivery system.
use crossbeam::{Scope, ScopedJoinHandle};
// use crossbeam_channel;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use hbbft::messaging::{SourcedMessage, Target, TargetedMessage};


#[derive(Clone, Debug, Fail)]
pub enum Error {
    #[fail(display = "Invalid messaging target: '{}'", _0)]
    NoSuchTarget(usize),
    // SendError,
}

// impl<T> From<crossbeam_channel::SendError<T>> for Error {
//     fn from(_: crossbeam_channel::SendError<T>) -> Error {
//         Error::SendError
//     }
// }


/// The queue functionality for messages sent between algorithm instances.
/// The messaging struct allows for targeted message exchange between comms
/// tasks on one side and algo tasks on the other.
pub struct Messaging<M> {
    /// Transmit sides of message channels to comms threads.
    txs_to_comms: Vec<Sender<M>>,
    /// Receive side of the routed message channel from comms threads.
    rx_from_comms: Receiver<SourcedMessage<M, usize>>,
    /// Transmit sides of message channels to algo thread.
    tx_to_algo: Sender<SourcedMessage<M, usize>>,
    /// Receive side of the routed message channel from comms threads.
    rx_from_algo: Receiver<TargetedMessage<M, usize>>,

    /// RX handles to be used by comms tasks.
    rxs_to_comms: Vec<Receiver<M>>,
    /// TX handle to be used by comms tasks.
    tx_from_comms: Sender<SourcedMessage<M, usize>>,
    /// RX handles to be used by algo task.
    rx_to_algo: Receiver<SourcedMessage<M, usize>>,
    /// TX handle to be used by algo task.
    tx_from_algo: Sender<TargetedMessage<M, usize>>,

    /// Control channel used to stop the listening thread.
    stop_tx: Sender<()>,
    stop_rx: Receiver<()>,
}

impl<M: Send> Messaging<M> {
    /// Initialises all the required TX and RX handles for the case on a total
    /// number `num_nodes` of consensus nodes.
    pub fn new(num_nodes: usize) -> Self {
        let to_comms: Vec<_> = (0..num_nodes).map(|_| unbounded::<M>()).collect();
        let txs_to_comms = to_comms.iter().map(|&(ref tx, _)| tx.to_owned()).collect();
        let rxs_to_comms: Vec<Receiver<M>> =
            to_comms.iter().map(|&(_, ref rx)| rx.to_owned()).collect();
        let (tx_from_comms, rx_from_comms) = unbounded();

        let (tx_to_algo, rx_to_algo) = unbounded();
        let (tx_from_algo, rx_from_algo) = unbounded();

        let (stop_tx, stop_rx) = bounded(1);

        Messaging {
            // internally used handles
            txs_to_comms,
            rx_from_comms,
            tx_to_algo,
            rx_from_algo,

            // externally used handles
            rxs_to_comms,
            tx_from_comms,
            rx_to_algo,
            tx_from_algo,

            stop_tx,
            stop_rx,
        }
    }

    pub fn rxs_to_comms(&self) -> &Vec<Receiver<M>> {
        &self.rxs_to_comms
    }

    pub fn tx_from_comms(&self) -> &Sender<SourcedMessage<M, usize>> {
        &self.tx_from_comms
    }

    pub fn rx_to_algo(&self) -> &Receiver<SourcedMessage<M, usize>> {
        &self.rx_to_algo
    }

    pub fn tx_from_algo(&self) -> &Sender<TargetedMessage<M, usize>> {
        &self.tx_from_algo
    }

    /// Gives the ownership of the handle to stop the message receive loop.
    pub fn stop_tx(&self) -> Sender<()> {
        self.stop_tx.to_owned()
    }

    /// Spawns the message delivery thread in a given thread scope.
    pub fn spawn<'a>(&self, scope: &Scope<'a>) -> ScopedJoinHandle<Result<(), Error>>
    where
        M: Clone + 'a,
    {
        let txs_to_comms = self.txs_to_comms.to_owned();
        let rx_from_comms = self.rx_from_comms.to_owned();
        let tx_to_algo = self.tx_to_algo.to_owned();
        let rx_from_algo = self.rx_from_algo.to_owned();

        let stop_rx = self.stop_rx.to_owned();
        let mut stop = false;

        // TODO: `select_loop!` seems to really confuse Clippy.
        #[cfg_attr(
            feature = "cargo-clippy",
            allow(never_loop, if_let_redundant_pattern_matching, deref_addrof)
        )]
        scope.spawn(move || {
            let mut result = Ok(());
            // This loop forwards messages according to their metadata.
            while !stop && result.is_ok() {
                select! {
                    recv(rx_from_algo, tm_opt) => {
                        match tm_opt {
                            Some(tm) => match tm.target {
                                Target::All => {
                                    // Send the message to all remote nodes.
                                    for tx in txs_to_comms.iter() {
                                        tx.send(tm.message.clone())
                                    }
                                },
                                Target::Node(i) => {
                                    // if i < txs_to_comms.len() {
                                    //     txs_to_comms[i].send(tm.message);
                                    // } else {
                                    //     result = Err(Error::NoSuchTarget);
                                    // }
                                    match txs_to_comms.get(i) {
                                        Some(tx) => tx.send(tm.message),
                                        None => { result = Err(Error::NoSuchTarget(i)); }
                                    }
                                }
                            }
                            _ => (),
                        }
                    },
                    recv(rx_from_comms, message) => {
                        // Send the message to all algorithm instances, stopping at
                        // the first error.
                        if let Some(msg) = message {
                            tx_to_algo.send(msg)
                        }
                    },
                    recv(stop_rx, _) => {
                        // Flag the thread ready to exit.
                        stop = true;
                    }
                }
            } // end of select_loop!
            result
        })
    }
}
