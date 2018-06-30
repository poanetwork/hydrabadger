//! A hydrabadger consensus node.
//!
//! Code heavily borrowed from: https://github.com/poanetwork/hbbft/blob/master/examples/network/node.rs
//!


use crossbeam;
use std::collections::{BTreeSet, HashSet};
use std::fmt::Debug;
use std::marker::{Send, Sync};
use std::net::SocketAddr;
use std::rc::Rc;
use std::{io, iter, process, thread, time};

use hbbft::broadcast::{Broadcast, BroadcastMessage};
use hbbft::crypto::poly::Poly;
use hbbft::crypto::SecretKeySet;
use hbbft::messaging::{DistAlgorithm, NetworkInfo, SourcedMessage};
use hbbft::proto::message::BroadcastProto;
use network::comms_task;
use network::connection;
use network::messaging::Messaging;


#[derive(Debug, Fail)]
pub enum Error {
	#[fail(display = "{}", _0)]
	IoError(io::Error),
	#[fail(display = "{}", _0)]
    CommsError(comms_task::Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<comms_task::Error> for Error {
    fn from(err: comms_task::Error) -> Error {
        Error::CommsError(err)
    }
}


pub struct Node<T> {
	/// Incoming connection socket.
    addr: SocketAddr,
    /// Sockets of remote nodes.
    remotes: HashSet<SocketAddr>,
    /// Optionally, a value to be broadcast by this node.
    value: Option<T>,
}


impl<T> Node<T>
        where T: Clone + Debug + AsRef<[u8]> + PartialEq +
                 Send + Sync + From<Vec<u8>> + Into<Vec<u8>> {
    /// Consensus node constructor. It only initialises initial parameters.
    pub fn new(addr: SocketAddr, remotes: HashSet<SocketAddr>, value: Option<T>) -> Self {
        Node {
            addr,
            remotes,
            value,
        }
    }

    /// Consensus node procedure implementing HoneyBadgerBFT.
    pub fn run(&self) -> Result<T, Error> {
        let value = &self.value;
        let (our_str, connections) = connection::make(&self.addr, &self.remotes);
        let mut node_strs: Vec<String> = iter::once(our_str.clone())
            .chain(connections.iter().map(|c| c.node_str.clone()))
            .collect();
        node_strs.sort();
        let our_id = node_strs.binary_search(&our_str).unwrap();
        let all_ids: BTreeSet<_> = (0..node_strs.len()).collect();

        // FIXME: This example doesn't call algorithms that use cryptography. However the keys are
        // required by the interface to all algorithms in Honey Badger. Therefore we set placeholder
        // keys here. A fully-featured application would need to take appropriately initialized keys
        // from elsewhere.
        let secret_key_set = SecretKeySet::from(Poly::zero());
        let secret_key = secret_key_set.secret_key_share(our_id as u64);
        let public_key_set = secret_key_set.public_keys();

        let netinfo = NetworkInfo::new(our_id, all_ids.clone(), secret_key, public_key_set);

        if value.is_some() != (our_id == 0) {
            panic!("Exactly the first node must propose a value.");
        }

        // Initialise the message delivery system and obtain TX and RX handles.
        let messaging: Messaging<BroadcastMessage> = Messaging::new(all_ids.len());
        let rxs_to_comms = messaging.rxs_to_comms();
        let tx_from_comms = messaging.tx_from_comms();
        let rx_to_algo = messaging.rx_to_algo();
        let tx_from_algo = messaging.tx_from_algo();
        let stop_tx = messaging.stop_tx();

        // All spawned threads will have exited by the end of the scope.
        crossbeam::scope(|scope| {
            // Start the centralised message delivery system.
            let _msg_handle = messaging.spawn(scope);

            // Associate a broadcast instance with this node. This instance will
            // broadcast the proposed value. There is no remote node
            // corresponding to this instance, and no dedicated comms task. The
            // node index is 0.
            let broadcast_handle = scope.spawn(move || {
                let mut broadcast =
                    Broadcast::new(Rc::new(netinfo), 0).expect("failed to instantiate broadcast");

                if let Some(v) = value {
                    broadcast.input(v.clone().into()).expect("propose value");
                    for msg in broadcast.message_iter() {
                        tx_from_algo.send(msg);
                    }
                }

                loop {
                    // Receive a message from the socket IO task.
                    let message = rx_to_algo.recv().expect("receive from algo");
                    let SourcedMessage { source: i, message } = message;
                    debug!("{} received from {}: {:?}", our_id, i, message);
                    broadcast
                        .handle_message(&i, message)
                        .expect("handle broadcast message");
                    for msg in broadcast.message_iter() {
                        debug!("{} sending to {:?}: {:?}", our_id, msg.target, msg.message);
                        tx_from_algo.send(msg);
                    }
                    if let Some(output) = broadcast.next_output() {
                        println!(
                            "Broadcast succeeded! Node {} output: {}",
                            our_id,
                            String::from_utf8(output).unwrap()
                        );
                        break;
                    }
                }
            });

            // Start a comms task for each connection. Node indices of those
            // tasks are 1 through N where N is the number of connections.
            for (i, c) in connections.iter().enumerate() {
                // Receive side of a single-consumer channel from algorithm
                // actor tasks to the comms task.
                let node_index = if c.node_str < our_str { i } else { i + 1 };
                let rx_to_comms = &rxs_to_comms[node_index];

                scope.spawn(move || {
                    match comms_task::CommsTask::<BroadcastProto, BroadcastMessage>::new(
                        tx_from_comms,
                        rx_to_comms,
                        // FIXME: handle error
                        c.stream.try_clone().unwrap(),
                        node_index,
                    ).run()
                    {
                        Ok(_) => debug!("Comms task {} succeeded", node_index),
                        Err(e) => error!("Comms task {}: {:?}", node_index, e),
                    }
                });
            }

            // Wait for the broadcast instances to finish before stopping the
            // messaging task.
            broadcast_handle.join();

            // Wait another second so that pending messages get sent out.
            thread::sleep(time::Duration::from_secs(1));

            // Stop the messaging task.
            stop_tx.send(());

            process::exit(0);
        }) // end of thread scope
    }
}
