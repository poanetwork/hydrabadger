//! A hydrabadger consensus node.
//!
//! Code heavily borrowed from: https://github.com/poanetwork/hbbft/blob/master/examples/network/node.rs
//!

#![allow(unused_imports, dead_code, unused_variables)]


use crossbeam;
use std::{
    time::{Duration, Instant},
    sync::{Arc, RwLock},
    {self, iter, process, thread, time},
    collections::{BTreeSet, HashSet, HashMap},
    fmt::Debug,
    marker::{Send, Sync},
    net::{SocketAddr},
    rc::Rc,
};
use futures::future;
use tokio::{
    self, io,
    reactor::{Reactor, Handle},
    net::{TcpListener, TcpStream},
    timer::Interval,
    prelude::*,
};
use hbbft::{
    broadcast::{Broadcast, BroadcastMessage},
    crypto::{
        SecretKeySet,
        poly::Poly,
    },
    messaging::{DistAlgorithm, NetworkInfo, SourcedMessage},
    proto::message::BroadcastProto,
    honey_badger::HoneyBadger,
};
use network::{comms_task, connection, messaging::Messaging};


#[derive(Debug, Fail)]
pub enum Error {
	#[fail(display = "{}", _0)]
	IoError(std::io::Error),
	#[fail(display = "{}", _0)]
    CommsError(comms_task::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<comms_task::Error> for Error {
    fn from(err: comms_task::Error) -> Error {
        Error::CommsError(err)
    }
}



pub struct Hydrabadger/*<T, N>*/ {
	/// Incoming connection socket.
    addr: SocketAddr,
    /// Sockets of remote nodes.
    remotes: HashSet<SocketAddr>,
    // /// Honey badger.
    // hb: HoneyBadger<T, N>,
    value: Option<Vec<u8>>,
    connections_in: Arc<RwLock<HashMap<SocketAddr, TcpStream>>>,
    connections_out: Arc<RwLock<HashMap<SocketAddr, TcpStream>>>,
}

impl Hydrabadger {
    /// Returns a new Hydrabadger node.
    pub fn new(addr: SocketAddr, remotes: HashSet<SocketAddr>, value: Option<Vec<u8>>) -> Self {
        let connections_in = Arc::new(RwLock::new(HashMap::new()));
        let connections_out = Arc::new(RwLock::new(HashMap::new()));

        Hydrabadger {
            addr,
            remotes,
            value,
            connections_in,
            connections_out,
        }
    }

    pub fn run(&self) {
        let socket = TcpListener::bind(&self.addr).unwrap();
        info!("Listening on: {}", self.addr);

        let connections_in = self.connections_in.clone();
        let listen = socket.incoming()
            .map_err(|e| error!("failed to accept socket; error = {:?}", e))
            .for_each(move |mut socket| {
                let connections_in = connections_in.clone();

                // let (reader, writer) = socket.split();
                // let amt = io::copy(reader, writer);

                // // After our copy operation is complete we just print out some helpful
                // // information.
                // let msg = amt.then(move |result| {
                //     match result {
                //         Ok((amt, _, _)) => println!("wrote {} bytes", amt),
                //         Err(e) => println!("error: {}", e),
                //     }

                //     Ok(())
                // });

                tokio::spawn(future::lazy(move || {
                    let peer_addr = socket.peer_addr().unwrap();
                    let local_addr = socket.local_addr().unwrap();
                    info!("Connection made with: [local_addr: {}, peer_addr: {}]",
                        local_addr, peer_addr);
                    let msg = b"Yo";
                    socket.write_all(msg)
                        .unwrap_or_else(|err| error!("Socket write error: {:?}", err));
                    info!("    '{:?}' written", msg);
                    connections_in.write().unwrap().insert(peer_addr, socket);
                    Ok(())
                }))
            });

        let remotes = self.remotes.clone();
        let connections_out = self.connections_out.clone();
        let connect = future::lazy(move || {
            for remote_addr in remotes.iter() {
                let connections_out = connections_out.clone();
                tokio::spawn(TcpStream::connect(remote_addr)
                    .and_then(move |socket| {
                        connections_out.write().unwrap().insert(socket.peer_addr().unwrap(), socket);
                        Ok(())
                    })
                    .map_err(|err| error!("Socket connection error: {:?}", err)));
            }
            Ok(())
        });

        let connections_in = self.connections_in.clone();
        let list = Interval::new(Instant::now(), Duration::from_millis(3000))
            .for_each(move |_| {
                let mut remove_list = HashSet::new();
                {
                    let connections_in = connections_in.read().unwrap();
                    info!("Incoming connection list:");

                    for (_, mut socket) in connections_in.iter() {
                        let peer_addr = socket.peer_addr().unwrap();
                        info!("     peer_addr: {}", peer_addr);

                        let mut buf = Vec::new();
                        match socket.read_to_end(&mut buf) {
                            Ok(bytes_read) => {
                                info!("        bytes_read: {}", bytes_read);
                                info!("        message: {:?}", buf);
                            },
                            Err(err) => {
                                error!("        Read error: {:?}", err);
                                remove_list.insert(peer_addr);
                            },
                        }
                    }
                }

                if remove_list.len() > 0 {
                    let mut cns = connections_in.write().unwrap();
                    for addr in remove_list {
                        cns.remove(&addr);
                    }
                }

                Ok(())
            })
            .map_err(|err| {
                error!("List connection inverval error: {:?}", err);
            });

        tokio::run(listen.join3(connect, list).map(|(_, _, _)| ()));

    }

    pub fn connect(&self) {

    }

    pub fn run_old(&self) {
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
            panic!("The first node must propose a value.");
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


mod codec {
    use std::io;
    use bytes::{BufMut, BytesMut};
    use tokio_codec::{Encoder, Decoder};

    /// A simple `Codec` implementation that just ships bytes around.
    ///
    /// This type is used for "framing" a TCP/UDP stream of bytes but it's really
    /// just a convenient method for us to work with streams/sinks for now.
    /// This'll just take any data read and interpret it as a "frame" and
    /// conversely just shove data into the output location without looking at
    /// it.
    pub struct Bytes;

    impl Decoder for Bytes {
        type Item = BytesMut;
        type Error = io::Error;

        fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<BytesMut>> {
            if buf.len() > 0 {
                let len = buf.len();
                Ok(Some(buf.split_to(len)))
            } else {
                Ok(None)
            }
        }
    }

    impl Encoder for Bytes {
        type Item = Vec<u8>;
        type Error = io::Error;

        fn encode(&mut self, data: Vec<u8>, buf: &mut BytesMut) -> io::Result<()> {
            buf.put(&data[..]);
            Ok(())
        }
    }
}