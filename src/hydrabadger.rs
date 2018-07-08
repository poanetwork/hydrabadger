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
    collections::{BTreeSet, HashSet, HashMap, VecDeque},
    fmt::Debug,
    marker::{Send, Sync},
    net::{SocketAddr},
    rc::Rc,
    io::Cursor,
};
use futures::{
    sync::mpsc,
    future::{self, Either},
    StartSend, AsyncSink,
};
use tokio::{
    self,
    io,
    reactor::{Reactor, Handle},
    net::{TcpListener, TcpStream},
    timer::Interval,
    executor::{Executor, DefaultExecutor},
    prelude::*,
};
use tokio_codec::Decoder;
use tokio_io::codec::length_delimited::Framed;
use bytes::{BytesMut, Bytes, BufMut, IntoBuf, Buf};
use rand::{self, Rng};
use uuid::{self, Uuid};
use byteorder::{self, ByteOrder, LittleEndian};
use serde::{Serializer, Deserializer, Serialize, Deserialize};
use serde_bytes;
use bincode::{self, serialize_into, deserialize_from, serialize, deserialize};
use tokio_serde_bincode::{ReadBincode, WriteBincode};



use hbbft::{
    broadcast::{Broadcast, BroadcastMessage},
    crypto::{
        SecretKeySet,
        poly::Poly,
    },
    messaging::{DistAlgorithm, NetworkInfo, SourcedMessage, Target},
    proto::message::BroadcastProto,
    honey_badger::HoneyBadger,
    dynamic_honey_badger::{DynamicHoneyBadger, Input, Batch, Message, Change},
};
// use network::{comms_task, connection, messaging::Messaging};


#[derive(Debug, Fail)]
pub enum Error {
	#[fail(display = "{}", _0)]
	Io(std::io::Error),
	// #[fail(display = "{}", _0)]
 //    CommsError(comms_task::Error),
    #[fail(display = "{}", _0)]
    Serde(bincode::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}


/// Messages sent over the network between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireMessage {
    Hello,
    Goodbye,
    Message,
    #[serde(with = "serde_bytes")]
    Bytes(Bytes),
}


/// Transmit half of the message channel.
type Tx = mpsc::UnboundedSender<WireMessage>;

/// Receive half of the message channel.
type Rx = mpsc::UnboundedReceiver<WireMessage>;

type PeerTxs = Arc<RwLock<HashMap<SocketAddr, Tx>>>;

/// A serialized message with a sender and the timestamp of arrival.
#[derive(Eq, PartialEq, Debug)]
struct TimestampedMessage {
    time: Duration,
    sender_id: Uuid,
    target: Target<Uuid>,
    message: Vec<u8>,
}


/// A stream/sink of `WireMessage`s connected to a socket.
#[derive(Debug)]
struct WireMessages {
    framed: Framed<TcpStream>,
}

impl WireMessages {
    pub fn new(socket: TcpStream) -> WireMessages {
        WireMessages {
            framed: Framed::new(socket),
        }
    }

    pub fn socket(&self) -> &TcpStream {
        self.framed.get_ref()
    }

    pub fn send_msg(&mut self, msg: WireMessage) -> Result<(), Error> {
        self.start_send(msg)?;
        let _ = self.poll_complete()?;
        Ok(())
    }
}

impl Stream for WireMessages {
    type Item = WireMessage;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.framed.poll()) {
            Some(frame) => {
                Ok(Async::Ready(Some(deserialize_from(frame.into_buf().reader()).map_err(Error::Serde)?)))
            }
            None => Ok(Async::Ready(None))
        }
    }
}

impl Sink for WireMessages {
    type SinkItem = WireMessage;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        // TODO: Reuse buffer:
        let mut serialized = BytesMut::new();
        match serialize(&item) {
            Ok(s) => serialized.extend_from_slice(&s),
            Err(err) => return Err(Error::Io(io::Error::new(io::ErrorKind::Other, err))),
        }
        match self.framed.start_send(serialized) {
            Ok(async_sink) => match async_sink {
                AsyncSink::Ready => Ok(AsyncSink::Ready),
                AsyncSink::NotReady(_) => Ok(AsyncSink::NotReady(item)),
            },
            Err(err) => Err(Error::Io(err))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.framed.poll_complete().map_err(Error::from)
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.framed.close().map_err(Error::from)
    }
}




/// The state for each connected client.
struct Peer {
    // /// Name of the peer.
    // name: BytesMut,

    // The incoming stream of messages:
    wire_messages: WireMessages,

    /// Handle to the shared message state.
    txs: PeerTxs,

    /// Receive half of the message channel.
    rx: Rx,
    /// Client socket address.
    addr: SocketAddr,
}

impl Peer {
    /// Create a new instance of `Peer`.
    fn new(txs: PeerTxs, wire_messages: WireMessages) -> Peer {
        // Get the client socket address
        let addr = wire_messages.socket().peer_addr().unwrap();

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded();

        // Add an entry for this `Peer` in the shared state map.
        let guard = txs.write().unwrap().insert(addr, tx);

        Peer {
            // name,
            wire_messages,
            txs,
            rx,
            addr,
        }
    }

    /// Sends a message to all connected peers.
    fn send_to_all(&mut self, msg: &WireMessage) {
        // Now, send the message to all other peers
        for (addr, tx) in self.txs.read().unwrap().iter() {
            // Don't send the message to ourselves
            if *addr != self.addr {
                // The send only fails if the rx half has been dropped,
                // however this is impossible as the `tx` half will be
                // removed from the map before the `rx` is dropped.
                tx.unbounded_send(msg.clone()).unwrap();
            }
        }
    }
}

/// This is where a connected client is managed.
///
/// A `Peer` is also a future representing completely processing the client.
///
/// While processing, the peer future implementation will:
///
/// 1) Receive messages on its message channel and write them to the socket.
/// 2) Receive messages from the socket and handle them appropriately.
///
impl Future for Peer {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        // Ensure the loop can't hog the thread for too long:
        const MESSAGES_PER_TICK: usize = 10;

        // Receive all messages from peers.
        for i in 0..MESSAGES_PER_TICK {
            // Polling an `UnboundedReceiver` cannot fail, so `unwrap` here is
            // safe.
            match self.rx.poll().unwrap() {
                Async::Ready(Some(v)) => {
                    // Buffer the message. Once all messages are buffered, they will
                    // be flushed to the socket (right below).
                    self.wire_messages.start_send(v)?;

                    // If this is the last iteration, the loop will break even
                    // though there could still be messages to read. Because we did
                    // not reach `Async::NotReady`, we have to notify ourselves
                    // in order to tell the executor to schedule the task again.
                    if i + 1 == MESSAGES_PER_TICK {
                        task::current().notify();
                    }
                }
                _ => break,
            }
        }

        // Flush the write buffer to the socket
        let _ = self.wire_messages.poll_complete()?;

        // Read new messages from the socket
        while let Async::Ready(message) = self.wire_messages.poll()? {
            info!("Received message: {:?}", message);

            if let Some(msg) = message {
                match msg {
                    WireMessage::Hello => info!("HELLO RECEIVED from '{}'", self.addr),
                    _ => (),
                }
            } else {
                // EOF was reached. The remote client has disconnected. There is
                // nothing more to do.
                return Ok(Async::Ready(()));
            }
        }

        // As always, it is important to not just return `NotReady` without
        // ensuring an inner future also returned `NotReady`.
        //
        // We know we got a `NotReady` from either `self.rx` or `self.wire_messages`, so
        // the contract is respected.
        Ok(Async::NotReady)
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.txs.write().unwrap().remove(&self.addr);
    }
}


/// Return a future to manage the socket.
pub fn handle_incoming(socket: TcpStream, peer_txs: PeerTxs) -> impl Future<Item = (), Error = ()> {
    let peer_addr = socket.peer_addr().unwrap();
    info!("Incoming connection from '{}'", peer_addr);

    let wire_messages = WireMessages::new(socket);

    wire_messages.into_future()
        .map_err(|(e, _)| e)
        .and_then(move |(message, wire_messages)| {
            let message = match message {
                Some(message) => message,
                None => {
                    // The remote client closed the connection without sending
                    // any data.
                    info!("Closing connection to '{}'", peer_addr);
                    return Either::A(future::ok(()));
                }
            };

            info!("Connected to '{}'", peer_addr);

            // Create the peer.
            //
            // This is also a future that processes the connection, only
            // completing when the socket closes.
            let peer = Peer::new(peer_txs, wire_messages);

            // Wrap `peer` with `Either::B` to make the return type fit.
            Either::B(peer)
        })
        .map_err(|e| {
            error!("Connection error = {:?}", e);
        })
}


// // Used to create a secret key from a UUID.
// fn sum_into_u64(bytes: &[u8]) -> u64 {
//     let mut id_u64s = vec![0; 2];
//     LittleEndian::read_u64_into(bytes, &mut id_u64s);
//     id_u64s.iter().sum()
// }


pub struct Hydrabadger/*<T, N>*/ {
	/// Incoming connection socket.
    addr: SocketAddr,
    value: Option<Vec<u8>>,
    peer_txs: PeerTxs,

    /// Honey badger.
    dhb: DynamicHoneyBadger<Vec<u8>, Uuid>,

    /// Incoming messages from other nodes that this node has not yet handled, with timestamps.
    in_queue: VecDeque<TimestampedMessage>,
    /// Outgoing messages to other nodes, with timestamps.
    out_queue: VecDeque<TimestampedMessage>,
}

impl Hydrabadger {
    /// Returns a new Hydrabadger node.
    pub fn new(addr: SocketAddr, value: Option<Vec<u8>>) -> Self {
        // let node_count_good = node_count_total - node_count_faulty;
        // let txns = (0..txn_count).map(|_| Transaction::new(txn_bytes));
        let sk_set = SecretKeySet::random(0, &mut rand::thread_rng());
        let pk_set = sk_set.public_keys();
        let id = Uuid::new_v4();
        let mut all_ids = BTreeSet::new();
        all_ids.insert(id);
        let sk_share = 0;

        let netinfo = NetworkInfo::new(
            id,
            all_ids,
            sk_set.secret_key_share(sk_share),
            pk_set.clone(),
        );

        let dhb = DynamicHoneyBadger::builder(netinfo)
            .batch_size(50)
            .max_future_epochs(0)
            .build().expect("Error creating `DynamicHoneyBadger`");

        Hydrabadger {
            addr,
            value,
            peer_txs: Arc::new(RwLock::new(HashMap::new())),
            dhb,
            in_queue: VecDeque::new(),
            out_queue: VecDeque::new(),
        }
    }

    /// Starts the server.
    pub fn run(&self, remotes: HashSet<SocketAddr>) {
        let socket = TcpListener::bind(&self.addr).unwrap();
        info!("Listening on: {}", self.addr);

        let peer_txs = self.peer_txs.clone();

        let listen = socket.incoming()
            .map_err(|e| error!("failed to accept socket; error = {:?}", e))
            .for_each(move |socket| {
                let peer_addr = socket.peer_addr().unwrap();
                info!("Incoming connection from '{}'", peer_addr);

                let wire_messages = WireMessages::new(socket);
                tokio::spawn(Peer::new(peer_txs.clone(), wire_messages)
                    .map_err(|e| {
                        error!("Connection error = {:?}", e);
                    })
                );

                Ok(())
            });

        let peer_txs = self.peer_txs.clone();
        let connect = future::lazy(move || {
            for remote_addr in remotes.iter() {
                let peer_txs = peer_txs.clone();
                tokio::spawn(TcpStream::connect(remote_addr)
                    .map_err(Error::from)
                    .and_then(move |socket| {
                        // Wrap the socket with the frame delimiter and codec:
                        let mut wire_messages = WireMessages::new(socket);

                        match wire_messages.send_msg(WireMessage::Hello) {
                            Ok(_) => Either::A(Peer::new(peer_txs, wire_messages)),
                            Err(err) => Either::B(future::err(err)),
                        }
                    })
                    .map_err(|err| error!("Socket connection error: {:?}", err)));
            }
            Ok(())
        });

        let peer_txs = self.peer_txs.clone();
        let list = Interval::new(Instant::now(), Duration::from_millis(3000))
            .for_each(move |_| {
                let peer_txs = peer_txs.read().unwrap();
                info!("Peer list:");

                for (peer_addr, mut pb) in peer_txs.iter() {
                    info!("     peer_addr: {}", peer_addr);
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
}
