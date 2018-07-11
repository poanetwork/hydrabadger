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
    StartSend, AsyncSink,
    sync::mpsc,
    future::{self, Either},
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
    crypto::{SecretKeySet, poly::Poly},
    messaging::{DistAlgorithm, NetworkInfo, SourcedMessage, Target, TargetedMessage},
    proto::message::BroadcastProto,
    honey_badger::HoneyBadger,
    dynamic_honey_badger::{/*DynamicHoneyBadger,*/ /*Input, Batch,*/ Message, /*Change*/},
    queueing_honey_badger::{QueueingHoneyBadger, Input, Batch, /*Message,*/ Change},
};


#[derive(Debug, Fail)]
pub enum Error {
	#[fail(display = "{}", _0)]
	Io(std::io::Error),
    #[fail(display = "{}", _0)]
    Serde(bincode::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}


/// A transaction.
#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd, Debug, Clone)]
pub struct Transaction(pub Vec<u8>);

impl Transaction {
    fn random(len: usize) -> Transaction {
        Transaction(rand::thread_rng().gen_iter().take(len).collect())
    }
}


/// Messages sent over the network between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireMessageKind {
    Hello,
    Goodbye,
    // Message,
    #[serde(with = "serde_bytes")]
    Bytes(Bytes),
    Message(Message<Uuid>),
    // Transaction()
}


/// Messages sent over the network between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireMessage {
    // src_uid: Uuid,
    kind: WireMessageKind,
}

impl WireMessage {
    pub fn hello(/*src_uid: Uuid*/) -> WireMessage {
        WireMessage {
            // src_uid,
            kind: WireMessageKind::Hello,
        }
    }

    // pub fn src_uid(&self) -> &Uuid {
    //     &self.src_uid
    // }

    pub fn kind(&self) -> &WireMessageKind {
        &self.kind
    }
}


/// A message between internal threads/tasks.
#[derive(Clone, Debug)]
pub enum InternalMessageKind {
    Wire(WireMessage),
}


/// A message between internal threads/tasks.
#[derive(Clone, Debug)]
pub struct InternalMessage {
    src_uid: Uuid,
    kind: InternalMessageKind,
}

impl InternalMessage {
    pub fn wire(src_uid: Uuid, wire_message: WireMessage) -> InternalMessage {
        InternalMessage {
            src_uid,
            kind: InternalMessageKind::Wire(wire_message),
        }
    }

    pub fn src_uid(&self) -> &Uuid {
        &self.src_uid
    }

    pub fn kind(&self) -> &InternalMessageKind {
        &self.kind
    }
}


/// Transmit half of the message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type WireTx = mpsc::UnboundedSender<WireMessage>;

/// Receive half of the message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type WireRx = mpsc::UnboundedReceiver<WireMessage>;

/// Transmit half of the message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type InternalTx = mpsc::UnboundedSender<InternalMessage>;

/// Receive half of the message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type InternalRx = mpsc::UnboundedReceiver<InternalMessage>;

// type PeerTxs = Arc<RwLock<HashMap<SocketAddr, Tx>>>;

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
    // Peer uid.
    uid: Uuid,

    // The incoming stream of messages:
    wire_messages: WireMessages,

    /// Handle to the shared message state.
    // txs: PeerTxs,
    hb: Arc<Hydrabadger>,

    /// Receive half of the message channel.
    rx: WireRx,

    peer_internal_tx: InternalTx,

    /// Peer socket address.
    addr: SocketAddr,
}

impl Peer {
    /// Create a new instance of `Peer`.
    fn new(hb: Arc<Hydrabadger>, wire_messages: WireMessages,
            peer_internal_tx: InternalTx) -> Peer {
        // Get the client socket address
        let addr = wire_messages.socket().peer_addr().unwrap();

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded();

        // Add an entry for this `Peer` in the shared state map.
        let guard = hb.peer_txs.write().unwrap().insert(addr, tx);

        Peer {
            uid: Uuid::new_v4(),
            wire_messages,
            hb,
            rx,
            peer_internal_tx,
            addr,
        }
    }

    /// Sends a message to all connected peers.
    fn send_to_all(&mut self, msg: &WireMessage) {
        // Now, send the message to all other peers
        for (addr, tx) in self.hb.peer_txs.read().unwrap().iter() {
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
                match msg.kind() {
                    WireMessageKind::Hello => info!("HELLO RECEIVED from '{}'", self.uid),
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
        self.hb.peer_txs.write().unwrap().remove(&self.addr);
    }
}



struct HoneyBadgerTask {

}



pub struct Hydrabadger {
    /// Node uid:
    uid: Uuid,
	/// Incoming connection socket.
    addr: SocketAddr,
    // value: Option<Vec<u8>>,

    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_txs: RwLock<HashMap<SocketAddr, WireTx>>,

    /// Honey badger.
    dhb: RwLock<QueueingHoneyBadger<Transaction, Uuid>>,

    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_tx: InternalTx,
    peer_internal_rx: InternalRx,


    peer_out_queue: RwLock<VecDeque<TargetedMessage<Message<usize>, usize>>>,
    batch_out_queue: RwLock<VecDeque<Batch<Transaction, usize>>>,
}

impl Hydrabadger {
    /// Returns a new Hydrabadger node.
    pub fn new(addr: SocketAddr, _value: Option<Vec<u8>>) -> Self {
        // let node_count_good = node_count_total - node_count_faulty;
        // let txns = (0..txn_count).map(|_| Transaction::new(txn_bytes));
        let sk_set = SecretKeySet::random(0, &mut rand::thread_rng());
        let pk_set = sk_set.public_keys();
        let uid = Uuid::new_v4();
        // let mut all_ids = BTreeSet::new();
        // all_ids.insert(id);
        // let sk_share = 0;

        let node_ids: BTreeSet<_> = iter::once(uid).collect();

        let netinfo = NetworkInfo::new(
            uid,
            node_ids.clone(),
            sk_set.secret_key_share(0 as u64),
            pk_set.clone(),
        );

        let dhb = RwLock::new(QueueingHoneyBadger::builder(netinfo)
            .batch_size(50)
            .max_future_epochs(0)
            .build());
            /*.build().expect("Error creating `QueueingHoneyBadger`");*/

        let (peer_internal_tx, peer_internal_rx) = mpsc::unbounded();

        Hydrabadger {
            uid,
            addr,
            // value,
            peer_txs: RwLock::new(HashMap::new()),
            dhb,

            // peer_in_queue: RwLock::new(VecDeque::new()),
            peer_internal_tx,
            peer_internal_rx,

            peer_out_queue: RwLock::new(VecDeque::new()),
            batch_out_queue: RwLock::new(VecDeque::new()),
        }
    }

    pub fn connect(&self) {

    }
}


/// Binds to a host address and returns a future which starts the node.
pub fn node(hb: Hydrabadger, remotes: HashSet<SocketAddr>)
        -> impl Future<Item = (), Error = ()> {
    let socket = TcpListener::bind(&hb.addr).unwrap();
    info!("Listening on: {}", hb.addr);

    // let peer_txs = hb.peer_txs.clone();
    let hydrabadger = Arc::new(hb);

    let hb = hydrabadger.clone();
    let listen = socket.incoming()
        .map_err(|e| error!("failed to accept socket; error = {:?}", e))
        .for_each(move |socket| {
            let peer_addr = socket.peer_addr().unwrap();
            info!("Incoming connection from '{}'", peer_addr);

            let wire_messages = WireMessages::new(socket);
            tokio::spawn(Peer::new(hb.clone(), wire_messages, hb.peer_internal_tx.clone())
                .map_err(|e| {
                    error!("Connection error = {:?}", e);
                })
            );

            Ok(())
        });

    // let peer_txs = hb.peer_txs.clone();
    let uid = hydrabadger.uid.clone();
    let hb = hydrabadger.clone();
    let connect = future::lazy(move || {
        for remote_addr in remotes.iter() {
            let hb = hb.clone();
            tokio::spawn(TcpStream::connect(remote_addr)
                .map_err(Error::from)
                .and_then(move |socket| {
                    // Wrap the socket with the frame delimiter and codec:
                    let mut wire_messages = WireMessages::new(socket);

                    match wire_messages.send_msg(WireMessage::hello()) {
                        Ok(_) => {
                            let peer_internal_tx = hb.peer_internal_tx.clone();
                            Either::A(Peer::new(hb, wire_messages, peer_internal_tx))
                        },
                        Err(err) => Either::B(future::err(err)),
                    }
                })
                .map_err(|err| error!("Socket connection error: {:?}", err)));
        }
        Ok(())
    });

    let hb = hydrabadger.clone();
    let list = Interval::new(Instant::now(), Duration::from_millis(3000))
        .for_each(move |_| {
            let hb = hb.clone();
            let peer_txs = hb.peer_txs.read().unwrap();
            // info!("Peer list:");

            for (peer_addr, mut pb) in peer_txs.iter() {
                info!("     peer_addr: {}", peer_addr);
            }

            // TODO: Send txns instead.

            Ok(())
        })
        .map_err(|err| {
            error!("List connection inverval error: {:?}", err);
        });

    listen.join3(connect, list).map(|(_, _, _)| ())
}


/// Starts a node.
pub fn run_node(hb: Hydrabadger, remotes: HashSet<SocketAddr>) {
    tokio::run(node(hb, remotes));
}