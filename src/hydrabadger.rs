//! A hydrabadger consensus node.
//!
//! Code heavily borrowed from: https://github.com/poanetwork/hbbft/blob/master/examples/network/node.rs
//!

#![allow(unused_imports, dead_code, unused_variables)]


use std::{
    mem,
    time::{Duration, Instant},
    sync::{Arc},
    {self, iter, process, thread, time},
    collections::{BTreeSet, HashSet, HashMap, VecDeque},
    fmt::Debug,
    marker::{Send, Sync},
    net::{SocketAddr},
    rc::Rc,
    io::Cursor,
};
use crossbeam;
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
use rand::{self, Rng, Rand};
use uuid::{self, Uuid};
use byteorder::{self, ByteOrder, LittleEndian};
use serde::{Serializer, Deserializer, Serialize, Deserialize};
use serde_bytes;
use bincode::{self, serialize_into, deserialize_from, serialize, deserialize};
use tokio_serde_bincode::{ReadBincode, WriteBincode};
use parking_lot::{RwLock, Mutex};
use clear_on_drop::ClearOnDrop;
use hbbft::{
    broadcast::{Broadcast, BroadcastMessage},
    crypto::{
        poly::{Poly, Commitment},
        SecretKeySet, PublicKey, PublicKeySet, SecretKey
    },
    messaging::{DistAlgorithm, NetworkInfo, SourcedMessage, Target, TargetedMessage},
    proto::message::BroadcastProto,
    dynamic_honey_badger::Message,
    queueing_honey_badger::{Error as QhbError, QueueingHoneyBadger, Input, Batch, Change},
    // dynamic_honey_badger::{Error as DhbError, DynamicHoneyBadger, Input, Batch, Change, Message},
};

const BATCH_SIZE: usize = 150;
const TXN_BYTES: usize = 10;
const NEW_TXNS_PER_INTERVAL: usize = 20;


#[derive(Debug, Fail)]
pub enum Error {
	#[fail(display = "Io error: {}", _0)]
	Io(std::io::Error),
    #[fail(display = "Serde error: {}", _0)]
    Serde(bincode::Error),
    #[fail(display = "Error polling hydrabadger internal receiver")]
    HydrabadgerPoll,
    // FIXME: Make honeybadger error thread safe.
    #[fail(display = "QueuingHoneyBadger propose error")]
    QhbPropose,
    #[fail(display = "DynamicHoneyBadger error")]
    Dhb // FIXME: ^^^^ DhbError
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
    RequestChangeAdd(Uuid, PublicKey),
    Goodbye,
    #[serde(with = "serde_bytes")]
    Bytes(Bytes),
    Message(Message<Uuid>),
    // TargetedMessage(TargetedMessage<Uuid>),
    // Transaction()
}


/// Messages sent over the network between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireMessage {
    kind: WireMessageKind,
}

impl WireMessage {
    /// Returns a `RequestChangeAdd` variant.
    pub fn request_change_add(uid: Uuid, pub_key: PublicKey) -> WireMessage {
        WireMessage { kind: WireMessageKind::RequestChangeAdd(uid, pub_key), }
    }

    /// Returns a `Message` variant.
    pub fn message(msg: Message<Uuid>) -> WireMessage {
        WireMessage { kind: WireMessageKind::Message(msg), }
    }

    /// Returns the wire message kind.
    pub fn kind(&self) -> &WireMessageKind {
        &self.kind
    }

    /// Consumes this `WireMessage` into its kind.
    pub fn into_kind(self) -> WireMessageKind {
        self.kind
    }
}


/// A message between internal threads/tasks.
#[derive(Clone, Debug)]
pub enum InternalMessageKind {
    Wire(WireMessage),
    // NewTransaction(Transaction),
    NewTransactions(Vec<Transaction>),
    IncomingHbMessage(Message<Uuid>),
    Input(Input<Vec<Transaction>, Uuid>),
}


/// A message between internal threads/tasks.
#[derive(Clone, Debug)]
pub struct InternalMessage {
    src_uid: Uuid,
    kind: InternalMessageKind,
}

impl InternalMessage {
    pub fn new(src_uid: Uuid, kind: InternalMessageKind) -> InternalMessage {
        InternalMessage { src_uid, kind, }
    }

    pub fn wire(src_uid: Uuid, wire_message: WireMessage) -> InternalMessage {
        InternalMessage::new(src_uid, InternalMessageKind::Wire(wire_message))
    }

    pub fn new_transactions(src_uid: Uuid, txns: Vec<Transaction>) -> InternalMessage {
        InternalMessage::new(src_uid, InternalMessageKind::NewTransactions(txns))
    }

    pub fn incoming_hb_message(src_uid: Uuid, msg: Message<Uuid>) -> InternalMessage {
        InternalMessage::new(src_uid, InternalMessageKind::IncomingHbMessage(msg))
    }

    pub fn input(src_uid: Uuid, input: Input<Vec<Transaction>, Uuid>) -> InternalMessage {
        InternalMessage::new(src_uid, InternalMessageKind::Input(input))
    }

    /// Returns the source unique identifier.
    pub fn src_uid(&self) -> &Uuid {
        &self.src_uid
    }

    /// Returns the internal message kind.
    pub fn kind(&self) -> &InternalMessageKind {
        &self.kind
    }

    /// Consumes this `InternalMessage` into its parts.
    pub fn into_parts(self) -> (Uuid, InternalMessageKind) {
        (self.src_uid, self.kind)
    }
}


/// Transmit half of the wire message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type WireTx = mpsc::UnboundedSender<WireMessage>;

/// Receive half of the wire message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type WireRx = mpsc::UnboundedReceiver<WireMessage>;

/// Transmit half of the internal message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type InternalTx = mpsc::UnboundedSender<InternalMessage>;

/// Receive half of the internal message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type InternalRx = mpsc::UnboundedReceiver<InternalMessage>;


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
    hb: Arc<HydrabadgerInner>,

    /// Receive half of the message channel.
    rx: WireRx,

    // /// Channel to `Hydrabadger`.
    // internal_tx: InternalTx,

    /// Peer socket address.
    addr: SocketAddr,
}

impl Peer {
    /// Create a new instance of `Peer`.
    fn new(uid: Uuid, hb: Arc<HydrabadgerInner>, wire_messages: WireMessages) -> Peer {
        // Get the client socket address
        let addr = wire_messages.socket().peer_addr().unwrap();
        // let uid = Uuid::new_v4();

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded();

        // Add an entry for this `Peer` in the shared state map.
        let guard = hb.peer_txs.write().insert(uid, tx);

        Peer {
            uid,
            wire_messages,
            hb,
            rx,
            // internal_tx,
            addr,
        }
    }

    /// Sends a message to all connected peers.
    fn wire_to_all(&mut self, msg: &WireMessage) {
        // Now, send the message to all other peers
        for (uid, tx) in self.hb.peer_txs.read().iter() {
            // Don't send the message to ourselves
            if *uid != self.uid {
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

                    // Exceeded max messages per tick, schedule notification:
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
            trace!("Received message: {:?}", message);

            if let Some(msg) = message {
                match msg.into_kind() {
                    WireMessageKind::RequestChangeAdd(_uid, _pub_key) => error!("Duplicate `WireMessage::RequestChangeAdd` \
                        received from '{}'", self.uid),
                    WireMessageKind::Message(msg) => {
                        self.hb.peer_internal_tx.unbounded_send(InternalMessage::incoming_hb_message(
                            self.uid, msg)).unwrap();
                    },
                    _ => unimplemented!(),
                }
            } else {
                // EOF was reached. The remote client has disconnected. There is
                // nothing more to do.
                info!("Peer ('{}') disconnected.", self.uid);
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
        self.hb.peer_txs.write().remove(&self.uid);
        // let hb = self.hb.clone();

        // // FIXME: Consider simply sending the 'change' input through the
        // // internal channel.
        // self.hb.qhb.write().input(Input::Change(Change::Remove(self.uid)))
        //     .expect("Error adding new peer to HB");

        self.hb.peer_internal_tx.unbounded_send(InternalMessage::input(self.uid,
            Input::Change(Change::Remove(self.uid)))).unwrap();
    }
}


struct Keys {
    sk: ClearOnDrop<Box<SecretKey>>,
    pk_set: PublicKeySet,
}


enum StateDiscriminant {
    Disconnected,
    ConnectionPending,
    ConnectedObserver,
    ConnectedValidator,
}


// The current hydrabadger state.
enum State {
    Disconnected {

    },
    ConnectionPending {
        // qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uuid>>,
    },
    ConnectedObserver {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uuid>>,
        // sk: SecretKey,
        // pk_set: PublicKeySet,
    },
    ConnectedValidator {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uuid>>,
        // sk: SecretKey,
        // pk_set: PublicKeySet,
    },
}

impl State {
    /// Returns the state discriminant.
    fn discriminant(&self) -> StateDiscriminant {
        match self {
            State::Disconnected { .. } => StateDiscriminant::Disconnected,
            State::ConnectionPending { .. } => StateDiscriminant::ConnectionPending,
            State::ConnectedObserver { .. } => StateDiscriminant::ConnectedObserver,
            State::ConnectedValidator { .. } => StateDiscriminant::ConnectedValidator,
        }
    }

    /// Returns a new `State::Disconnected`.
    fn disconnected() -> State {
        // let sk = SecretKey::rand(&mut rand::thread_rng());
        // let pk = sk.public_key();
        State::Disconnected { /*sk*/ }
    }

    /// Changes the variant (in-place) of this `State` to `ConnectionPending`.
    //
    // TODO: Add proper error handling:
    fn set_connection_pending(&mut self) {
        *self = match *self {
            State::Disconnected { .. } => State::ConnectionPending { },
            _ => panic!("Must be disconnected before connecting."),
        };
    }

    /// Changes the variant (in-place) of this `State` to `ConnectedObserver`.
    //
    // TODO: Add proper error handling:
    fn set_connected_observer(&mut self, qhb: QueueingHoneyBadger<Vec<Transaction>, Uuid>) {
        *self = match *self {
            State::ConnectionPending { .. } => State::ConnectedObserver { qhb: Some(qhb) },
            _ => panic!("Must be connection-pending before becoming connected-observer."),
        };
    }

    /// Changes the variant (in-place) of this `State` to `ConnectedObserver`.
    //
    // TODO: Add proper error handling:
    fn set_connected_validator(&mut self) {
        *self = match *self {
            State::ConnectedObserver { ref mut qhb } =>
                State::ConnectedValidator { qhb: qhb.take() },
            _ => panic!("Must be connection-pending before becoming connected-observer."),
        };
    }

    /// Returns a reference to the internal HB instance.
    fn qhb(&self) -> Option<&QueueingHoneyBadger<Vec<Transaction>, Uuid>> {
        match self {
            State::ConnectedObserver { ref qhb, .. } => qhb.as_ref(),
            State::ConnectedValidator { ref qhb, .. } => qhb.as_ref(),
            _ => None,
        }
    }

    /// Returns a reference to the internal HB instance.
    fn qhb_mut(&mut self) -> Option<&mut QueueingHoneyBadger<Vec<Transaction>, Uuid>> {
        match self {
            State::ConnectedObserver { ref mut qhb, .. } => qhb.as_mut(),
            State::ConnectedValidator { ref mut qhb, .. } => qhb.as_mut(),
            _ => None,
        }
    }
}


/// The `Arc`-wrapped portion of `Hydrabadger`.
struct HydrabadgerInner {
    /// Node uid:
    uid: Uuid,
    /// Incoming connection socket.
    addr: SocketAddr,

    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_txs: RwLock<HashMap<Uuid, WireTx>>,

    // /// Honey badger.
    // qhb: RwLock<QueueingHoneyBadger<Vec<Transaction>, Uuid>>,

    /// The current state containing HB when connected.
    state: RwLock<State>,

    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_tx: InternalTx,

    peer_out_queue: RwLock<VecDeque<TargetedMessage<Message<usize>, usize>>>,
    batch_out_queue: RwLock<VecDeque<Batch<Transaction, usize>>>,

    unhandled_inputs: RwLock<VecDeque<()>>,

    secret_key: Mutex<Option<ClearOnDrop<Box<SecretKey>>>>,
}


/// The core API for creation of and event handling for a `HoneyBadger` instance.
pub struct Hydrabadger {
    inner: Arc<HydrabadgerInner>,
    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_rx: InternalRx,
}

impl Hydrabadger {
    /// Returns a new Hydrabadger node.
    pub fn new(addr: SocketAddr, _value: Option<Vec<u8>>) -> Self {
        let uid = Uuid::new_v4();
        // // TODO: This needs to be updated based on network size:
        // let sk_threshold = 0;
        // let sk_set = SecretKeySet::random(sk_threshold, &mut rand::thread_rng());
        // // let sk = SecretKey::rand( &mut rand::thread_rng());
        // let sk = sk_set.secret_key_share(0 as u64);
        // let pk_set = sk_set.public_keys();

        // let node_ids: BTreeSet<_> = iter::once(uid).collect();

        // let netinfo = NetworkInfo::new(
        //     uid,
        //     node_ids.clone(),
        //     sk_set.secret_key_share(0 as u64),
        //     pk_set.clone(),
        // );

        // let _qhb = RwLock::new(QueueingHoneyBadger::builder(netinfo)
        //     // Default: 100:
        //     .batch_size(BATCH_SIZE)
        //     // Default: 3:
        //     .max_future_epochs(3)
        //     .build());
        let secret_key = Mutex::new(Some(ClearOnDrop::new(Box::new(SecretKey::rand(
            &mut rand::thread_rng())))));

        let (peer_internal_tx, peer_internal_rx) = mpsc::unbounded();

        Hydrabadger {
            inner: Arc::new(HydrabadgerInner {
                uid,
                addr,
                peer_txs: RwLock::new(HashMap::new()),
                state: RwLock::new(State::disconnected()),
                peer_internal_tx,
                peer_out_queue: RwLock::new(VecDeque::new()),
                batch_out_queue: RwLock::new(VecDeque::new()),
                unhandled_inputs: RwLock::new(VecDeque::new()),
                secret_key,
            }),
            peer_internal_rx,
        }
    }

    // fn add_peer_node(&mut self) {
    //     let mut state = self.inner.state.write();
    //     let mut qhb = match state.qhb_mut();
    // }


    /// Creates a new hydrabadger instance.
    fn gen_hb_instance(&self, peer_uid: Uuid, peer_pub_key: PublicKey)
            -> QueueingHoneyBadger<Vec<Transaction>, Uuid> {

        let node_ids: BTreeSet<_> = iter::once(self.inner.uid).collect();
        let node_ids = [self.inner.uid, peer_uid].iter().cloned().collect();
        // TODO: If this is `None`, make a new secret key.
        let sk = self.inner.secret_key.lock().take().unwrap();
        let pk_set = Commitment { coeff: vec![sk.public_key().0, peer_pub_key.0] }.into();

        let netinfo = NetworkInfo::new(
            self.inner.uid,
            node_ids,
            sk,
            pk_set,
        );

        QueueingHoneyBadger::builder(netinfo)
            // Default: 100:
            .batch_size(BATCH_SIZE)
            // Default: 3:
            .max_future_epochs(3)
            .build()
    }

    fn handle_internal_message(&mut self, i_msg: InternalMessage) {
        let (src_uid, w_msg) = i_msg.into_parts();
        let epoch = 0; // ?
        let mut state = self.inner.state.write();

        match w_msg {
            InternalMessageKind::Wire(w_msg) => match w_msg.into_kind() {
                WireMessageKind::RequestChangeAdd(src_uid, src_pub_key) => {
                    match state.discriminant() {
                        StateDiscriminant::Disconnected => {
                            panic!("Received `WireMessageKind::RequestChangeAdd` when disconnected.");
                        },
                        StateDiscriminant::ConnectionPending => {
                            let qhb = self.gen_hb_instance(src_uid, src_pub_key);
                            *state = State::ConnectedValidator { qhb: Some(qhb) };

                            // Reply to node with info...


                        },
                        StateDiscriminant::ConnectedObserver | StateDiscriminant::ConnectedValidator => {
                            let qhb = state.qhb_mut().unwrap();
                            info!("Change-Adding ('{}') to honey badger.", src_uid);
                            qhb.input(Input::Change(Change::Add(src_uid, src_pub_key)))
                                .expect("Error adding new peer to HB");
                        },
                    }
                },
                _ => {},
            },
            InternalMessageKind::NewTransactions(txns) => {
                // info!("Pushing {} new user transactions to queue.", txns.len());
                // qhb.input(Input::User(txns))
                //     .expect("Error inputing transactions into `HoneyBadger`");

                // // Turn the HB crank:
                // if qhb.queue().len() >= BATCH_SIZE {
                //     qhb.propose().unwrap();
                // }
            },
            InternalMessageKind::IncomingHbMessage(msg) => {
                // info!("Handling incoming message: {:?}", msg);
                // qhb.handle_message(&src_uid, msg).unwrap();
            },
            InternalMessageKind::Input(input) => {
                // qhb.input(input)
                //     .expect("Error inputting to HB");
            }
        }
    }
}

impl Future for Hydrabadger {
    type Item = ();
    type Error = Error;

    /// Polls the internal message receiver until all txs are dropped.
    fn poll(&mut self) -> Poll<(), Error> {
        // Ensure the loop can't hog the thread for too long:
        const MESSAGES_PER_TICK: usize = 50;

         // Handle incoming internal messages:
        for i in 0..MESSAGES_PER_TICK {
            match self.peer_internal_rx.poll() {
                Ok(Async::Ready(Some(i_msg))) => {
                    self.handle_internal_message(i_msg);

                    // Exceeded max messages per tick, schedule notification:
                    if i + 1 == MESSAGES_PER_TICK {
                        task::current().notify();
                    }
                },
                Ok(Async::Ready(None)) => {
                    // The sending ends have all dropped.
                    return Ok(Async::Ready(()));
                },
                Ok(Async::NotReady) => {},
                Err(()) => return Err(Error::HydrabadgerPoll),
            };
        }

        // Forward outgoing messages:
        let peer_txs = self.inner.peer_txs.read();
        let mut state = self.inner.state.write();
        for (i, hb_msg) in state.qhb_mut().unwrap().message_iter().enumerate() {
            info!("Forwarding message: {:?}", hb_msg);
            match hb_msg.target {
                Target::Node(n_uid) => {
                    peer_txs.get(&n_uid).unwrap().unbounded_send(
                        WireMessage::message(hb_msg.message)).unwrap();
                },
                Target::All => {
                    for (n_uid, tx) in peer_txs.iter().filter(|(&uid, _)| uid != self.inner.uid) {
                        tx.unbounded_send(WireMessage::message(hb_msg.message.clone())).unwrap();
                    }
                },
            }

            // Exceeded max messages per tick, schedule notification:
            if i + 1 == MESSAGES_PER_TICK {
                task::current().notify();
            }
        }

        // Check for batch outputs:
        for output in state.qhb_mut().unwrap().output_iter() {
            // self.batch_out_queue.push_back(txn);
            info!("BATCH OUTPUT: {:?}", output);
        }

        Ok(Async::NotReady)
    }
}


/// Returns a future that handles incoming connections on `socket`.
fn process_incoming(socket: TcpStream, hb: Arc<HydrabadgerInner>)
        -> impl Future<Item = (), Error = ()> {
    info!("Incoming connection from '{}'", socket.peer_addr().unwrap());
    let wire_messages = WireMessages::new(socket);

    wire_messages.into_future()
        .map_err(|(e, _)| e)
        .and_then(move |(msg_opt, w_messages)| {
            let hb = hb.clone();
            // let peer_internal_tx = hb.peer_internal_tx.clone();

            match msg_opt {
                Some(msg) => match msg.into_kind() {
                    WireMessageKind::RequestChangeAdd(uid, pub_key) => {
                        if let StateDiscriminant::Disconnected = hb.state.read().discriminant() {
                            // Set this so that the HB will be created:
                            hb.state.write().set_connection_pending();
                        }

                        let peer = Peer::new(uid, hb, w_messages);

                        // Relay request_change_add message (this could be
                        // simplified into a new msg variant).
                        peer.hb.peer_internal_tx.unbounded_send(
                                InternalMessage::wire(peer.uid,
                                    WireMessage::request_change_add(uid, pub_key)))
                            .map_err(|err| error!("Unable to send on internal tx. \
                                Internal rx has dropped: {}", err))
                            .unwrap();

                        Either::B(peer)
                    },
                    _ => {
                        error!("Peer connected without sending \
                            `WireMessageKind::RequestChangeAdd`.");
                        Either::A(future::ok(()))
                    },
                },
                None => {
                    // The remote client closed the connection without sending
                    // a request_change_add message.
                    Either::A(future::ok(()))
                },
            }
        })
        .map_err(|err| error!("Connection error = {:?}", err))
}


/// Binds to a host address and returns a future which starts the node.
pub fn node(hydrabadger: Hydrabadger, remotes: HashSet<SocketAddr>)
        -> impl Future<Item = (), Error = ()> {
    let socket = TcpListener::bind(&hydrabadger.inner.addr).unwrap();
    info!("Listening on: {}", hydrabadger.inner.addr);

    let hb = hydrabadger.inner.clone();
    let listen = socket.incoming()
        .map_err(|err| error!("Error accepting socket: {:?}", err))
        .for_each(move |socket| {
            tokio::spawn(process_incoming(socket, hb.clone()));
            Ok(())
        });

    let pk = hydrabadger.inner.secret_key.lock().as_ref().unwrap().public_key();
    let uid = hydrabadger.inner.uid.clone();
    let hb = hydrabadger.inner.clone();
    let connect = future::lazy(move || {
        for remote_addr in remotes.iter() {
            let pk = pk.clone();
            let hb = hb.clone();
            tokio::spawn(TcpStream::connect(remote_addr)
                .map_err(Error::from)
                .and_then(move |socket| {
                    // Wrap the socket with the frame delimiter and codec:
                    let mut wire_messages = WireMessages::new(socket);

                    match wire_messages.send_msg(WireMessage::request_change_add(uid, pk.clone())) {
                        Ok(_) => {
                            // Set our state appropriately:
                            hb.state.write().set_connection_pending();

                            // let uid = hb.uid;

                            Either::A(Peer::new(uid, hb, wire_messages))
                        },
                        Err(err) => Either::B(future::err(err)),
                    }
                })
                .map_err(|err| error!("Socket connection error: {:?}", err)));
        }
        Ok(())
    });

    let hb = hydrabadger.inner.clone();
    let generate_txns = Interval::new(Instant::now(), Duration::from_millis(3000))
        .for_each(move |_| {
            let hb = hb.clone();
            let peer_txs = hb.peer_txs.read();
            trace!("Peer list:");
            for (peer_addr, mut pb) in peer_txs.iter() {
                trace!("     peer_addr: {}", peer_addr); }

            if let Some(qhb) = &hb.state.read().qhb() {
                // If no nodes are connected, ignore new transactions:
                // if qhb.dyn_hb().netinfo().num_nodes() > 1 {
                    info!("Generating and inputting {} random transactions...", NEW_TXNS_PER_INTERVAL);
                    // Send some random transactions to our internal HB instance.
                    let txns: Vec<_> = (0..NEW_TXNS_PER_INTERVAL).map(|_| Transaction::random(TXN_BYTES)).collect();
                    hb.peer_internal_tx.unbounded_send(
                        InternalMessage::new_transactions(hb.uid, txns)
                    ).unwrap();
                // } else {
                //     error!("No nodes connected. Panicking...");
                //     panic!("Invalid state");
                // }
            } else {
                info!("Not connected.");
            }

            Ok(())
        })
        .map_err(|err| error!("List connection inverval error: {:?}", err));

    let hydrabadger = hydrabadger.map_err(|err| error!("Hydrabadger internal error: {:?}", err));

    listen.join4(connect, generate_txns, hydrabadger).map(|(_, _, _, _)| ())
}


/// Starts a node.
pub fn run_node(hb: Hydrabadger, remotes: HashSet<SocketAddr>) {
    tokio::run(node(hb, remotes));
}