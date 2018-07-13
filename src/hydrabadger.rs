//! A hydrabadger consensus node.
//!

#![allow(unused_imports, dead_code, unused_variables, unused_mut)]

use std::{
    mem,
    time::{Duration, Instant},
    sync::{Arc},
    {self, iter, process, thread, time},
    collections::{BTreeSet, HashSet, HashMap, VecDeque},
    fmt::{self, Debug},
    marker::{Send, Sync},
    net::{SocketAddr},
    rc::Rc,
    io::Cursor,
    ops::Deref,
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

// The minimum number of peers needed to spawn a HB instance.
const PEER_MINIMUM_COUNT: usize = 4;



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


/// A peer's incoming (listening) address.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct InAddr(pub SocketAddr);

impl Deref for InAddr {
    type Target = SocketAddr;
    fn deref(&self) -> &SocketAddr {
        &self.0
    }
}

impl fmt::Display for InAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InAddr({})", self.0)
    }
}


/// An internal address used to respond to a connected peer.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct OutAddr(pub SocketAddr);

impl Deref for OutAddr {
    type Target = SocketAddr;
    fn deref(&self) -> &SocketAddr {
        &self.0
    }
}

impl fmt::Display for OutAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OutAddr({})", self.0)
    }
}


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



/// Nodes of the network.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkNodeInfo {
    uid: Uuid,
    in_addr: InAddr,
    pk: PublicKey,
}

/// The current state of the network.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkState {
    AwaitingMorePeers(Vec<NetworkNodeInfo>),
    Active,
}


/// Messages sent over the network between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireMessageKind {
    HelloRequestChangeAdd(Uuid, InAddr, PublicKey),
    WelcomeReceivedChangeAdd(Uuid, NetworkState),
    RequestNetworkState,
    NetworkState(NetworkState),
    Goodbye,
    #[serde(with = "serde_bytes")]
    Bytes(Bytes),
    Message(Message<Uuid>),
    // TargetedMessage(TargetedMessage<Uuid>),
}


/// Messages sent over the network between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireMessage {
    kind: WireMessageKind,
}

impl WireMessage {
    /// Returns a `HelloRequestChangeAdd` variant.
    pub fn request_change_add(uid: Uuid, in_addr: InAddr, pub_key: PublicKey) -> WireMessage {
        WireMessage { kind: WireMessageKind::HelloRequestChangeAdd(uid, in_addr, pub_key), }
    }

    /// Returns a `WelcomeReceivedChangeAdd` variant.
    pub fn received_change_add(uid: Uuid, net_state: NetworkState) -> WireMessage {
        WireMessage { kind: WireMessageKind::WelcomeReceivedChangeAdd(uid, net_state) }
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
    NewTransactions(Vec<Transaction>),
    IncomingHbMessage(Message<Uuid>),
    Input(Input<Vec<Transaction>, Uuid>),
    PeerDisconnect,
}


/// A message between internal threads/tasks.
#[derive(Clone, Debug)]
pub struct InternalMessage {
    src_uid: Uuid,
    src_addr: OutAddr,
    kind: InternalMessageKind,
}

impl InternalMessage {
    pub fn new(src_uid: Uuid, src_addr: OutAddr, kind: InternalMessageKind) -> InternalMessage {
        InternalMessage { src_uid, kind, src_addr}
    }

    pub fn wire(src_uid: Uuid, src_addr: OutAddr, wire_message: WireMessage) -> InternalMessage {
        InternalMessage::new(src_uid, src_addr, InternalMessageKind::Wire(wire_message))
    }

    pub fn new_transactions(src_uid: Uuid, src_addr: OutAddr, txns: Vec<Transaction>) -> InternalMessage {
        InternalMessage::new(src_uid, src_addr, InternalMessageKind::NewTransactions(txns))
    }

    pub fn incoming_hb_message(src_uid: Uuid, src_addr: OutAddr, msg: Message<Uuid>) -> InternalMessage {
        InternalMessage::new(src_uid, src_addr, InternalMessageKind::IncomingHbMessage(msg))
    }

    pub fn input(src_uid: Uuid, src_addr: OutAddr, input: Input<Vec<Transaction>, Uuid>) -> InternalMessage {
        InternalMessage::new(src_uid, src_addr, InternalMessageKind::Input(input))
    }

    pub fn peer_disconnect(src_uid: Uuid, src_addr: OutAddr) -> InternalMessage {
        InternalMessage::new(src_uid, src_addr, InternalMessageKind::PeerDisconnect)
    }

    /// Returns the source unique identifier this message was received in.
    pub fn src_uid(&self) -> &Uuid {
        &self.src_uid
    }

    /// Returns the source socket this message was received on.
    pub fn src_addr(&self) -> &OutAddr {
        &self.src_addr
    }

    /// Returns the internal message kind.
    pub fn kind(&self) -> &InternalMessageKind {
        &self.kind
    }

    /// Consumes this `InternalMessage` into its parts.
    pub fn into_parts(self) -> (Uuid, OutAddr, InternalMessageKind) {
        (self.src_uid, self.src_addr, self.kind)
    }
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
                Ok(Async::Ready(Some(
                    // deserialize_from(frame.reader()).map_err(Error::Serde)?
                    deserialize(&frame.freeze()).map_err(Error::Serde)?
                )))
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
    uid: Option<Uuid>,

    // The incoming stream of messages:
    wire_messages: WireMessages,

    /// Handle to the shared message state.
    // txs: PeerTxs,
    hb: Arc<HydrabadgerInner>,

    // TODO: Consider adding back a separate clone of `peer_internal_tx`. Is
    // there any difference if capacity isn't an issue?

    /// Receive half of the message channel.
    rx: WireRx,

    /// Peer socket address.
    addr: OutAddr,
}

impl Peer {
    /// Create a new instance of `Peer`.
    fn new(uid: Option<Uuid>, hb: Arc<HydrabadgerInner>, wire_messages: WireMessages) -> Peer {
        // Get the client socket address
        let addr = OutAddr(wire_messages.socket().peer_addr().unwrap());
        // let uid = Uuid::new_v4();

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded();

        // Add an entry for this `Peer` in the shared state map.
        let guard = hb.peer_txs.write().insert(addr, tx);

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
        for (p_addr, tx) in self.hb.peer_txs.read().iter() {
            // Don't send the message to ourselves
            if *p_addr != self.addr {
                // The send only fails if the rx half has been dropped,
                // however this is impossible as the `tx` half will be
                // removed from the map before the `rx` is dropped.
                tx.unbounded_send(msg.clone()).unwrap();
            }
        }
    }

    /// Sends a hello response (welcome).
    fn wire_welcome_received_change_add(&self, net_state: NetworkState) {
        self.hb.peer_txs.read().get(&self.addr).unwrap()
            .unbounded_send(WireMessage::received_change_add(self.uid.clone().unwrap(), net_state))
            .unwrap();
    }
}

/// A future representing the client connection.
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
                    WireMessageKind::HelloRequestChangeAdd(src_uid, in_addr, _pub_key) => {
                        error!("Duplicate `WireMessage::HelloRequestChangeAdd` \
                            received from '{}'", src_uid);
                    },
                    WireMessageKind::WelcomeReceivedChangeAdd(src_uid, _pub_key_set) => {
                        self.uid = Some(src_uid);
                        // FIXME: Do something with the pk_set

                    },
                    WireMessageKind::Message(msg) => {
                        let uid = self.uid.clone()
                            .expect("`WireMessageKind::Message` received before \
                                `WireMessageKind::WelcomeReceivedChangeAdd`");
                        self.hb.peer_internal_tx.unbounded_send(
                            InternalMessage::incoming_hb_message(uid, self.addr, msg)).unwrap();
                    },
                    _ => unimplemented!(),
                }
            } else {
                // EOF was reached. The remote client has disconnected. There is
                // nothing more to do.
                info!("Peer ({}: '{}') disconnected.", self.addr, self.uid.clone().unwrap());
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
        debug!("Removing peer ({}: '{}') from the list of txs.",
            self.addr, self.uid.clone().unwrap());
        // Remove peer transmitter from the list:
        self.hb.peer_txs.write().remove(&self.addr);

        // // FIXME: Consider simply sending the 'change' input through the
        // // internal channel.
        // self.hb.qhb.write().input(Input::Change(Change::Remove(self.uid)))
        //     .expect("Error adding new peer to HB");
        if let Some(uid) = self.uid.clone() {
            debug!("Sending peer ({}: '{}') disconnect internal message.",
                self.addr, self.uid.clone().unwrap());

            // self.hb.peer_internal_tx.unbounded_send(InternalMessage::input(
            //     uid, self.addr, Input::Change(Change::Remove(uid)))).unwrap();

            self.hb.peer_internal_tx.unbounded_send(InternalMessage::peer_disconnect(
                uid, self.addr)).unwrap()
        }
    }
}


/// A peer on the list of awaiting nodes.
struct AwaitingPeerInfo {
    out_addr: OutAddr,
    info: NetworkNodeInfo,
}

    ///////////////////////// BOOTSTRAP ////////////////////////
    // // First, you generate keys independently. Then:
    //
    // let (mut key_gen, proposal) = SyncKeyGen::new(id, sk, pub_keys, 0);
    // send_to_everyone(proposal);
    // let proposals = receive_everyones_proposal(); // Including our own!
    // for (sender_id, proposal) in proposals {
    //     if Some(ProposeOutcome::Valid(accept)) = key_gen.handle_propose(&sender_id, proposal) {
    //         send_to_everyone(accept);
    //     }
    // }
    // let accepts = receive_everyones_accepts(); // Including our own!
    // for (sender_id, accept) in accepts {
    //     key_gen.handle_accept(&sender_id, accept);
    // }
    // let (pub_key_set, secret_key_share) = key_gen.generate();
    ////////////////////////////////////////////////////////////


/// Returns a new hydrabadger instance.
fn gen_qhb_instance(
        local_uid: Uuid,
        local_addr: InAddr,
        secret_key: ClearOnDrop<Box<SecretKey>>,
        peers: Vec<AwaitingPeerInfo>,
        ) -> QueueingHoneyBadger<Vec<Transaction>, Uuid> {
    assert!(peers.len() >= PEER_MINIMUM_COUNT);
    let mut node_ids: BTreeSet<_> = peers.iter().map(|p_info| p_info.info.uid).collect();
    node_ids.insert(local_uid);
    let mut pks: Vec<_> = peers.iter().map(|p_info| p_info.info.pk.clone()).collect();
    pks.push(secret_key.public_key());

    // FIXME: Generate this properly (using bootstrap above):s
    let pk_set = Commitment { coeff: vec![   ] }.into();


    let netinfo = NetworkInfo::new(
        local_uid,
        node_ids,
        secret_key,
        pk_set,
    );

    QueueingHoneyBadger::builder(netinfo)
        // Default: 100:
        .batch_size(BATCH_SIZE)
        // Default: 3:
        .max_future_epochs(3)
        .build()
}


/// A `State` discriminant.
#[derive(Copy, Clone, Debug)]
enum StateDsct {
    Disconnected,
    ConnectionPending,
    ConnectedAwaitingMorePeers(usize),
    ConnectedObserver,
    ConnectedValidator,
}


// The current hydrabadger state.
enum State {
    Disconnected {
        local_uid: Uuid,
        local_addr: InAddr,
        secret_key: Option<ClearOnDrop<Box<SecretKey>>>,
    },
    ConnectionPending {
        // qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uuid>>,
        local_uid: Uuid,
        local_addr: InAddr,
        secret_key: Option<ClearOnDrop<Box<SecretKey>>>,
    },
    ConnectedAwaitingMorePeers {
        local_uid: Uuid,
        local_addr: InAddr,
        peers: Option<Vec<AwaitingPeerInfo>>,
        secret_key: Option<ClearOnDrop<Box<SecretKey>>>,
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
    fn discriminant(&self) -> StateDsct {
        match self {
            State::Disconnected { .. } => StateDsct::Disconnected,
            State::ConnectionPending { .. } => StateDsct::ConnectionPending,
            State::ConnectedAwaitingMorePeers { ref peers, .. } => {
                StateDsct::ConnectedAwaitingMorePeers(peers.as_ref().unwrap().len())
            },
            State::ConnectedObserver { .. } => StateDsct::ConnectedObserver,
            State::ConnectedValidator { .. } => StateDsct::ConnectedValidator,
        }
    }

    /// Returns a new `State::Disconnected`.
    fn disconnected(local_uid: Uuid, local_addr: InAddr,
            secret_key: ClearOnDrop<Box<SecretKey>>) -> State {
        // let sk = SecretKey::rand(&mut rand::thread_rng());
        // let pk = sk.public_key();
        State::Disconnected { local_uid, local_addr, secret_key: Some(secret_key) }
    }

    /// Changes the variant (in-place) of this `State` to `ConnectedAwaitingMorePeers`.
    fn set_connected_awaiting_more_peers(&mut self, peers: Vec<AwaitingPeerInfo>) {
        *self = match *self {
            State::Disconnected { local_uid, local_addr, ref mut secret_key } => {
                State::ConnectedAwaitingMorePeers { local_uid, local_addr,
                    secret_key: secret_key.take(), peers: Some(peers) }
            },
            _ => panic!("Must be disconnected before awaiting more peers."),
        };
    }

    /// Adds an awaiting peer and changes the network state appropriately when
    /// enough peers are added.
    fn add_awaiting_peer(&mut self, peer_info: AwaitingPeerInfo) {
        *self = match *self {
            State::ConnectedAwaitingMorePeers {
                    local_uid, local_addr, ref mut secret_key, ref mut peers } => {
                let mut prs = peers.take().expect("Peer list gone!");
                prs.push(peer_info);
                if prs.len() >= PEER_MINIMUM_COUNT {
                    info!("Peer count has reached the minimum ({}). Creating HB instance.",
                        PEER_MINIMUM_COUNT);
                    // TODO: If this is `None`, make a new secret key?.
                    let sk = secret_key.take().expect("Secret key is gone!");
                    let qhb = gen_qhb_instance(local_uid, local_addr, sk, prs);
                    State::ConnectedValidator { qhb: Some(qhb) }
                } else {
                    State::ConnectedAwaitingMorePeers {
                        local_uid, local_addr, secret_key: secret_key.take(), peers: Some(prs) }
                }

            },
            _ => panic!("Must be awaiting more peers to add a peer."),
        }
    }

    /// Adds an awaiting peer and changes the network state appropriately when
    /// enough peers are added.
    fn remove_awaiting_peer(&mut self, uid: Uuid, out_addr: OutAddr) {
        match self {
            State::ConnectedAwaitingMorePeers { ref mut peers, .. } => {
                let mut prs = peers.as_mut().expect("Peer list gone!");
                // prs.remove(peer_info);
                let start_len = prs.len();
                prs.retain(|p_info| {
                    debug_assert!((p_info.info.uid == uid) == (p_info.out_addr == out_addr));
                    p_info.info.uid != uid
                });
                debug_assert!(prs.len() == start_len - 1);

                debug!("Peer ({}: '{}') removed from await list.", out_addr, uid);

                if prs.len() == 0 {
                    // Change state back to Disconnected?
                } else {
                    // ?
                }

            },
            _ => panic!("Must be awaiting more peers to remove a peer."),
        }
    }

    /// Changes the variant (in-place) of this `State` to `ConnectionPending`.
    //
    // TODO: Add proper error handling:
    fn set_connection_pending(&mut self) {
        *self = match *self {
            State::Disconnected { local_uid, local_addr, ref mut secret_key, } => {
                State::ConnectionPending { local_uid, local_addr, secret_key: secret_key.take() }
            },
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

    fn local_public_key(&self) -> PublicKey {
        match self {
            State::Disconnected { secret_key, .. }
                    | State::ConnectionPending { secret_key, .. }
                    | State::ConnectedAwaitingMorePeers { secret_key, .. } => {
                secret_key.as_ref().expect("No secret key!").public_key()
            },
            // State::ConnectionPending { .. } => StateDsct::ConnectionPending,
            // State::ConnectedAwaitingMorePeers { .. } => StateDsct::ConnectedAwaitingMorePeers,
            State::ConnectedObserver { .. } => unimplemented!(),
            State::ConnectedValidator { .. } => unimplemented!(),
        }
    }
}


/// The `Arc`-wrapped portion of `Hydrabadger`.
///
/// Shared all over the place.
struct HydrabadgerInner {
    /// Node uid:
    uid: Uuid,
    /// Incoming connection socket.
    addr: InAddr,

    peer_txs: RwLock<HashMap<OutAddr, WireTx>>,

    peer_out_addrs: RwLock<HashMap<Uuid, OutAddr>>,

    /// The current state containing HB when connected.
    state: RwLock<State>,

    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_tx: InternalTx,

    peer_out_queue: RwLock<VecDeque<TargetedMessage<Message<usize>, usize>>>,
    batch_out_queue: RwLock<VecDeque<Batch<Transaction, usize>>>,

    unhandled_inputs: RwLock<VecDeque<()>>,
}

impl HydrabadgerInner {

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
        let secret_key = ClearOnDrop::new(Box::new(SecretKey::rand(
            &mut rand::thread_rng())));

        let (peer_internal_tx, peer_internal_rx) = mpsc::unbounded();

        Hydrabadger {
            inner: Arc::new(HydrabadgerInner {
                uid,
                addr: InAddr(addr),
                peer_txs: RwLock::new(HashMap::new()),
                peer_out_addrs: RwLock::new(HashMap::new()),
                state: RwLock::new(State::disconnected(uid, InAddr(addr), secret_key)),
                peer_internal_tx,
                peer_out_queue: RwLock::new(VecDeque::new()),
                batch_out_queue: RwLock::new(VecDeque::new()),
                unhandled_inputs: RwLock::new(VecDeque::new()),
            }),
            peer_internal_rx,
        }
    }

    /// Returns this node's local key.
    pub fn local_public_key(&self) -> PublicKey {
        self.inner.state.read().local_public_key()
    }

    fn handle_internal_message(&mut self, i_msg: InternalMessage) {
        let (src_uid, src_addr, w_msg) = i_msg.into_parts();
        // let epoch = 0; // ?

        trace!("Hydrabadger::handle_internal_message: Locking state for writing...");
        let mut state = self.inner.state.write();
        trace!("Hydrabadger::handle_internal_message: State locked for writing.");

        match w_msg {
            InternalMessageKind::Wire(w_msg) => match w_msg.into_kind() {
                WireMessageKind::HelloRequestChangeAdd(src_uid, in_addr, src_pub_key) => {
                    match state.discriminant() {
                        StateDsct::Disconnected => {
                            panic!("Received `WireMessageKind::HelloRequestChangeAdd` while disconnected.");
                        },
                        StateDsct::ConnectionPending => {
                            panic!("Received `WireMessageKind::HelloRequestChangeAdd` while \
                                `StateDsct::ConnectionPending`.");
                        },
                        StateDsct::ConnectedAwaitingMorePeers(_peer_count) => {
                            // Assume this is handled already.
                            debug_assert!({
                                // TODO: Check to make sure peer is in the list
                                true
                            });
                        },
                        StateDsct::ConnectedObserver | StateDsct::ConnectedValidator => {
                            let qhb = state.qhb_mut().unwrap();
                            info!("Change-Adding ('{}') to honey badger.", src_uid);
                            qhb.input(Input::Change(Change::Add(src_uid, src_pub_key)))
                                .expect("Error adding new peer to HB");
                        },
                    }
                },
                WireMessageKind::WelcomeReceivedChangeAdd( .. ) => {
                    // TODO: Handle
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

            InternalMessageKind::PeerDisconnect => {
                match state.discriminant() {
                    StateDsct::Disconnected => {
                        panic!("Received `WireMessageKind::PeerDisconnect` while disconnected.");
                    },
                    StateDsct::ConnectionPending => {
                        unimplemented!();
                    },
                    StateDsct::ConnectedAwaitingMorePeers(_peer_count) => {
                        debug!("Removing peer ({}: '{}') from await list.", src_addr, src_uid);
                        state.remove_awaiting_peer(src_uid, src_addr);
                    },
                    StateDsct::ConnectedObserver | StateDsct::ConnectedValidator => {
                        unimplemented!();
                    },
                }
            }
        }

        trace!("Hydrabadger::handle_internal_message: State unlocked for writing.");
    }

    fn handle_hb_message(&mut self) {
        trace!("Hydrabadger::handle_hb_message: Locking state for writing...");
        let mut state = self.inner.state.write();
        trace!("Hydrabadger::handle_hb_message: State locked for writing.");

    }

    fn handle_output(&mut self) {
        trace!("Hydrabadger::handle_output: Locking state for writing...");
        let mut state = self.inner.state.write();
        trace!("Hydrabadger::handle_output: State locked for writing.");

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
        trace!("Hydrabadger::poll: Locking state for writing...");
        let mut state = self.inner.state.write();
        trace!("Hydrabadger::poll: State locked for writing.");

        if let Some(qhb) = state.qhb_mut() {
            let peer_txs = self.inner.peer_txs.read();
            let peer_out_addrs = self.inner.peer_out_addrs.read();

            for (i, hb_msg) in qhb.message_iter().enumerate() {
                info!("Forwarding message: {:?}", hb_msg);
                match hb_msg.target {
                    Target::Node(p_uid) => {
                        let p_addr = peer_out_addrs.get(&p_uid)
                            .expect("Hydrabadger::poll: No peer address inserted.");
                        peer_txs.get(&p_addr).unwrap().unbounded_send(
                            WireMessage::message(hb_msg.message)).unwrap();
                    },
                    Target::All => {
                        for (p_addr, tx) in peer_txs.iter()
                                .filter(|(&p_addr, _)| p_addr != OutAddr(*self.inner.addr)) {
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
            for output in qhb.output_iter() {
                // self.batch_out_queue.push_back(txn);
                info!("BATCH OUTPUT: {:?}", output);
            }
        }

        trace!("Hydrabadger::poll: State unlocked for writing.");
        Ok(Async::NotReady)
    }
}


/// Handles a `WireMessageKind::HelloRequestChangeAdd`.
///
/// If the current state is `StateDsct::Disconnected`, set state to
/// `ConnectedAwaitingMorePeers`.
///
/// If enough peers connect, create a new HB instance and promote to validator
/// (`ConnectedValidator`) immediately (i.e. root bootnode).
fn handle_incoming_hello(hb: Arc<HydrabadgerInner>, peer_uid: Uuid, peer_in_addr: InAddr,
        peer_pk: PublicKey, w_messages: WireMessages) -> Peer {
    // Establish a write lock for the entire block to ensure no two incoming
    // connections can spawn an HB instance at the same time.
    let hb_clone = hb.clone();
    trace!("::handle_incoming: Locking state for writing...");
    let mut state = hb_clone.state.write();
    trace!("::handle_incoming: State locked for writing.");

    let peer_out_addr = OutAddr(w_messages.socket().peer_addr().unwrap());

    match state.discriminant() {
        StateDsct::Disconnected => {
            // let qhb = hb.gen_hb_instance(peer_uid, peer_pk.clone());

            // // Because this node is receiving a connection and has no previous
            // // connections, it is the root bootnode and is promoted to
            // // validator immediately.

            // let local_pk = hb.secret_key.lock().as_ref()
            //     .expect("Secret key already used!")
            //     .public_key();

            let peer_list = vec![
                // (peer_uid, peer_out_addr, peer_pk.clone())
                AwaitingPeerInfo {
                    out_addr: peer_out_addr,
                    info: NetworkNodeInfo {
                        uid: peer_uid,
                        in_addr: peer_in_addr,
                        pk: peer_pk.clone(),
                    },
                }
            ];
            state.set_connected_awaiting_more_peers(peer_list);
        },
        StateDsct::ConnectedAwaitingMorePeers(_peer_count) => {
            state.add_awaiting_peer(
                AwaitingPeerInfo {
                    out_addr: peer_out_addr,
                    info: NetworkNodeInfo {
                        uid: peer_uid,
                        in_addr: peer_in_addr,
                        pk: peer_pk.clone(),
                    },
                }
            );

            // if state.awaiting_peer_count() >= PEER_MINIMUM_COUNT {
            //     let qhb = hb.gen_hb_instance(peer_uid, peer_pk.clone());
            //     debug!("HB generated. Setting state to `ConnectedValidator`.");

            //     state.set_connection_pending();
            //     state.set_connected_observer(qhb);
            //     state.set_connected_validator();
            // }
        },
        StateDsct::ConnectionPending => unreachable!(),
        _ => {},
    }

    let peer = Peer::new(Some(peer_uid), hb, w_messages);
    peer.hb.peer_out_addrs.write().insert(peer_uid, peer_out_addr);

    // Get a NetworkState

    // Actually add peers:
    let net_state = NetworkState::AwaitingMorePeers(vec![]);

    peer.wire_welcome_received_change_add(net_state);

    // Relay incoming `HelloRequestChangeAdd` message.
    peer.hb.peer_internal_tx.unbounded_send(
            InternalMessage::wire(
                peer_uid,
                peer_out_addr,
                WireMessage::request_change_add(peer_uid, peer_in_addr, peer_pk)
            )
        )
        .map_err(|err| error!("Unable to send on internal tx. \
            Internal rx has dropped: {}", err))
        .unwrap();

    trace!("::handle_incoming: State unlocked for writing.");
    peer
}


/// Returns a future that handles incoming connections on `socket`.
fn handle_incoming(socket: TcpStream, hb: Arc<HydrabadgerInner>)
        -> impl Future<Item = (), Error = ()> {
    info!("Incoming connection from '{}'", socket.peer_addr().unwrap());
    let wire_messages = WireMessages::new(socket);

    wire_messages.into_future()
        .map_err(|(e, _)| e)
        .and_then(move |(msg_opt, w_messages)| {
            let hb = hb.clone();

            match msg_opt {
                Some(msg) => match msg.into_kind() {
                    // The only correct entry point:
                    WireMessageKind::HelloRequestChangeAdd(peer_uid, peer_in_addr, peer_pk) => {
                        let peer = handle_incoming_hello(hb, peer_uid, peer_in_addr, peer_pk, w_messages);
                        Either::B(peer)
                    },
                    _ => {
                        error!("Peer connected without sending \
                            `WireMessageKind::HelloRequestChangeAdd`.");
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
            tokio::spawn(handle_incoming(socket, hb.clone()));
            Ok(())
        });

    let uid = hydrabadger.inner.uid.clone();
    let in_addr = hydrabadger.inner.addr;
    let pk = hydrabadger.local_public_key();
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
                    let wire_hello_result = wire_messages.send_msg(
                        WireMessage::request_change_add(uid, in_addr, pk.clone()));
                    match wire_hello_result {
                        Ok(_) => {
                            // Set our state appropriately:
                            trace!("::node: Locking state for writing...");
                            hb.state.write().set_connection_pending();
                            trace!("::node: State locked and unlocked for writing.");

                            Either::A(Peer::new(None, hb, wire_messages))
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
            for (peer_addr, mut peer_tx) in peer_txs.iter() {
                trace!("     peer_addr: {}", peer_addr); }

            trace!("::node: Locking state for reading...");
            if let Some(qhb) = &hb.state.read().qhb() {
                debug!("::node: State locked for reading.");
                // If no other nodes are connected, panic:
                if qhb.dyn_hb().netinfo().num_nodes() > PEER_MINIMUM_COUNT {
                    info!("Generating and inputting {} random transactions...", NEW_TXNS_PER_INTERVAL);
                    // Send some random transactions to our internal HB instance.
                    let txns: Vec<_> = (0..NEW_TXNS_PER_INTERVAL).map(|_| Transaction::random(TXN_BYTES)).collect();
                    hb.peer_internal_tx.unbounded_send(
                        InternalMessage::new_transactions(hb.uid, OutAddr(*hb.addr), txns)
                    ).unwrap();
                } else {
                    error!("No nodes connected. Panicking...");
                    panic!("Invalid state");
                }
            } else {
                // info!("Disconnected, connection pending, or awaiting more peers.");
                info!("State: {:?}", hb.state.read().discriminant());
            }
            trace!("::node: State unlocked for reading.");

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