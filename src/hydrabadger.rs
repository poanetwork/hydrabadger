//! A hydrabadger consensus node.
//!

#![allow(unused_imports, dead_code, unused_variables, unused_mut)]

use std::{
    mem,
    time::{Duration, Instant},
    sync::{Arc},
    {self, iter, process, thread, time},
    collections::{
        hash_map::Iter as HashMapIter,
        BTreeSet, HashSet, HashMap, VecDeque,
    },
    fmt::{self, Debug},
    marker::{Send, Sync},
    net::{SocketAddr},
    rc::Rc,
    io::Cursor,
    ops::Deref,
    borrow::Borrow,
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
use parking_lot::{RwLock, Mutex, RwLockReadGuard, RwLockWriteGuard};
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
    HydrabadgerHandlerPoll,
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
    pub fn hello_request_change_add(uid: Uuid, in_addr: InAddr, pub_key: PublicKey) -> WireMessage {
        WireMessage { kind: WireMessageKind::HelloRequestChangeAdd(uid, in_addr, pub_key), }
    }

    /// Returns a `WelcomeReceivedChangeAdd` variant.
    pub fn welcome_received_change_add(uid: Uuid, net_state: NetworkState) -> WireMessage {
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
    // NewOutgoingConnection,
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

    // /// Returns a new `InternalMessage` with a default uid.
    // //
    // // TODO: Do something safer.
    // pub fn new_without_uid(src_addr: OutAddr, kind: InternalMessageKind) -> InternalMessage {
    //     InternalMessage::new(Uuid::default(), src_addr, kind)
    // }

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

    // pub fn new_outgoing_connection(src_addr: OutAddr) -> InternalMessage {
    //     InternalMessage::new_without_uid(src_addr, InternalMessageKind::NewOutgoingConnection)
    // }

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
struct PeerHandler {
    // Peer uid.
    uid: Option<Uuid>,

    // The incoming stream of messages:
    wire_msgs: WireMessages,

    /// Handle to the shared message state.
    hdb: Hydrabadger,

    // TODO: Consider adding back a separate clone of `peer_internal_tx`. Is
    // there any difference if capacity isn't an issue? -- doubtful

    /// Receive half of the message channel.
    rx: WireRx,

    /// Peer socket address.
    addr: OutAddr,
}

impl PeerHandler {
    /// Create a new instance of `Peer`.
    fn new(uid: Option<Uuid>, in_addr: Option<InAddr>, pk: Option<PublicKey>,
            mut hdb: Hydrabadger, wire_msgs: WireMessages) -> PeerHandler {
        // Get the client socket address
        let addr = OutAddr(wire_msgs.socket().peer_addr().unwrap());

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded();

        // Add an entry for this `Peer` in the shared state map.
        let guard = hdb.peers_mut().add(addr, tx, uid, in_addr, pk);

        PeerHandler {
            uid,
            wire_msgs,
            hdb,
            rx,
            addr,
        }
    }

    /// Sends a message to all connected peers.
    fn wire_to_all(&mut self, msg: &WireMessage) {
        // Now, send the message to all other peers
        for (p_addr, peer) in self.hdb.peers().iter() {
            // Don't send the message to ourselves
            if *p_addr != self.addr {
                // The send only fails if the rx half has been dropped,
                // however this is impossible as the `tx` half will be
                // removed from the map before the `rx` is dropped.
                peer.tx.unbounded_send(msg.clone()).unwrap();
            }
        }
    }

    /// Sends a hello response (welcome).
    fn wire_welcome_received_change_add(&self, net_state: NetworkState) {
        self.hdb.peers().get(&self.addr).unwrap()
            .tx.unbounded_send(WireMessage::welcome_received_change_add(self.uid.clone().unwrap(), net_state))
            .unwrap();
    }
}

/// A future representing the client connection.
impl Future for PeerHandler {
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
                    self.wire_msgs.start_send(v)?;

                    // Exceeded max messages per tick, schedule notification:
                    if i + 1 == MESSAGES_PER_TICK {
                        task::current().notify();
                    }
                }
                _ => break,
            }
        }

        // Flush the write buffer to the socket
        let _ = self.wire_msgs.poll_complete()?;

        // Read new messages from the socket
        while let Async::Ready(message) = self.wire_msgs.poll()? {
            trace!("Received message: {:?}", message);

            if let Some(msg) = message {
                match msg.into_kind() {
                    WireMessageKind::HelloRequestChangeAdd(src_uid, in_addr, _pub_key) => {
                        error!("Duplicate `WireMessage::HelloRequestChangeAdd` \
                            received from '{}'", src_uid);
                    },
                    WireMessageKind::WelcomeReceivedChangeAdd(src_uid, net_state) => {
                        self.uid = Some(src_uid);
                        self.hdb.send_internal(
                            InternalMessage::wire(src_uid, self.addr,
                                WireMessage::welcome_received_change_add(src_uid, net_state)
                            )
                        );
                    },
                    WireMessageKind::Message(msg) => {
                        let uid = self.uid.clone()
                            .expect("`WireMessageKind::Message` received before \
                                `WireMessageKind::WelcomeReceivedChangeAdd`");
                        self.hdb.send_internal(
                            InternalMessage::incoming_hb_message(uid, self.addr, msg)
                        )
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
        // We know we got a `NotReady` from either `self.rx` or `self.wire_msgs`, so
        // the contract is respected.
        Ok(Async::NotReady)
    }
}

impl Drop for PeerHandler {
    fn drop(&mut self) {
        debug!("Removing peer ({}: '{}') from the list of peers.",
            self.addr, self.uid.clone().unwrap());
        // Remove peer transmitter from the lists:
        self.hdb.peers_mut().remove(&self.addr);

        // // FIXME: Consider simply sending the 'change' input through the
        // // internal channel.
        // self.hdb.qhb.write().input(Input::Change(Change::Remove(self.uid)))
        //     .expect("Error adding new peer to HB");
        if let Some(uid) = self.uid.clone() {
            debug!("Sending peer ({}: '{}') disconnect internal message.",
                self.addr, self.uid.clone().unwrap());

            // self.hdb.peer_internal_tx.unbounded_send(InternalMessage::input(
            //     uid, self.addr, Input::Change(Change::Remove(uid)))).unwrap();

            self.hdb.send_internal(InternalMessage::peer_disconnect(
                uid, self.addr));
        }
    }
}


/// Nodes of the network.
#[derive(Clone, Debug)]
pub struct Peer {
    out_addr: OutAddr,
    tx: WireTx,
    uid: Option<Uuid>,
    in_addr: Option<InAddr>,
    pk: Option<PublicKey>,
}

impl Peer {
    /// Returns a new `Peer`
    fn new(out_addr: OutAddr, tx: WireTx, uid: Option<Uuid>, in_addr: Option<InAddr>,
            pk: Option<PublicKey>) -> Peer {
        Peer { out_addr, tx, uid, in_addr, pk, }
    }
}


/// Peer nodes of the network.
#[derive(Debug)]
pub(crate) struct Peers {
    peers: HashMap<OutAddr, Peer>,
    addrs: HashMap<Uuid, OutAddr>,
}

impl Peers {
    /// Returns a new empty list of peers.
    pub(crate) fn new() -> Peers {
        Peers {
            peers: HashMap::with_capacity(64),
            addrs: HashMap::with_capacity(64),
        }
    }

    /// Adds a peer to the list.
    pub(crate) fn add(&mut self, out_addr: OutAddr, tx: WireTx, uid: Option<Uuid>,
            in_addr: Option<InAddr>, pk: Option<PublicKey>) {
        let peer = Peer { out_addr, tx, uid, in_addr, pk, };
        if let Some(uid) = peer.uid {
            self.addrs.insert(uid, peer.out_addr);
        }
        self.peers.insert(peer.out_addr, peer);
    }

    /// Removes a peer the list if it exists.
    pub(crate) fn remove<O: Borrow<OutAddr>>(&mut self, out_addr: O) {
        let peer = self.peers.remove(out_addr.borrow());
        if let Some(p) = peer {
            if let Some(uid) = p.uid {
                self.addrs.remove(&uid);
            }
        }
    }

    pub(crate) fn get<O: Borrow<OutAddr>>(&self, out_addr: O) -> Option<&Peer> {
        self.peers.get(out_addr.borrow())
    }

    pub(crate) fn get_by_uid<U: Borrow<Uuid>>(&self, uid: U) -> Option<&Peer> {
        // self.peers.get()
        self.addrs.get(uid.borrow()).and_then(|addr| self.get(addr))
    }

    /// Returns an Iterator over the list of peers.
    pub(crate) fn iter(&self) -> HashMapIter<OutAddr, Peer> {
        self.peers.iter()
    }

    /// Returns the current number of connected peers.
    pub(crate) fn len(&self) -> usize {
        self.peers.len()
    }
}


// /// A peer on the list of awaiting nodes.
// #[derive(Clone, Debug)]
// pub(crate) struct AwaitingPeerInfo {
//     out_addr: OutAddr,
//     info: NetworkNodeInfo,
// }

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
        // peers: Vec<AwaitingPeerInfo>,
        peers: &Peers,
        ) -> QueueingHoneyBadger<Vec<Transaction>, Uuid> {
    assert!(peers.len() >= PEER_MINIMUM_COUNT);
    let mut node_ids: BTreeSet<_> = peers.iter().filter_map(|(_, p)| p.uid).collect();
    node_ids.insert(local_uid);
    let mut pks: Vec<_> = peers.iter().filter_map(|(_, p)| p.pk.clone()).collect();
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
    ConnectedAwaitingMorePeers,
    ConnectedObserver,
    ConnectedValidator,
}


// The current hydrabadger state.
pub(crate) enum State {
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
        // count: usize,
    },
    ConnectedAwaitingMorePeers {
        local_uid: Uuid,
        local_addr: InAddr,
        secret_key: Option<ClearOnDrop<Box<SecretKey>>>,
        // peers: Option<Vec<AwaitingPeerInfo>>,
    },
    ConnectedObserver {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uuid>>,
        // peers: Option<Vec<AwaitingPeerInfo>>,
        // sk: SecretKey,
        // pk_set: PublicKeySet,
    },
    ConnectedValidator {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uuid>>,
        // peers: Option<Vec<AwaitingPeerInfo>>,
        // sk: SecretKey,
        // pk_set: PublicKeySet,
    },
}

impl State {
    /// Returns the state discriminant.
    fn discriminant(&self) -> StateDsct {
        match self {
            State::Disconnected { .. } => StateDsct::Disconnected,
            State::ConnectionPending { /*count,*/ .. } => StateDsct::ConnectionPending,
            State::ConnectedAwaitingMorePeers { /*ref peers,*/ .. } => {
                StateDsct::ConnectedAwaitingMorePeers
            },
            State::ConnectedObserver { .. } => StateDsct::ConnectedObserver,
            State::ConnectedValidator { .. } => StateDsct::ConnectedValidator,
        }
    }

    /// Returns a new `State::Disconnected`.
    fn disconnected(local_uid: Uuid, local_addr: InAddr,
            secret_key: ClearOnDrop<Box<SecretKey>>) -> State {
        State::Disconnected { local_uid, local_addr, secret_key: Some(secret_key) }
    }

    /// Changes the variant (in-place) of this `State` to `ConnectedAwaitingMorePeers`.
    fn set_connected_awaiting_more_peers(&mut self) {
        *self = match *self {
            State::Disconnected { local_uid, local_addr, ref mut secret_key } => {
                info!("Setting state: `ConnectedAwaitingMorePeers`.");
                State::ConnectedAwaitingMorePeers { local_uid, local_addr,
                    secret_key: secret_key.take(), /*peers: Some(peers)*/ }
            },
            _ => panic!("Must be disconnected before awaiting more peers."),
        };
    }

    /// Adds an awaiting peer and changes the network state appropriately when
    /// enough peers are added.
    fn awaiting_peer_added(&mut self, /*peer_info: AwaitingPeerInfo,*/ peers: &Peers) {
        *self = match *self {
            State::ConnectedAwaitingMorePeers {
                    local_uid, local_addr, ref mut secret_key, /*ref mut peers*/ } => {
                // let mut prs = peers.take().expect("Peer list gone!");
                // peers.push(peer_info);
                // if peers.len() >= PEER_MINIMUM_COUNT {
                //     info!("Peer count has reached the minimum ({}). Creating HB instance.",
                //         PEER_MINIMUM_COUNT);
                //     // TODO: If this is `None`, make a new secret key?.
                //     let sk = secret_key.take().expect("Secret key is gone!");
                //     let qhb = gen_qhb_instance(local_uid, local_addr, sk, peers);
                //     State::ConnectedValidator { qhb: Some(qhb) }
                // } else {
                //     info!("Setting state: `ConnectedAwaitingMorePeers`.");
                //     State::ConnectedAwaitingMorePeers {
                //         local_uid,
                //         local_addr,
                //         secret_key: secret_key.take(),
                //         // peers: Some(prs)
                //     }
                // }


                State::ConnectedAwaitingMorePeers {
                        local_uid,
                        local_addr,
                        secret_key: secret_key.take(),
                        // peers: Some(prs)
                    }
                // unimplemented!();

            },
            _ => panic!("Must be awaiting more peers to add a peer."),
        }
    }

    /// Adds an awaiting peer and changes the network state appropriately when
    /// enough peers are added.
    fn remove_awaiting_peer(&mut self, uid: Uuid, out_addr: OutAddr) {
        *self = match *self {
            State::ConnectedAwaitingMorePeers {
                    local_uid, local_addr, ref mut secret_key, /*ref mut peers*/ } => {
            //     let mut prs = peers.take().expect("Peer list gone!");
            //     // prs.remove(peer_info);
            //     let start_len = prs.len();
            //     prs.retain(|p_info| {
            //         debug_assert!((p_info.info.uid == uid) == (p_info.out_addr == out_addr));
            //         p_info.info.uid != uid
            //     });
            //     debug_assert!(prs.len() == start_len - 1);

            //     debug!("Peer ({}: '{}') removed from await list.", out_addr, uid);

            //     if prs.len() == 0 {
            //         info!("Setting state: `Disconnected`.");
            //         State::Disconnected {
            //             local_uid,
            //             local_addr,
            //             secret_key: secret_key.take()
            //         }
            //     } else {
            //         State::ConnectedAwaitingMorePeers {
            //             local_uid,
            //             local_addr,
            //             secret_key: secret_key.take(),
            //             peers: Some(prs),
            //         }
            //      }

                State::ConnectedAwaitingMorePeers {
                        local_uid,
                        local_addr,
                        secret_key: secret_key.take(),
                        // peers: Some(prs),
                    }
            },
            _ => panic!("Must be awaiting more peers to remove a peer."),
        }
    }

    /// Changes the variant (in-place) of this `State` to `ConnectionPending`.
    //
    // TODO: Add proper error handling:
    fn set_connection_pending(&mut self, _count: usize) {
        *self = match *self {
            State::Disconnected { local_uid, local_addr, ref mut secret_key, } => {
                info!("Setting state: `ConnectionPending`.");
                State::ConnectionPending {
                    local_uid,
                    local_addr,
                    secret_key: secret_key.take(),
                    // count,
                }
            },
            _ => panic!("Must be disconnected before connecting."),
        };
    }

    /// Sets state to `ConnectionPending` or increases pending connection
    /// count, otherwise does nothing.
    fn outgoing_connection_added(&mut self) {
        let dsct = self.discriminant();
        match dsct {
            StateDsct::Disconnected => self.set_connection_pending(1),
            _ => match self {
                State::ConnectionPending { /*ref mut count,*/ .. } => {
                    // *count += 1;
                    unimplemented!();
                }
                _ => {}
            }
        }
    }

    /// Decreases pending connection count or sets state to `Disconnected`,
    /// otherwise does nothing.
    fn outgoing_connection_dropped(&mut self) {
        let mut set_disconnected = false;
        match self {
            State::ConnectionPending { /*ref mut count,*/ .. } => {
                // *count -= 1;
                // if *count == 0 { set_disconnected = true; }
                unimplemented!();
            },
            _ => {},
        }
        if set_disconnected {
            *self = match *self {
                State::ConnectionPending { local_uid, local_addr, ref mut secret_key, .. } => {
                    State::Disconnected { local_uid, local_addr, secret_key: secret_key.take() }
                },
                _ => unreachable!(),
            }
        }
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

    /// Returns the network state, if possible.
    fn network_state(&self) -> Option<NetworkState> {
        match self {
            State::ConnectedAwaitingMorePeers { /*peers,*/ .. } => {
                // peers.as_ref().map(|ps| {
                //     NetworkState::AwaitingMorePeers(
                //         ps.iter().map(|p| p.info.clone()).collect()
                //     )
                // })
                unimplemented!();
            },
            State::ConnectedObserver { .. } => unimplemented!(),
            State::ConnectedValidator { .. } => unimplemented!(),
            _ => None,
        }
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

    /// Returns the public key for the local node.
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


/// The `Arc` wrapped portion of `Hydrabadger`.
///
/// Shared all over the place.
struct HydrabadgerInner {
    /// Node uid:
    uid: Uuid,
    /// Incoming connection socket.
    addr: InAddr,

    peers: RwLock<Peers>,

    /// The current state containing HB when connected.
    state: RwLock<State>,

    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_tx: InternalTx,

    peer_out_queue: RwLock<VecDeque<TargetedMessage<Message<usize>, usize>>>,
    batch_out_queue: RwLock<VecDeque<Batch<Transaction, usize>>>,

    unhandled_inputs: RwLock<VecDeque<()>>,
}


/// Hydrabadger event (internal message) handler.
pub struct HydrabadgerHandler {
    hdb: Hydrabadger,
    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_rx: InternalRx,
}

impl HydrabadgerHandler {
    fn handle_internal_message(&mut self, i_msg: InternalMessage) {
        let (src_uid, src_addr, w_msg) = i_msg.into_parts();

        trace!("HydrabadgerHandler::handle_internal_message: Locking state for writing...");
        let mut state = self.hdb.state_mut();
        trace!("HydrabadgerHandler::handle_internal_message: State locked for writing.");

        match w_msg {
            // InternalMessageKind::NewOutgoingConnection => {
            //     // NOTE: In this case `src_uid` is a default and must not be used!
            //     match state.discriminant() {
            //         _ => unimplemented!(),
            //     }
            // }
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
                        StateDsct::ConnectedAwaitingMorePeers => {
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
                    StateDsct::ConnectedAwaitingMorePeers => {
                        debug!("Removing peer ({}: '{}') from await list.", src_addr, src_uid);
                        state.remove_awaiting_peer(src_uid, src_addr);
                    },
                    StateDsct::ConnectedObserver | StateDsct::ConnectedValidator => {
                        unimplemented!();
                    },
                }
            }
        }

        trace!("HydrabadgerHandler::handle_internal_message: State unlocked for writing.");
    }

    fn handle_hb_message(&mut self) {
        trace!("HydrabadgerHandler::handle_hb_message: Locking state for writing...");
        let mut state = self.hdb.state_mut();
        trace!("HydrabadgerHandler::handle_hb_message: State locked for writing.");

    }

    fn handle_output(&mut self) {
        trace!("HydrabadgerHandler::handle_output: Locking state for writing...");
        let mut state = self.hdb.state_mut();
        trace!("HydrabadgerHandler::handle_output: State locked for writing.");

    }
}

impl Future for HydrabadgerHandler {
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
                    info!("Shutting down HydrabadgerHandler...");
                    return Ok(Async::Ready(()));
                },
                Ok(Async::NotReady) => {},
                Err(()) => return Err(Error::HydrabadgerHandlerPoll),
            };
        }

        // Forward outgoing messages:
        trace!("HydrabadgerHandler::poll: Locking state for writing...");
        let mut state = self.hdb.inner.state.write();
        trace!("HydrabadgerHandler::poll: State locked for writing.");

        if let Some(qhb) = state.qhb_mut() {
            let peers = self.hdb.peers();

            for (i, hb_msg) in qhb.message_iter().enumerate() {
                info!("Forwarding message: {:?}", hb_msg);
                match hb_msg.target {
                    Target::Node(p_uid) => {
                        peers.get_by_uid(&p_uid).unwrap().tx.unbounded_send(
                            WireMessage::message(hb_msg.message)).unwrap();
                    },
                    Target::All => {
                        for (p_addr, peer) in peers.iter()
                                .filter(|(&p_addr, _)| p_addr != OutAddr(*self.hdb.inner.addr)) {
                            peer.tx.unbounded_send(WireMessage::message(hb_msg.message.clone())).unwrap();
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

        trace!("HydrabadgerHandler::poll: State unlocked for writing.");
        Ok(Async::NotReady)
    }
}


/// A `HoneyBadger` network node.
#[derive(Clone)]
pub struct Hydrabadger {
    inner: Arc<HydrabadgerInner>,
    handler: Arc<Mutex<Option<HydrabadgerHandler>>>,
}

impl Hydrabadger {
    /// Returns a new Hydrabadger node.
    pub fn new(addr: SocketAddr) -> Self {
        let uid = Uuid::new_v4();
        let secret_key = ClearOnDrop::new(Box::new(SecretKey::rand(
            &mut rand::thread_rng())));

        let (peer_internal_tx, peer_internal_rx) = mpsc::unbounded();

        let inner = Arc::new(HydrabadgerInner {
            uid,
            addr: InAddr(addr),
            peers: RwLock::new(Peers::new()),
            state: RwLock::new(State::disconnected(uid, InAddr(addr), secret_key)),
            peer_internal_tx,
            peer_out_queue: RwLock::new(VecDeque::new()),
            batch_out_queue: RwLock::new(VecDeque::new()),
            unhandled_inputs: RwLock::new(VecDeque::new()),
        });

        let hdb = Hydrabadger {
            inner,
            handler: Arc::new(Mutex::new(None)),
        };

        let handler = HydrabadgerHandler {
            hdb: hdb.clone(),
            peer_internal_rx,
        };

        *hdb.handler.lock() = Some(handler);

        hdb
    }

    /// Returns the pre-created handler.
    pub fn handler(&self) -> Option<HydrabadgerHandler> {
        self.handler.lock().take()
    }

    /// Returns a reference to the inner state.
    pub(crate) fn state(&self) -> RwLockReadGuard<State> {
        self.inner.state.read()
    }

    /// Returns a mutable reference to the inner state.
    pub(crate) fn state_mut(&self) -> RwLockWriteGuard<State> {
        self.inner.state.write()
    }

    /// Returns a reference to the peers list.
    pub(crate) fn peers(&self) -> RwLockReadGuard<Peers> {
        self.inner.peers.read()
    }

    /// Returns a mutable reference to the peers list.
    pub(crate) fn peers_mut(&self) -> RwLockWriteGuard<Peers> {
        self.inner.peers.write()
    }

    /// Sends a message on the internal tx.
    fn send_internal(&self, msg: InternalMessage) {
        self.inner.peer_internal_tx.unbounded_send(msg)
            .expect("Unable to send on internal tx. Internal rx has dropped");
    }

    /// Handles a `WireMessageKind::HelloRequestChangeAdd`.
    ///
    /// If the current state is `StateDsct::Disconnected`, set state to
    /// `ConnectedAwaitingMorePeers`.
    ///
    /// If enough peers connect, create a new HB instance and promote to validator
    /// (`ConnectedValidator`) immediately (i.e. root bootnode).
    fn handle_incoming_hello(self: Hydrabadger, peer_uid: Uuid, peer_in_addr: InAddr,
            peer_pk: PublicKey, w_messages: WireMessages) -> PeerHandler {
        // Establish a write lock for the entire block to ensure no two incoming
        // connections can spawn an HB instance at the same time.
        let mut hdb_clone = self.clone();
        trace!("::handle_incoming: Locking state for writing...");
        let mut state = hdb_clone.state_mut();
        trace!("::handle_incoming: State locked for writing.");

        let peer_out_addr = OutAddr(w_messages.socket().peer_addr().unwrap());

        let peer = PeerHandler::new(Some(peer_uid), Some(peer_in_addr), Some(peer_pk.clone()),
            self, w_messages);

        // Get the current `NetworkState`:
        //
        // FIXME: Implement returning observer/validator network states:
        let net_state = state.network_state().unwrap();

        // Send response to remote peer:
        peer.wire_welcome_received_change_add(net_state);

        // Relay incoming `HelloRequestChangeAdd` message internally.
        peer.hdb.send_internal(
            InternalMessage::wire(
                peer_uid,
                peer_out_addr,
                WireMessage::hello_request_change_add(peer_uid, peer_in_addr, peer_pk)
            )
        );

        trace!("::handle_incoming: State unlocked for writing.");
        peer
    }

    /// Returns a future that handles incoming connections on `socket`.
    fn handle_incoming(self, socket: TcpStream)
            -> impl Future<Item = (), Error = ()> {
        info!("Incoming connection from '{}'", socket.peer_addr().unwrap());
        let wire_msgs = WireMessages::new(socket);

        wire_msgs.into_future()
            .map_err(|(e, _)| e)
            .and_then(move |(msg_opt, w_messages)| {
                let hdb = self.clone();

                match msg_opt {
                    Some(msg) => match msg.into_kind() {
                        // The only correct entry point:
                        WireMessageKind::HelloRequestChangeAdd(peer_uid, peer_in_addr, peer_pk) => {
                            let peer = hdb.handle_incoming_hello(peer_uid, peer_in_addr, peer_pk, w_messages);
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
                        // a welcome_request_change_add message.
                        Either::A(future::ok(()))
                    },
                }
            })
            .map_err(|err| error!("Connection error = {:?}", err))
    }

    /// Returns a future that connects to new peer.
    fn connect_outgoing(self, remote_addr: SocketAddr)
            -> impl Future<Item = (), Error = ()> {
        let uid = self.inner.uid.clone();
        let in_addr = self.inner.addr;
        let pk = self.state().local_public_key();

        TcpStream::connect(&remote_addr)
            .map_err(Error::from)
            .and_then(move |socket| {
                // Wrap the socket with the frame delimiter and codec:
                let mut wire_msgs = WireMessages::new(socket);
                let wire_hello_result = wire_msgs.send_msg(
                    WireMessage::hello_request_change_add(uid, in_addr, pk));
                match wire_hello_result {
                    Ok(_) => {
                        // Set our state appropriately:
                        trace!("::node: Locking state for writing...");
                        self.inner.state.write().outgoing_connection_added();
                        trace!("::node: State locked and unlocked for writing.");

                        // hdb.send_internal(InternalMessage::new_outgoing_connection());

                        let peer = PeerHandler::new(None, None, None, self, wire_msgs);

                        Either::A(peer)
                    },
                    Err(err) => Either::B(future::err(err)),
                }
            })
            .map_err(|err| error!("Socket connection error: {:?}", err))
    }

    /// Binds to a host address and returns a future which starts the node.
    pub fn node(mut self, remotes: HashSet<SocketAddr>)
            -> impl Future<Item = (), Error = ()> {
        let socket = TcpListener::bind(&self.inner.addr).unwrap();
        info!("Listening on: {}", self.inner.addr);

        let hdb = self.clone();
        let listen = socket.incoming()
            .map_err(|err| error!("Error accepting socket: {:?}", err))
            .for_each(move |socket| {
                tokio::spawn(hdb.clone().handle_incoming(socket));
                Ok(())
            });

        let hdb = self.clone();
        let connect = future::lazy(move || {
            for &remote_addr in remotes.iter() {
                tokio::spawn(hdb.clone().connect_outgoing(remote_addr));
            }
            Ok(())
        });

        let hdb = self.clone();
        let generate_txns = Interval::new(Instant::now(), Duration::from_millis(3000))
            .for_each(move |_| {
                let hdb = hdb.clone();
                let peers = hdb.peers();

                trace!("PeerHandler list:");
                for (peer_addr, mut peer) in peers.iter() {
                    trace!("     peer_addr: {}", peer_addr); }

                trace!("::node: Locking state for reading...");
                if let Some(qhb) = &hdb.state().qhb() {
                    debug!("::node: State locked for reading.");
                    // If no other nodes are connected, panic:
                    if qhb.dyn_hb().netinfo().num_nodes() > PEER_MINIMUM_COUNT {
                        info!("Generating and inputting {} random transactions...", NEW_TXNS_PER_INTERVAL);
                        // Send some random transactions to our internal HB instance.
                        let txns: Vec<_> = (0..NEW_TXNS_PER_INTERVAL).map(|_| Transaction::random(TXN_BYTES)).collect();
                        hdb.send_internal(
                            InternalMessage::new_transactions(hdb.inner.uid, OutAddr(*hdb.inner.addr), txns)
                        );
                    } else {
                        error!("No nodes connected. Panicking...");
                        panic!("Invalid state");
                    }
                } else {
                    // info!("Disconnected, connection pending, or awaiting more peers.");
                    info!("State: {:?}", hdb.state().discriminant());
                }
                trace!("::node: State unlocked for reading.");

                Ok(())
            })
            .map_err(|err| error!("List connection inverval error: {:?}", err));

        let hdb_handler = self.handler()
            .map_err(|err| error!("HydrabadgerHandler internal error: {:?}", err));

        listen.join4(connect, generate_txns, hdb_handler).map(|(_, _, _, _)| ())
    }

    /// Starts a node.
    pub fn run_node(mut self, remotes: HashSet<SocketAddr>) {
        tokio::run(self.node(remotes));
    }
}


// /// Handles a `WireMessageKind::HelloRequestChangeAdd`.
// ///
// /// If the current state is `StateDsct::Disconnected`, set state to
// /// `ConnectedAwaitingMorePeers`.
// ///
// /// If enough peers connect, create a new HB instance and promote to validator
// /// (`ConnectedValidator`) immediately (i.e. root bootnode).
// fn handle_incoming_hello(self: Hydrabadger, peer_uid: Uuid, peer_in_addr: InAddr,
//         peer_pk: PublicKey, w_messages: WireMessages) -> PeerHandler {
//     // Establish a write lock for the entire block to ensure no two incoming
//     // connections can spawn an HB instance at the same time.
//     let mut hdb_clone = self.clone();
//     trace!("::handle_incoming: Locking state for writing...");
//     let mut state = hdb_clone.state_mut();
//     trace!("::handle_incoming: State locked for writing.");

//     let peer_out_addr = OutAddr(w_messages.socket().peer_addr().unwrap());

//     // match state.discriminant() {
//     //     StateDsct::Disconnected => {
//     //         // let peer_list = vec![
//     //         //     AwaitingPeerInfo {
//     //         //         out_addr: peer_out_addr,
//     //         //         info: NetworkNodeInfo {
//     //         //             uid: peer_uid,
//     //         //             in_addr: peer_in_addr,
//     //         //             pk: peer_pk.clone(),
//     //         //         },
//     //         //     }
//     //         // ];
//     //         state.set_connected_awaiting_more_peers(/*peer_list*/);
//     //     },
//     //     StateDsct::ConnectedAwaitingMorePeers => {
//     //         // NEW: `awaiting_peer_added`
//     //         //
//     //         // state.add_awaiting_peer(
//     //         //     AwaitingPeerInfo {
//     //         //         out_addr: peer_out_addr,
//     //         //         info: NetworkNodeInfo {
//     //         //             uid: peer_uid,
//     //         //             in_addr: peer_in_addr,
//     //         //             pk: peer_pk.clone(),
//     //         //         },
//     //         //     }
//     //         // );
//     //         // state.adding(&)
//     //         unimplemented!();
//     //     },
//     //     StateDsct::ConnectionPending => unreachable!(),
//     //     _ => {},
//     // }

//     let peer = PeerHandler::new(Some(peer_uid), Some(peer_in_addr), Some(peer_pk.clone()),
//         self, w_messages);

//     // Get the current `NetworkState`:
//     //
//     // FIXME: Implement returning observer/validator network states:
//     let net_state = state.network_state().unwrap();

//     // Send response to remote peer:
//     peer.wire_welcome_received_change_add(net_state);

//     // Relay incoming `HelloRequestChangeAdd` message internally.
//     peer.hdb.send_internal(
//         InternalMessage::wire(
//             peer_uid,
//             peer_out_addr,
//             WireMessage::hello_request_change_add(peer_uid, peer_in_addr, peer_pk)
//         )
//     );

//     trace!("::handle_incoming: State unlocked for writing.");
//     peer
// }

// /// Returns a future that handles incoming connections on `socket`.
// fn handle_incoming(hdb: Hydrabadger, socket: TcpStream)
//         -> impl Future<Item = (), Error = ()> {
//     info!("Incoming connection from '{}'", socket.peer_addr().unwrap());
//     let wire_msgs = WireMessages::new(socket);

//     wire_msgs.into_future()
//         .map_err(|(e, _)| e)
//         .and_then(move |(msg_opt, w_messages)| {
//             let hdb = hdb.clone();

//             match msg_opt {
//                 Some(msg) => match msg.into_kind() {
//                     // The only correct entry point:
//                     WireMessageKind::HelloRequestChangeAdd(peer_uid, peer_in_addr, peer_pk) => {
//                         let peer = handle_incoming_hello(hdb, peer_uid, peer_in_addr, peer_pk, w_messages);
//                         Either::B(peer)
//                     },
//                     _ => {
//                         error!("Peer connected without sending \
//                             `WireMessageKind::HelloRequestChangeAdd`.");
//                         Either::A(future::ok(()))
//                     },
//                 },
//                 None => {
//                     // The remote client closed the connection without sending
//                     // a welcome_request_change_add message.
//                     Either::A(future::ok(()))
//                 },
//             }
//         })
//         .map_err(|err| error!("Connection error = {:?}", err))
// }

// /// Connects to a new peer.
// fn connect_outgoing(hdb: Hydrabadger, remote_addr: SocketAddr)
//         -> impl Future<Item = (), Error = ()> {
//     let uid = hdb.inner.uid.clone();
//     let in_addr = hdb.inner.addr;
//     let pk = hdb.state().local_public_key();

//     TcpStream::connect(&remote_addr)
//         .map_err(Error::from)
//         .and_then(move |socket| {
//             // Wrap the socket with the frame delimiter and codec:
//             let mut wire_msgs = WireMessages::new(socket);
//             let wire_hello_result = wire_msgs.send_msg(
//                 WireMessage::hello_request_change_add(uid, in_addr, pk));
//             match wire_hello_result {
//                 Ok(_) => {
//                     // Set our state appropriately:
//                     trace!("::node: Locking state for writing...");
//                     hdb.inner.state.write().outgoing_connection_added();
//                     trace!("::node: State locked and unlocked for writing.");

//                     // hdb.send_internal(InternalMessage::new_outgoing_connection());

//                     let peer = PeerHandler::new(None, None, None, hdb, wire_msgs);

//                     Either::A(peer)
//                 },
//                 Err(err) => Either::B(future::err(err)),
//             }
//         })
//         .map_err(|err| error!("Socket connection error: {:?}", err))
// }


// /// Starts a node.
// pub fn run_node(hdb: Hydrabadger, remotes: HashSet<SocketAddr>) {
//     tokio::run(hdb.node(remotes));
// }