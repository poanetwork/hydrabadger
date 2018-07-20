//! A hydrabadger consensus node.
//!

#![allow(unused_imports, dead_code, unused_variables, unused_mut, unused_assignments,
    unreachable_code)]

use std::{
    mem,
    time::{Duration, Instant},
    sync::{Arc},
    {self, iter, process, thread, time},
    collections::{
        hash_map::Iter as HashMapIter,
        BTreeSet, HashSet, HashMap, VecDeque, BTreeMap,
    },
    fmt::{self, Debug},
    marker::{Send, Sync},
    net::{SocketAddr},
    rc::Rc,
    io::Cursor,
    ops::Deref,
    borrow::Borrow,
};
use crossbeam::sync::SegQueue;
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
        SecretKeySet, PublicKey, PublicKeySet, SecretKey, SignatureShare,
    },
    sync_key_gen::{SyncKeyGen, Part, PartOutcome, Ack},
    messaging::{DistAlgorithm, NetworkInfo, SourcedMessage, Target, TargetedMessage},
    proto::message::BroadcastProto,
    dynamic_honey_badger::Message as DhbMessage,
    queueing_honey_badger::{Error as QhbError, QueueingHoneyBadger, Input as QhbInput, Batch,
        Change, QueueingHoneyBadgerStep},
    dynamic_honey_badger::{Error as DhbError, DynamicHoneyBadger},
};
use peer::{Peer, PeerHandler, Peers};

const BATCH_SIZE: usize = 150;
const TXN_BYTES: usize = 10;
const NEW_TXNS_PER_INTERVAL: usize = 40;

// The minimum number of peers needed to spawn a HB instance.
const HB_PEER_MINIMUM_COUNT: usize = 4;

// Number of times to attempt wire message re-send.
const WIRE_MESSAGE_RETRY_MAX: usize = 10;



/// Transmit half of the wire message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
pub(crate) type WireTx = mpsc::UnboundedSender<WireMessage>;

/// Receive half of the wire message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
pub(crate) type WireRx = mpsc::UnboundedReceiver<WireMessage>;

/// Transmit half of the internal message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type InternalTx = mpsc::UnboundedSender<InternalMessage>;

/// Receive half of the internal message channel.
// TODO: Use a bounded tx/rx (find a sensible upper bound):
type InternalRx = mpsc::UnboundedReceiver<InternalMessage>;

type Step = QueueingHoneyBadgerStep<Vec<Transaction>, Uid>;
type Input = QhbInput<Vec<Transaction>, Uid>;
type Message = DhbMessage<Uid>;


/// A unique identifier.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Uid(pub(crate) Uuid);

impl Uid {
    /// Returns a new, random `Uid`.
    pub fn new() -> Uid {
        Uid(Uuid::new_v4())
    }

    // /// Returns a new, zeroed `Uid`.
    // pub fn nil() -> Uid {
    //     Uid(Uuid::nil())
    // }
}

impl Rand for Uid {
    fn rand<R: Rng>(_rng: &mut R) -> Uid {
        Uid::new()
    }
}

impl fmt::Display for Uid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::Debug for Uid {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}


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
    QhbPart,
    /// TEMPORARY UNTIL WE FIX HB ERROR TYPES:
    #[fail(display = "DynamicHoneyBadger error")]
    Dhb,
    /// TEMPORARY UNTIL WE FIX HB ERROR TYPES:
    #[fail(display = "QueuingHoneyBadger error [FIXME]")]
    Qhb(()),
    /// TEMPORARY UNTIL WE FIX HB ERROR TYPES:
    #[fail(display = "QueuingHoneyBadger step error")]
    HbStepError,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<QhbError> for Error {
    fn from(err: QhbError) -> Error {
        Error::Qhb(())
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
    pub(crate) uid: Uid,
    pub(crate) in_addr: InAddr,
    pub(crate) pk: PublicKey,
}

/// The current state of the network.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkState {
    None,
    Unknown(Vec<NetworkNodeInfo>),
    AwaitingMorePeers(Vec<NetworkNodeInfo>),
    Active(Vec<NetworkNodeInfo>),
}


/// Messages sent over the network between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WireMessageKind {
    HelloFromValidator(Uid, InAddr, PublicKey, NetworkState),
    HelloRequestChangeAdd(Uid, InAddr, PublicKey),
    WelcomeReceivedChangeAdd(Uid, PublicKey, NetworkState),
    RequestNetworkState,
    NetworkState(NetworkState),
    Goodbye,
    #[serde(with = "serde_bytes")]
    Bytes(Bytes),
    Message(Message),
    KeyGenProposal(Part),
    KeyGenProposalAck(Ack),
    // TargetedMessage(TargetedMessage<Uid>),
}


/// Messages sent over the network between nodes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireMessage {
    kind: WireMessageKind,
}

impl WireMessage {
    pub fn hello_from_validator(uid: Uid, in_addr: InAddr, pk: PublicKey,
            net_state: NetworkState) -> WireMessage {
        WireMessageKind::HelloFromValidator(uid, in_addr, pk, net_state).into()
    }

    /// Returns a `HelloRequestChangeAdd` variant.
    pub fn hello_request_change_add(uid: Uid, in_addr: InAddr, pk: PublicKey) -> WireMessage {
        WireMessage { kind: WireMessageKind::HelloRequestChangeAdd(uid, in_addr, pk), }
    }

    /// Returns a `WelcomeReceivedChangeAdd` variant.
    pub fn welcome_received_change_add(uid: Uid, pk: PublicKey, net_state: NetworkState)
            -> WireMessage {
        WireMessage { kind: WireMessageKind::WelcomeReceivedChangeAdd(uid, pk, net_state) }
    }

    pub fn key_gen_proposal(proposal: Part) -> WireMessage {
        WireMessage { kind: WireMessageKind::KeyGenProposal(proposal) }
    }

    pub fn key_gen_proposal_accept(outcome: Ack) -> WireMessage {
        WireMessageKind::KeyGenProposalAck(outcome).into()
    }

    /// Returns a `Message` variant.
    pub fn message(msg: Message) -> WireMessage {
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

impl From<WireMessageKind> for WireMessage {
    fn from(kind: WireMessageKind) -> WireMessage {
        WireMessage { kind }
    }
}


/// A message between internal threads/tasks.
#[derive(Clone, Debug)]
pub enum InternalMessageKind {
    Wire(WireMessage),
    // NewTransactions(Vec<Transaction>),
    HbMessage(Message),
    HbInput(Input),
    PeerDisconnect,
    NewIncomingConnection(InAddr, PublicKey),
    NewOutgoingConnection,
}


/// A message between internal threads/tasks.
#[derive(Clone, Debug)]
pub struct InternalMessage {
    src_uid: Option<Uid>,
    src_addr: OutAddr,
    kind: InternalMessageKind,
}

impl InternalMessage {
    pub fn new(src_uid: Option<Uid>, src_addr: OutAddr, kind: InternalMessageKind) -> InternalMessage {
        InternalMessage { src_uid: src_uid, src_addr, kind }
    }

    /// Returns a new `InternalMessage` without a uid.
    pub fn new_without_uid(src_addr: OutAddr, kind: InternalMessageKind) -> InternalMessage {
        InternalMessage::new(None, src_addr, kind)
    }

    pub fn wire(src_uid: Option<Uid>, src_addr: OutAddr, wire_message: WireMessage) -> InternalMessage {
        InternalMessage::new(src_uid, src_addr, InternalMessageKind::Wire(wire_message))
    }

    // pub fn new_transactions(src_uid: Uid, src_addr: OutAddr, txns: Vec<Transaction>) -> InternalMessage {
    //     InternalMessage::new(Some(src_uid), src_addr, InternalMessageKind::NewTransactions(txns))
    // }

    pub fn hb_message(src_uid: Uid, src_addr: OutAddr, msg: Message) -> InternalMessage {
        InternalMessage::new(Some(src_uid), src_addr, InternalMessageKind::HbMessage(msg))
    }

    pub fn hb_input(src_uid: Uid, src_addr: OutAddr, input: Input) -> InternalMessage {
        InternalMessage::new(Some(src_uid), src_addr, InternalMessageKind::HbInput(input))
    }

    pub fn peer_disconnect(src_uid: Uid, src_addr: OutAddr) -> InternalMessage {
        InternalMessage::new(Some(src_uid), src_addr, InternalMessageKind::PeerDisconnect)
    }

    pub fn new_incoming_connection(src_uid: Uid, src_addr: OutAddr, src_in_addr: InAddr,
            src_pk: PublicKey) -> InternalMessage {
        InternalMessage::new(Some(src_uid), src_addr,
            InternalMessageKind::NewIncomingConnection(src_in_addr, src_pk))
    }

    pub fn new_outgoing_connection(src_addr: OutAddr) -> InternalMessage {
        InternalMessage::new_without_uid(src_addr, InternalMessageKind::NewOutgoingConnection)
    }

    /// Returns the source unique identifier this message was received in.
    pub fn src_uid(&self) -> Option<&Uid> {
        self.src_uid.as_ref()
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
    pub fn into_parts(self) -> (Option<Uid>, OutAddr, InternalMessageKind) {
        (self.src_uid, self.src_addr, self.kind)
    }
}


/// A stream/sink of `WireMessage`s connected to a socket.
#[derive(Debug)]
pub struct WireMessages {
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


/// A HoneyBadger input or message.
#[derive(Clone, Debug)]
pub(crate) enum InputOrMessage {
    Input(Input),
    Message(Message),
}


/// A `State` discriminant.
#[derive(Copy, Clone, Debug)]
enum StateDsct {
    Disconnected,
    ConnectionPending,
    ConnectedAwaitingMorePeers,
    ConnectedGeneratingKeys,
    ConnectedObserver,
    ConnectedValidator,
}


// The current hydrabadger state.
pub(crate) enum State {
    Disconnected { },
    ConnectionPending { },
    ConnectedAwaitingMorePeers {
        // Queued input to HoneyBadger:
        iom_queue: Option<SegQueue<InputOrMessage>>,
    },
    ConnectedGeneratingKeys {
        sync_key_gen: Option<SyncKeyGen<Uid>>,
        public_key: Option<PublicKey>,
        acceptance_count: usize,
        // Queued input to HoneyBadger:
        iom_queue: SegQueue<InputOrMessage>,
    },
    ConnectedObserver {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uid>>,
    },
    ConnectedValidator {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uid>>,
    },
}

impl State {
    /// Returns the state discriminant.
    fn discriminant(&self) -> StateDsct {
        match self {
            State::Disconnected { .. } => StateDsct::Disconnected,
            State::ConnectionPending { .. } => StateDsct::ConnectionPending,
            State::ConnectedAwaitingMorePeers { .. } => StateDsct::ConnectedAwaitingMorePeers,
            State::ConnectedObserver { .. } => StateDsct::ConnectedObserver,
            State::ConnectedValidator { .. } => StateDsct::ConnectedValidator,
            State::ConnectedGeneratingKeys{ .. } => StateDsct::ConnectedGeneratingKeys,
        }
    }

    /// Returns a new `State::Disconnected`.
    fn disconnected(/*local_uid: Uid, local_addr: InAddr,*/ /*secret_key: SecretKey*/) -> State {
        State::Disconnected { /*secret_key: secret_key*/ }
    }

    /// Sets the state to `ConnectionPending`.
    //
    // TODO: Add proper error handling:
    fn set_connection_pending(&mut self) {
        *self = match *self {
            State::Disconnected { /*ref secret_key*/ } => {
                info!("Setting state: `ConnectionPending`.");
                State::ConnectionPending {
                    // secret_key: secret_key.clone(),
                    // count,
                }
            },
            _ => panic!("Must be disconnected before calling `::set_connection_pending`."),
        };
    }

    /// Sets the state to `ConnectedAwaitingMorePeers`.
    fn set_connected_awaiting_more_peers(&mut self) {
        *self = match *self {
            State::Disconnected { /*ref secret_key*/ }
                    | State::ConnectionPending { /*ref secret_key*/ } => {
                info!("Setting state: `ConnectedAwaitingMorePeers`.");
                State::ConnectedAwaitingMorePeers { iom_queue: Some(SegQueue::new()) }
            },
            // _ => panic!("Must be disconnected before awaiting more peers."),
            _ => return,
        };
    }

    /// Sets the state to `ConnectedAwaitingMorePeers`.
    fn set_generating_keys(&mut self, local_id: &Uid, local_sk: SecretKey, peers: &Peers)
            -> (Part, Ack) {
        let (proposal, accept);
        *self = match *self {
            State::ConnectedAwaitingMorePeers { ref mut iom_queue } => {
                // let secret_key = secret_key.clone();
                let (threshold, node_count) = (1, 5);

                let mut public_keys: BTreeMap<Uid, PublicKey> = peers.validators().map(|p| {
                    p.pub_info().map(|(uid, _, pk)| (*uid, *pk)).unwrap()
                }).collect();

                let pk = local_sk.public_key();
                public_keys.insert(*local_id, pk);

                let (mut sync_key_gen, opt_proposal) = SyncKeyGen::new(*local_id, local_sk,
                    public_keys.clone(), threshold);
                proposal = opt_proposal.expect("This node is not a validator (somehow)!");

                // Handle our own proposal (weird).
                info!("KEY GENERATION: Processing our own proposal...");
                accept = match sync_key_gen.handle_part(&local_id, proposal.clone()) {
                    Some(PartOutcome::Valid(accept)) => accept,
                    Some(PartOutcome::Invalid(faults)) => panic!("Invalid proposal \
                        (FIXME: handle): {:?}", faults),
                    None => unimplemented!(),
                };

                State::ConnectedGeneratingKeys { sync_key_gen: Some(sync_key_gen),
                    public_key: Some(pk), acceptance_count: 1, iom_queue: iom_queue.take().unwrap() }
            },
            _ => panic!("State::set_generating_keys: \
                Must be State::ConnectedAwaitingMorePeers"),
        };

        (proposal, accept)
    }


    /// Changes the variant (in-place) of this `State` to `ConnectedObserver`.
    //
    // TODO: Add proper error handling:
    fn set_connected_observer(&mut self, /*qhb: QueueingHoneyBadger<Vec<Transaction>, Uid>*/) {
        *self = match *self {
            State::ConnectionPending { .. } => State::ConnectedObserver { qhb: None },
            _ => panic!("Must be connection-pending before becoming connected-observer."),
        };
        unimplemented!()
    }

    /// Changes the variant (in-place) of this `State` to `ConnectedObserver`.
    //
    // TODO: Add proper error handling:
    fn set_connected_validator(&mut self, local_uid: Uid, local_sk: SecretKey, peers: &Peers) {
        *self = match *self {
            State::ConnectedGeneratingKeys { ref mut sync_key_gen, mut public_key, .. } => {
                let mut sync_key_gen = sync_key_gen.take().unwrap();
                let pk = public_key.take().unwrap();

                // generate(&self) -> (PublicKeySet, Option<SecretKeyShare>)
                let (pk_set, sk_share_opt) = sync_key_gen.generate();
                let sk_share = sk_share_opt.unwrap();

                assert!(peers.count_validators() >= HB_PEER_MINIMUM_COUNT);

                let mut node_ids: BTreeMap<Uid, PublicKey> = peers.validators().map(|p| {
                    (p.uid().cloned().unwrap(), p.public_key().cloned().unwrap())
                }).collect();
                node_ids.insert(local_uid, local_sk.public_key());

                let netinfo = NetworkInfo::new(
                    local_uid,
                    sk_share,
                    pk_set,
                    local_sk,
                    node_ids,
                );

                let dhb = DynamicHoneyBadger::builder(netinfo)
                    .build()
                    .expect("instantiate DHB");
                let qhb = QueueingHoneyBadger::builder(dhb).batch_size(BATCH_SIZE).build();

                info!("== HONEY BADGER INITIALIZED ==");

                State::ConnectedValidator { qhb: Some(qhb) }
            }
            _ => panic!("State::set_connected_validator: \
                State must be `ConnectedGeneratingKeys`."),
        };
    }

    /// Sets state to `ConnectionPending` if `Disconnected`, otherwise does
    /// nothing.
    fn outgoing_connection_added(&mut self, peers: &Peers) {
        let dsct = self.discriminant();
        match dsct {
            StateDsct::Disconnected => self.set_connection_pending(),
            // _ => match self {
            //     State::ConnectionPending { /*ref mut count,*/ .. } => {
            //         // *count += 1;
            //         unimplemented!();
            //     }
            //     _ => {}
            // }
            _ => {},
        }
    }

    /// Decreases pending connection count or sets state to `Disconnected`,
    /// otherwise does nothing.
    fn peer_connection_dropped(&mut self, peers: &Peers) {
        if peers.count_total() == 0 {
            *self = match *self {
                State::Disconnected { .. } => {
                    panic!("Received `WireMessageKind::PeerDisconnect` while disconnected.");
                },
                State::ConnectionPending { /*ref mut secret_key,*/ .. } => {
                    State::Disconnected { /*secret_key: secret_key.clone()*/ }
                },
                _ => {
                    error!("State::peer_connection_dropped: No peers connected!");
                    return;
                },
            }
        }
    }

    /// Sets the state appropriately.
    fn update_state(&mut self, peers: &Peers) {

    }

    /// Returns the network state, if possible.
    fn network_state(&self, peers: &Peers) -> NetworkState {
        let peer_infos = peers.peers().filter_map(|peer| {
            peer.pub_info().map(|(&uid, &in_addr, &pk)| {
                NetworkNodeInfo { uid, in_addr, pk }
            })
        }).collect::<Vec<_>>();
        match self {
            State::ConnectedAwaitingMorePeers { .. } => {
                NetworkState::AwaitingMorePeers(peer_infos)
            },
            State::ConnectedObserver { .. } => {
                NetworkState::Active(peer_infos)
            },
            State::ConnectedValidator { .. } => {
                NetworkState::Active(peer_infos)
            },
            _ => NetworkState::Unknown(peer_infos),
        }
    }

    /// Returns a reference to the internal HB instance.
    fn qhb(&self) -> Option<&QueueingHoneyBadger<Vec<Transaction>, Uid>> {
        match self {
            State::ConnectedObserver { ref qhb, .. } => qhb.as_ref(),
            State::ConnectedValidator { ref qhb, .. } => qhb.as_ref(),
            _ => None,
        }
    }

    /// Returns a reference to the internal HB instance.
    fn qhb_mut(&mut self) -> Option<&mut QueueingHoneyBadger<Vec<Transaction>, Uid>> {
        match self {
            State::ConnectedObserver { ref mut qhb, .. } => qhb.as_mut(),
            State::ConnectedValidator { ref mut qhb, .. } => qhb.as_mut(),
            _ => None,
        }
    }

    /// Presents input to HoneyBadger or queues it for later.
    ///
    /// Cannot be called while disconnected or connection-pending.
    fn input(&mut self, input: Input) -> Option<Result<Step, QhbError>> {
        match self {
            State::ConnectedObserver { ref mut qhb, .. }
                    | State::ConnectedValidator { ref mut qhb, .. } => {
                return Some(qhb.as_mut().unwrap().input(input))
            },
            State::ConnectedAwaitingMorePeers { ref iom_queue } => {
                iom_queue.as_ref().unwrap().push(InputOrMessage::Input(input));
            },
            State::ConnectedGeneratingKeys { ref iom_queue, .. } => {
                iom_queue.push(InputOrMessage::Input(input));
            },
            _ => panic!("State::input: Must be connected to input to honey badger."),
        }
        None
    }

    /// Presents a message to HoneyBadger or queues it for later.
    ///
    /// Cannot be called while disconnected or connection-pending.
    fn handle_message(&mut self, src_uid: &Uid, msg: Message) -> Option<Result<Step, QhbError>> {
        match self {
            State::ConnectedObserver { ref mut qhb, .. }
                    | State::ConnectedValidator { ref mut qhb, .. } => {
                return Some(qhb.as_mut().unwrap().handle_message(src_uid, msg))
            },
            State::ConnectedAwaitingMorePeers { ref iom_queue } => {
                iom_queue.as_ref().unwrap().push(InputOrMessage::Message(msg));
            },
            State::ConnectedGeneratingKeys { ref iom_queue, .. } => {
                iom_queue.push(InputOrMessage::Message(msg));
            },
            _ => panic!("State::input: Must be connected to input to honey badger."),
        }
        None
    }
}


// struct Hb


/// The `Arc` wrapped portion of `Hydrabadger`.
///
/// Shared all over the place.
struct HydrabadgerInner {
    /// Node uid:
    uid: Uid,
    /// Incoming connection socket.
    addr: InAddr,

    /// This node's secret key.
    secret_key: SecretKey,

    peers: RwLock<Peers>,

    /// The current state containing HB when connected.
    state: RwLock<State>,

    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_tx: InternalTx,

    // peer_out_queue: RwLock<VecDeque<TargetedMessage<Message, Uid>>>,
    // batch_out_queue: RwLock<VecDeque<Step>>,

    unhandled_inputs: RwLock<VecDeque<()>>,
}


/// Hydrabadger event (internal message) handler.
pub struct HydrabadgerHandler {
    hdb: Hydrabadger,
    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_rx: InternalRx,
    // Outgoing wire message queue:
    wire_queue: SegQueue<(Uid, WireMessage, usize)>,
    // Output from HoneyBadger:
    step_queue: SegQueue<Step>,
}

impl HydrabadgerHandler {
    fn wire_to_all(&self, msg: WireMessage, peers: &Peers) {
        for (p_addr, peer) in peers.iter()
                .filter(|(&p_addr, _)| p_addr != OutAddr(*self.hdb.inner.addr)) {
            peer.tx().unbounded_send(msg.clone()).unwrap();
        }
    }

    fn wire_to_validators(&self, msg: WireMessage, peers: &Peers) {
        for peer in peers.validators()
                .filter(|p| p.out_addr() != &OutAddr(*self.hdb.inner.addr)) {
            peer.tx().unbounded_send(msg.clone()).unwrap();
        }
    }

    // `tar_uid` of `None` sends to all peers.
    fn wire_to(&self, tar_uid: Uid, msg: WireMessage, retry_count: usize, peers: &Peers) {
        match peers.get_by_uid(&tar_uid) {
            Some(p) => p.tx().unbounded_send(msg).unwrap(),
            None => self.wire_queue.push((tar_uid, msg, retry_count + 1)),
        }
    }

    // fn send_welcome(&self, src_addr: OutAddr, state: &mut State, peers: &Peers) {
    //     // Get the current `NetworkState`:
    //     let net_state = state.network_state(&peers);

    //     // Send response to remote peer:
    //     peers.get(&src_addr).unwrap().tx().unbounded_send(
    //         WireMessage::welcome_received_change_add(
    //             self.hdb.inner.uid.clone(), state.local_public_key(), net_state)
    //     ).unwrap();
    // }

    fn handle_net_state(&self, net_state: NetworkState, state: &mut State, peers: &Peers) {
        let peer_infos;
        match net_state {
            NetworkState::AwaitingMorePeers(p_infos) | NetworkState::Unknown(p_infos) => {
                peer_infos = p_infos;
                state.set_connected_awaiting_more_peers();
                // Do other stuff?
            },
            NetworkState::Active(p_infos) => {
                peer_infos = p_infos;
                // state.set_connected_observer();
                // TODO: Do other stuff..
                // unimplemented!();
                // FIXME: DO NOTHING FOR NOW
            },
            NetworkState::None => panic!("`NetworkState::None` received."),
        }

        // Connect to all newly discovered peers.
        for peer_info in peer_infos.iter() {
            // Only connect with peers which are not already
            // connected (and are not us).
            if peer_info.in_addr != self.hdb.inner.addr &&
                    !peers.contains_in_addr(&peer_info.in_addr) {
                let local_pk = self.hdb.inner.secret_key.public_key();
                tokio::spawn(self.hdb.clone().connect_outgoing(
                    peer_info.in_addr.0, local_pk,
                    Some((peer_info.uid, peer_info.in_addr, peer_info.pk))));
            }
        }
    }

    fn handle_new_established_peer(&self, src_uid: Uid, src_addr: OutAddr, src_pk: PublicKey,
            state: &mut State, peers: &Peers) {
        match state.discriminant() {
            StateDsct::Disconnected | StateDsct::ConnectionPending => {
                panic!("HydrabadgerHandler::handle_new_established_peer: \
                    Received `WireMessageKind::WelcomeRequestChangeAdd` while \
                    `StateDsct::Disconnected` or `ConnectionPending`.");
            },
            StateDsct::ConnectedAwaitingMorePeers => {
                if peers.count_validators() >= HB_PEER_MINIMUM_COUNT {
                    info!("== BEGINNING KEY GENERATION ==");

                    let local_uid = self.hdb.inner.uid;
                    let local_in_addr = self.hdb.inner.addr;
                    let local_sk = self.hdb.inner.secret_key.public_key();

                    let (proposal, accept) = state.set_generating_keys(&local_uid,
                        self.hdb.inner.secret_key.clone(), peers);

                    info!("KEY GENERATION: Sending initial proposals and our own acceptance.");
                    self.wire_to_validators(
                        WireMessage::hello_from_validator(
                            local_uid, local_in_addr, local_sk, state.network_state(&peers)),
                        peers);
                    self.wire_to_validators(WireMessage::key_gen_proposal(proposal), peers);
                    self.wire_to_validators(WireMessage::key_gen_proposal_accept(accept), peers);
                }
            },
            StateDsct::ConnectedGeneratingKeys { .. } => {
                // This *could* be called multiple times when initially
                // establishing outgoing connections. Do nothing for now.
            },
            StateDsct::ConnectedObserver | StateDsct::ConnectedValidator => {
                let qhb = state.qhb_mut().unwrap();
                info!("Change-Adding ('{}') to honey badger.", src_uid);
                let step = qhb.input(QhbInput::Change(Change::Add(src_uid, src_pk)))
                    .expect("Error adding new peer to HB");
                self.step_queue.push(step);
            },
            // _ => unimplemented!("Need to figure out what to do with peers that connect \
            //     during keygen process."),
        }
    }

    fn handle_key_gen_proposal(&self, src_uid: &Uid, proposal: Part, state: &mut State) {
        match state {
            State::ConnectedGeneratingKeys { ref mut sync_key_gen, .. } => {
                // TODO: Move this match block into a function somewhere for re-use:
                info!("KEY GENERATION: Processing proposal from {}...", src_uid);
                let accept = match sync_key_gen.as_mut().unwrap().handle_part(src_uid, proposal) {
                    Some(PartOutcome::Valid(accept)) => accept,
                    Some(PartOutcome::Invalid(faults)) => panic!("Invalid proposal \
                        (FIXME: handle): {:?}", faults),
                    None => {
                        error!("`QueueingHoneyBadger::handle_part` returned `None`.");
                        return;
                    }
                };
                let peers = self.hdb.peers();
                info!("KEY GENERATION: Proposal from '{}' accepted. Broadcasting...", src_uid);
                self.wire_to_validators(WireMessage::key_gen_proposal_accept(accept), &peers);
            }
            _ => panic!("::handle_key_gen_proposal: State must be `ConnectedGeneratingKeys`."),
        }
    }

    fn handle_key_gen_proposal_accept(&self, src_uid: &Uid, accept: Ack, state: &mut State, peers: &Peers) {
        let mut complete = false;
        match state {
            State::ConnectedGeneratingKeys { ref mut sync_key_gen, ref mut acceptance_count, .. } => {
                let mut sync_key_gen = sync_key_gen.as_mut().unwrap();
                info!("KEY GENERATION: Processing acceptance from '{}'...", src_uid);
                let fault_log = sync_key_gen.handle_ack(src_uid, accept);
                if !fault_log.is_empty() {
                    error!("Errors accepting proposal:\n {:?}", fault_log);
                }
                *acceptance_count += 1;

                debug!("   Peers complete: {}", sync_key_gen.count_complete());
                debug!("   Proposals acceptances: {}", acceptance_count);

                // *acceptance_count >= HB_PEER_MINIMUM_COUNT &&
                if sync_key_gen.count_complete() > HB_PEER_MINIMUM_COUNT {
                    // assert_eq!(sync_key_gen.count_complete(), *acceptance_count);
                    // debug!("KEY GENERATION: SyncKeyGen::count_complete() -> {}",
                    //     sync_key_gen.count_complete());
                    assert!(sync_key_gen.is_ready());
                    complete = true;
                }
            },
            State::ConnectedValidator { .. } | State::ConnectedObserver { .. } => {
                // Extra incoming accept messages. Ignore.
            }
            _ => panic!("::handle_key_gen_proposal_accept: State must be `ConnectedGeneratingKeys`."),
        }

        if complete {
            info!("== INITIALIZING HONEY BADGER ==");
            state.set_connected_validator(self.hdb.inner.uid, self.hdb.inner.secret_key.clone(), peers);
        }
    }

    fn handle_internal_message(&self, i_msg: InternalMessage, state: &mut State)
            -> Result<(), Error> {
        let (src_uid, src_out_addr, w_msg) = i_msg.into_parts();

        // trace!("HydrabadgerHandler::handle_internal_message: Locking 'state' for writing...");
        // let mut state = self.hdb.state_mut();
        // trace!("HydrabadgerHandler::handle_internal_message: 'state' locked for writing.");

        match w_msg {
            // New incoming connection:
            InternalMessageKind::NewIncomingConnection(_src_in_addr, src_pk) => {
                let peers = self.hdb.peers();

                if let StateDsct::Disconnected = state.discriminant() {
                    state.set_connected_awaiting_more_peers();
                }

                // Get the current `NetworkState`:
                let net_state = state.network_state(&peers);

                // Send response to remote peer:
                peers.get(&src_out_addr).unwrap().tx().unbounded_send(
                    WireMessage::welcome_received_change_add(
                        self.hdb.inner.uid.clone(), self.hdb.inner.secret_key.public_key(),
                        net_state)
                ).unwrap();

                // Modify state accordingly:
                self.handle_new_established_peer(src_uid.unwrap(), src_out_addr, src_pk, state, &peers);
            },
            // New outgoing connection (initial):
            InternalMessageKind::NewOutgoingConnection => {
                // This message must be immediately followed by either a
                // `WireMessage::HelloFromValidator` or
                // `WireMessage::WelcomeReceivedChangeAdd`.
                debug_assert!(src_uid.is_none());

                let peers = self.hdb.peers();
                state.outgoing_connection_added(&peers);
            },
            InternalMessageKind::HbMessage(msg) => {
                // info!("Handling incoming message: {:?}", msg);
                // qhb.handle_message(&src_uid, msg).unwrap();
                if let Some(step_res) = state.handle_message(
                        src_uid.as_ref().unwrap(), msg) {
                    let step = step_res.map_err(|err| {
                        error!("Honey Badger handle_message error: {:?}", err);
                        Error::HbStepError
                    })?;
                    self.step_queue.push(step);
                }
            },
            InternalMessageKind::HbInput(input) => {
                if let Some(step_res) = state.input(input) {
                    let step = step_res.map_err(|err| {
                        error!("Honey Badger input error: {:?}", err);
                        Error::HbStepError
                    })?;
                    self.step_queue.push(step);
                }
            }
            InternalMessageKind::PeerDisconnect => {
                info!("Peer disconnected: ({}: '{}').", src_out_addr, src_uid.clone().unwrap());
                state.peer_connection_dropped(&*self.hdb.peers());

                // TODO: Send a node removal (Change-Remove) vote?

                match state.discriminant() {
                    StateDsct::Disconnected => {
                        // panic!("Received `WireMessageKind::PeerDisconnect` while disconnected.");
                    },
                    StateDsct::ConnectionPending => {
                        // unimplemented!();
                    },
                    StateDsct::ConnectedAwaitingMorePeers => {
                        // info!("Removing peer ({}: '{}') from await list.",
                        //     src_out_addr, src_uid.clone().unwrap());
                        // state.peer_connection_dropped(&*self.hdb.peers());
                    },
                    StateDsct::ConnectedGeneratingKeys => {

                    },
                    StateDsct::ConnectedObserver | StateDsct::ConnectedValidator => {
                        // unimplemented!();
                    },
                }
            },
            InternalMessageKind::Wire(w_msg) => match w_msg.into_kind() {
                // This is sent on the wire to ensure that we have all of the
                // relevant details for a peer (generally preceeding other
                // messages which may arrive before `Welcome...`.
                WireMessageKind::HelloFromValidator(src_uid_new, src_in_addr, src_pk, net_state) => {
                    debug!("Received hello from {}", src_uid_new);
                    let mut peers = self.hdb.peers_mut();
                    match peers.establish_validator(src_out_addr, (src_uid_new, src_in_addr, src_pk)) {
                        true => assert!(src_uid_new == src_uid.clone().unwrap()),
                        false => assert!(src_uid.is_none()),
                    }

                    // Modify state accordingly:
                    self.handle_net_state(net_state, state, &peers);
                }
                // Received from recently new outgoing connection:
                WireMessageKind::WelcomeReceivedChangeAdd(src_uid_new, src_pk, net_state) => {
                    debug!("Received NetworkState: \n{:?}", net_state);
                    assert!(src_uid_new == src_uid.clone().unwrap());
                    let mut peers = self.hdb.peers_mut();

                    // Set new (outgoing-connection) peer's public info:
                    peers.establish_validator(src_out_addr,
                        (src_uid_new, InAddr(src_out_addr.0), src_pk));

                    // Modify state accordingly:
                    self.handle_net_state(net_state, state, &peers);

                    // Modify state accordingly:
                    self.handle_new_established_peer(src_uid_new, src_out_addr, src_pk, state, &peers);
                },
                WireMessageKind::KeyGenProposal(proposal) => {
                    self.handle_key_gen_proposal(&src_uid.unwrap(), proposal, state);
                },
                WireMessageKind::KeyGenProposalAck(accept) => {
                    let peers = self.hdb.peers();
                    self.handle_key_gen_proposal_accept(&src_uid.unwrap(), accept, state, &peers);
                },
                _ => {},
            },
        }

        // trace!("HydrabadgerHandler::handle_internal_message: 'state' unlocked for writing.");
        Ok(())
    }
}

impl Future for HydrabadgerHandler {
    type Item = ();
    type Error = Error;

    /// Polls the internal message receiver until all txs are dropped.
    fn poll(&mut self) -> Poll<(), Error> {
        // Ensure the loop can't hog the thread for too long:
        const MESSAGES_PER_TICK: usize = 50;

        trace!("HydrabadgerHandler::poll: Locking 'state' for writing...");
        let mut state = self.hdb.inner.state.write();
        trace!("HydrabadgerHandler::poll: 'state' locked for writing.");

         // Handle incoming internal messages:
        for i in 0..MESSAGES_PER_TICK {
            match self.peer_internal_rx.poll() {
                Ok(Async::Ready(Some(i_msg))) => {
                    self.handle_internal_message(i_msg, &mut state)?;

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

        let peers = self.hdb.peers();

        // Process outgoing wire queue:
        while let Some((tar_uid, msg, retry_count)) = self.wire_queue.try_pop() {
            if retry_count < WIRE_MESSAGE_RETRY_MAX {
                self.wire_to(tar_uid, msg, retry_count + 1, &peers);
            }
        }

        // Forward outgoing messages:
        if let Some(qhb) = state.qhb_mut() {
            for (i, hb_msg) in qhb.message_iter().enumerate() {
                trace!("Forwarding message: {:?}", hb_msg);
                match hb_msg.target {
                    Target::Node(p_uid) => {
                        self.wire_to(p_uid, WireMessage::message(hb_msg.message), 0, &peers);
                    },
                    Target::All => {
                        self.wire_to_all(WireMessage::message(hb_msg.message), &peers);
                    },
                }

                // Exceeded max messages per tick, schedule notification:
                if i + 1 == MESSAGES_PER_TICK {
                    task::current().notify();
                }
            }
        }

        drop(peers);

        // Process all honey badger output batches:
        while let Some(mut step) = self.step_queue.try_pop() {
            if step.output.len() > 0 {
                info!("NEW STEP STEP OUTPUT:",);
                for output in step.output.drain(..) {
                    info!("    BATCH: \n{:?}", output);
                    // TODO: Something useful!
                }
                if !step.fault_log.is_empty() {
                    error!("    FAULT LOG: \n{:?}", step.fault_log);
                }
            }
        }

        drop(state);
        trace!("HydrabadgerHandler::poll: 'state' unlocked for writing.");

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
        let uid = Uid::new();
        let secret_key = SecretKey::rand(&mut rand::thread_rng());

        let (peer_internal_tx, peer_internal_rx) = mpsc::unbounded();

        let inner = Arc::new(HydrabadgerInner {
            uid,
            addr: InAddr(addr),
            secret_key,
            peers: RwLock::new(Peers::new()),
            state: RwLock::new(State::disconnected(/*uid, InAddr(addr),*/ /*secret_key*/)),
            peer_internal_tx,
            // peer_out_queue: RwLock::new(VecDeque::new()),
            // batch_out_queue: RwLock::new(VecDeque::new()),
            unhandled_inputs: RwLock::new(VecDeque::new()),
        });

        let hdb = Hydrabadger {
            inner,
            handler: Arc::new(Mutex::new(None)),
        };

        let handler = HydrabadgerHandler {
            hdb: hdb.clone(),
            peer_internal_rx,
            wire_queue: SegQueue::new(),
            step_queue: SegQueue::new(),
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
        trace!("Hydrabadger::state: Locking 'state' for reading...");
        let state = self.inner.state.read();
        trace!("Hydrabadger::state: 'state' locked for reading.");
        state
    }

    /// Returns a mutable reference to the inner state.
    pub(crate) fn state_mut(&self) -> RwLockWriteGuard<State> {
        trace!("Hydrabadger::state_mut: Locking 'state' for writing...");
        let state = self.inner.state.write();
        trace!("Hydrabadger::state_mut: 'state' locked for writing.");
        state
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
    pub(crate) fn send_internal(&self, msg: InternalMessage) {
        self.inner.peer_internal_tx.unbounded_send(msg)
            .expect("Unable to send on internal tx. Internal rx has dropped");
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
                            // Also adds a `Peer` to `self.peers`.
                            let peer_h = PeerHandler::new(Some((peer_uid, peer_in_addr, peer_pk)),
                                self.clone(), w_messages);

                            // Relay incoming `HelloRequestChangeAdd` message internally.
                            peer_h.hdb().send_internal(
                                InternalMessage::new_incoming_connection(
                                    peer_uid, *peer_h.out_addr(), peer_in_addr, peer_pk)
                            );
                            Either::B(peer_h)
                        },
                        _ => {
                            // TODO: Return this as a future-error (handled below):
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
    fn connect_outgoing(self, remote_addr: SocketAddr, local_pk: PublicKey,
            pub_info: Option<(Uid, InAddr, PublicKey)>)
            -> impl Future<Item = (), Error = ()> {
        let uid = self.inner.uid.clone();
        let in_addr = self.inner.addr;
        // let pk = self.state().local_public_key().clone();
        info!("Initiating outgoing connection to: {}", remote_addr);

        TcpStream::connect(&remote_addr)
            .map_err(Error::from)
            .and_then(move |socket| {
                // Wrap the socket with the frame delimiter and codec:
                let mut wire_msgs = WireMessages::new(socket);
                let wire_hello_result = wire_msgs.send_msg(
                    WireMessage::hello_request_change_add(uid, in_addr, local_pk));
                match wire_hello_result {
                    Ok(_) => {
                        // // Set our state appropriately:
                        // trace!("::node: Locking 'state' for writing...");
                        // self.inner.state.write().outgoing_connection_added();
                        // trace!("::node: State locked and unlocked for writing.");

                        let peer = PeerHandler::new(pub_info, self.clone(), wire_msgs);

                        self.send_internal(InternalMessage::new_outgoing_connection(*peer.out_addr()));

                        Either::A(peer)
                    },
                    Err(err) => Either::B(future::err(err)),
                }
            })
            .map_err(|err| error!("Socket connection error: {:?}", err))
    }

    /// Returns a future that generates random transactions and logs status
    /// messages.
    fn generate_txns(self) -> impl Future<Item = (), Error = ()> {
        Interval::new(Instant::now(), Duration::from_millis(3000))
            .for_each(move |_| {
                let hdb = self.clone();
                let peers = hdb.peers();

                // Log state:
                let disc = hdb.state().discriminant();
                let peer_count = hdb.peers().count_total();
                info!("State: {:?}({})", disc, peer_count);

                // Log peer list:
                let peer_list = hdb.peers().peers().map(|p| {
                    p.in_addr().map(|ia| ia.to_string())
                        .unwrap_or(format!("No in address"))
                }).collect::<Vec<_>>();
                debug!("    Peers: {:?}", peer_list);

                // Log (trace) full peerhandler details:
                trace!("PeerHandler list:");
                for (peer_addr, mut peer) in peers.iter() {
                    trace!("     peer_addr: {}", peer_addr); }

                trace!("::node: Locking 'state' for reading...");
                if let Some(qhb) = &hdb.state().qhb() {
                    trace!("::node: 'state' locked for reading.");
                    // If no other nodes are connected, panic:
                    if qhb.dyn_hb().netinfo().num_nodes() > HB_PEER_MINIMUM_COUNT {
                        info!("Generating and inputting {} random transactions...", NEW_TXNS_PER_INTERVAL);
                        // Send some random transactions to our internal HB instance.
                        let txns: Vec<_> = (0..NEW_TXNS_PER_INTERVAL).map(|_| {
                            Transaction::random(TXN_BYTES)
                        }).collect();

                        hdb.send_internal(
                            InternalMessage::hb_input(hdb.inner.uid, OutAddr(*hdb.inner.addr), QhbInput::User(txns))
                        );
                    } else {
                        error!("No nodes connected. Panicking...");
                        panic!("Invalid state");
                    }
                }
                trace!("::node: 'state' unlocked for reading.");

                Ok(())
            })
            .map_err(|err| error!("List connection inverval error: {:?}", err))
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
        let local_pk = hdb.inner.secret_key.public_key();
        let connect = future::lazy(move || {
            for &remote_addr in remotes.iter() {
                tokio::spawn(hdb.clone().connect_outgoing(remote_addr, local_pk, None));
            }
            Ok(())
        });

        let generate_txns = self.clone().generate_txns();

        let hdb_handler = self.handler()
            .map_err(|err| error!("HydrabadgerHandler internal error: {:?}", err));

        listen.join4(connect, generate_txns, hdb_handler).map(|(_, _, _, _)| ())
    }

    /// Starts a node.
    pub fn run_node(mut self, remotes: HashSet<SocketAddr>) {
        tokio::run(self.node(remotes));
    }
}
