mod state;
mod handler;
mod hydrabadger;

use std::{
    {self, },
    fmt::{self},
    net::{SocketAddr},
    ops::Deref,
};
use futures::{
    StartSend, AsyncSink,
    sync::mpsc,
};
use tokio::{
    io,
    net::{TcpStream},
    prelude::*,
};
use tokio_io::codec::length_delimited::Framed;
use bytes::{BytesMut, Bytes};
use rand::{self, Rng, Rand};
use uuid::Uuid;
use serde_bytes;
use bincode::{self, serialize, deserialize};
use hbbft::{
    crypto::PublicKey,
    sync_key_gen::{Part, Ack},
    dynamic_honey_badger::Message as DhbMessage,
    queueing_honey_badger::{Error as QhbError, Input as QhbInput, QueueingHoneyBadgerStep},
};
use self::state::{State, StateDsct};
use self::handler::{HydrabadgerHandler};

pub use self::hydrabadger::Hydrabadger;

const BATCH_SIZE: usize = 100;
const TXN_BYTES: usize = 100;
const NEW_TXN_INTERVAL_MS: u64 = 5000;
const NEW_TXNS_PER_INTERVAL: usize = 20;

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
    fn from(_err: QhbError) -> Error {
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
    GeneratingKeys(Vec<NetworkNodeInfo>),
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
    KeyGenPart(Part),
    KeyGenPartAck(Ack),
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

    pub fn key_gen_part(part: Part) -> WireMessage {
        WireMessage { kind: WireMessageKind::KeyGenPart(part) }
    }

    pub fn key_gen_part_ack(outcome: Ack) -> WireMessage {
        WireMessageKind::KeyGenPartAck(outcome).into()
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

