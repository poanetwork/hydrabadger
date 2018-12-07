//! A hydrabadger consensus node.
//!

use super::{Error, Handler, StateDsct, StateMachine};
use crate::peer::{PeerHandler, Peers};
use crate::{
    key_gen, BatchRx, Change, Contribution, EpochRx, EpochTx, InAddr, InternalMessage, InternalTx,
    OutAddr, Uid, WireMessage, WireMessageKind, WireMessages,
};
use futures::{
    future::{self, Either},
    sync::mpsc,
};
use hbbft::crypto::{PublicKey, SecretKey};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use rand::{self, Rand};
use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::{Duration, Instant},
};
use tokio::{
    self,
    net::{TcpListener, TcpStream},
    prelude::*,
    timer::{Delay, Interval},
};

// The number of random transactions to generate per interval.
const DEFAULT_TXN_GEN_COUNT: usize = 5;
// The interval between randomly generated transactions.
const DEFAULT_TXN_GEN_INTERVAL: u64 = 5000;
// The number of bytes per randomly generated transaction.
const DEFAULT_TXN_GEN_BYTES: usize = 2;
// The minimum number of peers needed to spawn a HB instance.
const DEFAULT_KEYGEN_PEER_COUNT: usize = 2;
// Causes the primary hydrabadger thread to sleep after every batch. Used for
// debugging.
const DEFAULT_OUTPUT_EXTRA_DELAY_MS: u64 = 0;

/// Hydrabadger configuration options.
//
// TODO: Convert to builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub txn_gen_count: usize,
    pub txn_gen_interval: u64,
    // TODO: Make this a range:
    pub txn_gen_bytes: usize,
    pub keygen_peer_count: usize,
    pub output_extra_delay_ms: u64,
    pub start_epoch: u64,
}

impl Config {
    pub fn with_defaults() -> Config {
        Config {
            txn_gen_count: DEFAULT_TXN_GEN_COUNT,
            txn_gen_interval: DEFAULT_TXN_GEN_INTERVAL,
            txn_gen_bytes: DEFAULT_TXN_GEN_BYTES,
            keygen_peer_count: DEFAULT_KEYGEN_PEER_COUNT,
            output_extra_delay_ms: DEFAULT_OUTPUT_EXTRA_DELAY_MS,
            start_epoch: 0,
        }
    }
}

impl Default for Config {
    fn default() -> Config {
        Config::with_defaults()
    }
}

/// The `Arc` wrapped portion of `Hydrabadger`.
///
/// Shared all over the place.
struct Inner<T: Contribution> {
    /// Node uid:
    uid: Uid,
    /// Incoming connection socket.
    addr: InAddr,

    /// This node's secret key.
    secret_key: SecretKey,

    peers: RwLock<Peers<T>>,

    /// The current state containing HB when connected.
    state: RwLock<StateMachine<T>>,

    /// A reference to the last known state discriminant. May be stale when read.
    state_dsct_stale: Arc<AtomicUsize>,

    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_tx: InternalTx<T>,

    /// The earliest epoch from which we have not yet received output.
    //
    // Used as an initial value when a new epoch listener is registered.
    current_epoch: Mutex<u64>,

    // TODO: Create a separate type which uses a hashmap internally and allows
    // for Tx removal. Altenratively just `Option` wrap Txs.
    epoch_listeners: RwLock<Vec<EpochTx>>,

    config: Config,
}

/// A `HoneyBadger` network node.
#[derive(Clone)]
pub struct Hydrabadger<T: Contribution> {
    inner: Arc<Inner<T>>,
    handler: Arc<Mutex<Option<Handler<T>>>>,
    batch_rx: Arc<Mutex<Option<BatchRx<T>>>>,
}

impl<T: Contribution> Hydrabadger<T> {
    /// Returns a new Hydrabadger node.
    pub fn new(addr: SocketAddr, cfg: Config) -> Self {
        let uid = Uid::new();
        let secret_key = SecretKey::rand(&mut rand::OsRng::new().expect("Unable to create rng"));

        let (peer_internal_tx, peer_internal_rx) = mpsc::unbounded();
        let (batch_tx, batch_rx) = mpsc::unbounded();

        info!("");
        info!("Local Hydrabadger Node: ");
        info!("    UID:             {}", uid);
        info!("    Socket Address:  {}", addr);
        info!("    Public Key:      {:?}", secret_key.public_key());

        warn!("");
        warn!("****** This is an alpha build. Do not use in production! ******");
        warn!("");

        let current_epoch = cfg.start_epoch;

        let state = StateMachine::disconnected();
        let state_dsct_stale = state.dsct.clone();

        let inner = Arc::new(Inner {
            uid,
            addr: InAddr(addr),
            secret_key,
            peers: RwLock::new(Peers::new(InAddr(addr))),
            state: RwLock::new(state),
            state_dsct_stale,
            peer_internal_tx,
            config: cfg,
            current_epoch: Mutex::new(current_epoch),
            epoch_listeners: RwLock::new(Vec::new()),
        });

        let hdb = Hydrabadger {
            inner,
            handler: Arc::new(Mutex::new(None)),
            batch_rx: Arc::new(Mutex::new(Some(batch_rx))),
        };

        *hdb.handler.lock() = Some(Handler::new(hdb.clone(), peer_internal_rx, batch_tx));

        hdb
    }

    /// Returns a new Hydrabadger node.
    pub fn with_defaults(addr: SocketAddr) -> Self {
        Hydrabadger::new(addr, Config::default())
    }

    /// Returns the pre-created handler.
    pub fn handler(&self) -> Option<Handler<T>> {
        self.handler.lock().take()
    }

    /// Returns the batch output receiver.
    pub fn batch_rx(&self) -> Option<BatchRx<T>> {
        self.batch_rx.lock().take()
    }

    /// Returns a reference to the inner state.
    pub fn state(&self) -> RwLockReadGuard<StateMachine<T>> {
        self.inner.state.read()
    }

    /// Returns a mutable reference to the inner state.
    pub(crate) fn state_mut(&self) -> RwLockWriteGuard<StateMachine<T>> {
        self.inner.state.write()
    }

    /// Returns a recent state discriminant.
    ///
    /// The returned value may not be up to date and must be considered
    /// immediately stale.
    pub fn state_dsct_stale(&self) -> StateDsct {
        self.inner.state_dsct_stale.load(Ordering::Relaxed).into()
    }

    pub fn is_validator(&self) -> bool {
        self.state_dsct_stale() == StateDsct::Validator
    }

    /// Returns a reference to the peers list.
    pub fn peers(&self) -> RwLockReadGuard<Peers<T>> {
        self.inner.peers.read()
    }

    /// Returns a mutable reference to the peers list.
    pub(crate) fn peers_mut(&self) -> RwLockWriteGuard<Peers<T>> {
        self.inner.peers.write()
    }

    /// Sets the current epoch and returns the previous epoch.
    ///
    /// The returned value should (always?) be equal to `epoch - 1`.
    //
    // TODO: Convert to a simple incrementer?
    pub(crate) fn set_current_epoch(&self, epoch: u64) -> u64 {
        let mut ce = self.inner.current_epoch.lock();
        let prev_epoch = *ce;
        *ce = epoch;
        prev_epoch
    }

    /// Returns the epoch of the next batch to be output.
    pub fn current_epoch(&self) -> u64 {
        *self.inner.current_epoch.lock()
    }

    /// Returns a stream of epoch numbers (e) indicating that a batch has been
    /// output for an epoch (e - 1) and that a new epoch has begun.
    ///
    /// The current epoch will be sent upon registration. If a listener is
    /// registered before any batches have been output by this instance of
    /// Hydrabadger, the start epoch will be output.
    pub fn register_epoch_listener(&self) -> EpochRx {
        let (tx, rx) = mpsc::unbounded();
        if self.is_validator() {
            tx.unbounded_send(self.current_epoch())
                .expect("Unknown error: receiver can not have dropped");
        }
        self.inner.epoch_listeners.write().push(tx);
        rx
    }

    /// Returns a reference to the epoch listeners list.
    pub(crate) fn epoch_listeners(&self) -> RwLockReadGuard<Vec<EpochTx>> {
        self.inner.epoch_listeners.read()
    }

    /// Returns a reference to the config.
    pub(crate) fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Sends a message on the internal tx.
    pub(crate) fn send_internal(&self, msg: InternalMessage<T>) {
        if let Err(err) = self.inner.peer_internal_tx.unbounded_send(msg) {
            error!(
                "Unable to send on internal tx. Internal rx has dropped: {}",
                err
            );
            ::std::process::exit(-1)
        }
    }

    /// Handles a incoming batch of user transactions.
    pub fn propose_user_contribution(&self, txn: T) -> Result<(), Error> {
        if self.is_validator() {
            self.send_internal(InternalMessage::hb_contribution(
                self.inner.uid,
                OutAddr(*self.inner.addr),
                txn,
            ));
            Ok(())
        } else {
            Err(Error::ProposeUserContributionNotValidator)
        }
    }

    /// Casts a vote for a change in the validator set or configuration.
    pub fn vote_for(&self, change: Change) -> Result<(), Error> {
        if self.is_validator() {
            self.send_internal(InternalMessage::hb_vote(
                self.inner.uid,
                OutAddr(*self.inner.addr),
                change,
            ));
            Ok(())
        } else {
            Err(Error::VoteForNotValidator)
        }
    }

    /// Begins a synchronous distributed key generation instance and returns a
    /// stream which may be polled for events and messages.
    pub fn new_key_gen_instance(&self) -> mpsc::UnboundedReceiver<key_gen::Message> {
        let (tx, rx) = mpsc::unbounded();
        self.send_internal(InternalMessage::new_key_gen_instance(
            self.inner.uid,
            OutAddr(*self.inner.addr),
            tx,
        ));
        rx
    }

    /// Returns a future that handles incoming connections on `socket`.
    fn handle_incoming(self, socket: TcpStream) -> impl Future<Item = (), Error = ()> {
        info!("Incoming connection from '{}'", socket.peer_addr().unwrap());
        let wire_msgs = WireMessages::new(socket, self.inner.secret_key.clone());

        wire_msgs
            .into_future()
            .map_err(|(e, _)| e)
            .and_then(move |(msg_opt, w_messages)| {
                // let _hdb = self.clone();

                match msg_opt {
                    Some(msg) => match msg.into_kind() {
                        // The only correct entry point:
                        WireMessageKind::HelloRequestChangeAdd(peer_uid, peer_in_addr, peer_pk) => {
                            // Also adds a `Peer` to `self.peers`.
                            let peer_h = PeerHandler::new(
                                Some((peer_uid, peer_in_addr, peer_pk)),
                                self.clone(),
                                w_messages,
                            );

                            // Relay incoming `HelloRequestChangeAdd` message internally.
                            peer_h
                                .hdb()
                                .send_internal(InternalMessage::new_incoming_connection(
                                    peer_uid,
                                    *peer_h.out_addr(),
                                    peer_in_addr,
                                    peer_pk,
                                    true,
                                ));
                            Either::B(peer_h)
                        }
                        _ => {
                            // TODO: Return this as a future-error (handled below):
                            error!(
                                "Peer connected without sending \
                                 `WireMessageKind::HelloRequestChangeAdd`."
                            );
                            Either::A(future::ok(()))
                        }
                    },
                    None => {
                        // The remote client closed the connection without sending
                        // a welcome_request_change_add message.
                        Either::A(future::ok(()))
                    }
                }
            })
            .map_err(|err| error!("Connection error = {:?}", err))
    }

    /// Returns a future that connects to new peer.
    pub(super) fn connect_outgoing(
        self,
        remote_addr: SocketAddr,
        local_sk: SecretKey,
        pub_info: Option<(Uid, InAddr, PublicKey)>,
        is_optimistic: bool,
    ) -> impl Future<Item = (), Error = ()> {
        let uid = self.inner.uid;
        let in_addr = self.inner.addr;

        info!("Initiating outgoing connection to: {}", remote_addr);

        TcpStream::connect(&remote_addr)
            .map_err(Error::from)
            .and_then(move |socket| {
                let local_pk = local_sk.public_key();
                // Wrap the socket with the frame delimiter and codec:
                let mut wire_msgs = WireMessages::new(socket, local_sk);
                let wire_hello_result = wire_msgs.send_msg(WireMessage::hello_request_change_add(
                    uid, in_addr, local_pk,
                ));
                match wire_hello_result {
                    Ok(_) => {
                        let peer = PeerHandler::new(pub_info, self.clone(), wire_msgs);

                        self.send_internal(InternalMessage::new_outgoing_connection(
                            *peer.out_addr(),
                        ));

                        Either::A(peer)
                    }
                    Err(err) => Either::B(future::err(err)),
                }
            })
            .map_err(move |err| {
                if is_optimistic {
                    warn!(
                        "Unable to connect to: {} ({e:?}: {e})",
                        remote_addr,
                        e = err
                    );
                } else {
                    error!("Error connecting to: {} ({e:?}: {e})", remote_addr, e = err);
                }
            })
    }

    fn generate_contributions(
        self,
        gen_txns: Option<fn(usize, usize) -> T>,
    ) -> impl Future<Item = (), Error = ()> {
        if let Some(gen_txns) = gen_txns {
            let epoch_stream = self.register_epoch_listener();
            let gen_delay = self.inner.config.txn_gen_interval;
            let gen_cntrb = epoch_stream
                .and_then(move |epoch_no| {
                    Delay::new(Instant::now() + Duration::from_millis(gen_delay))
                        .map_err(|err| panic!("Timer error: {:?}", err))
                        .and_then(move |_| Ok(epoch_no))
                })
                .for_each(move |_epoch_no| {
                    let hdb = self.clone();

                    if let StateDsct::Validator = hdb.state_dsct_stale() {
                        info!(
                            "Generating and inputting {} random transactions...",
                            self.inner.config.txn_gen_count
                        );
                        // Send some random transactions to our internal HB instance.
                        let txns = gen_txns(
                            self.inner.config.txn_gen_count,
                            self.inner.config.txn_gen_bytes,
                        );

                        hdb.send_internal(InternalMessage::hb_contribution(
                            hdb.inner.uid,
                            OutAddr(*hdb.inner.addr),
                            txns,
                        ));
                    }
                    Ok(())
                })
                .map_err(|err| panic!("Contribution generation error: {:?}", err));

            Either::A(gen_cntrb)
        } else {
            Either::B(future::ok(()))
        }
    }

    /// Returns a future that generates random transactions and logs status
    /// messages.
    fn log_status(self) -> impl Future<Item = (), Error = ()> {
        Interval::new(
            Instant::now(),
            Duration::from_millis(self.inner.config.txn_gen_interval),
        )
        .for_each(move |_| {
            let hdb = self.clone();
            let peers = hdb.peers();

            // Log state:
            let dsct = hdb.state_dsct_stale();
            let peer_count = peers.count_total();
            info!("Hydrabadger State: {:?}({})", dsct, peer_count);

            // Log peer list:
            let peer_list = peers
                .peers()
                .map(|p| {
                    p.in_addr()
                        .map(|ia| ia.0.to_string())
                        .unwrap_or(format!("No in address"))
                })
                .collect::<Vec<_>>();
            info!("    Peers: {:?}", peer_list);

            // Log (trace) full peerhandler details:
            trace!("PeerHandler list:");
            for (peer_addr, _peer) in peers.iter() {
                trace!("     peer_addr: {}", peer_addr);
            }

            drop(peers);

            Ok(())
        })
        .map_err(|err| panic!("List connection interval error: {:?}", err))
    }

    /// Binds to a host address and returns a future which starts the node.
    pub fn node(
        self,
        remotes: Option<HashSet<SocketAddr>>,
        gen_txns: Option<fn(usize, usize) -> T>,
    ) -> impl Future<Item = (), Error = ()> {
        let socket = TcpListener::bind(&self.inner.addr).unwrap();
        info!("Listening on: {}", self.inner.addr);

        let remotes = remotes.unwrap_or_default();

        let hdb = self.clone();
        let listen = socket
            .incoming()
            .map_err(|err| error!("Error accepting socket: {:?}", err))
            .for_each(move |socket| {
                tokio::spawn(hdb.clone().handle_incoming(socket));
                Ok(())
            });

        let hdb = self.clone();
        let local_sk = hdb.inner.secret_key.clone();
        let connect = future::lazy(move || {
            for &remote_addr in remotes.iter().filter(|&&ra| ra != hdb.inner.addr.0) {
                tokio::spawn(hdb.clone().connect_outgoing(
                    remote_addr,
                    local_sk.clone(),
                    None,
                    true,
                ));
            }
            Ok(())
        });

        let hdb_handler = self
            .handler()
            .map_err(|err| error!("Handler internal error: {:?}", err));

        let log_status = self.clone().log_status();
        let generate_contributions = self.clone().generate_contributions(gen_txns);

        listen
            .join5(connect, hdb_handler, log_status, generate_contributions)
            .map(|(..)| ())
    }

    /// Starts a node.
    pub fn run_node(
        self,
        remotes: Option<HashSet<SocketAddr>>,
        gen_txns: Option<fn(usize, usize) -> T>,
    ) {
        tokio::run(self.node(remotes, gen_txns));
    }

    pub fn addr(&self) -> &InAddr {
        &self.inner.addr
    }

    pub fn uid(&self) -> &Uid {
        &self.inner.uid
    }

    pub fn secret_key(&self) -> &SecretKey {
        &self.inner.secret_key
    }

    pub fn to_weak(&self) -> HydrabadgerWeak<T> {
        HydrabadgerWeak {
            inner: Arc::downgrade(&self.inner),
            handler: Arc::downgrade(&self.handler),
            batch_rx: Arc::downgrade(&self.batch_rx),
        }
    }
}

pub struct HydrabadgerWeak<T: Contribution> {
    inner: Weak<Inner<T>>,
    handler: Weak<Mutex<Option<Handler<T>>>>,
    batch_rx: Weak<Mutex<Option<BatchRx<T>>>>,
}

impl<T: Contribution> HydrabadgerWeak<T> {
    pub fn upgrade(self) -> Option<Hydrabadger<T>> {
        self.inner.upgrade().and_then(|inner| {
            self.handler.upgrade().and_then(|handler| {
                self.batch_rx.upgrade().and_then(|batch_rx| {
                    Some(Hydrabadger {
                        inner,
                        handler,
                        batch_rx,
                    })
                })
            })
        })
    }
}
