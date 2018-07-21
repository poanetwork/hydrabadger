//! A hydrabadger consensus node.
//!

// #![allow(unused_imports, dead_code, unused_variables, unused_mut, unused_assignments,
//     unreachable_code)]

use std::{
    time::{Duration, Instant},
    sync::Arc,
    collections::HashSet,
    net::SocketAddr,
};
use futures::{
    sync::mpsc,
    future::{self, Either},
};
use tokio::{
    self,
    net::{TcpListener, TcpStream},
    timer::Interval,
    prelude::*,
};
use rand::{self, Rand};
use parking_lot::{RwLock, Mutex, RwLockReadGuard, RwLockWriteGuard};
use hbbft::{
    crypto::{PublicKey, SecretKey},
    queueing_honey_badger::{Input as QhbInput},
};
use peer::{PeerHandler, Peers};
use ::{InternalMessage, WireMessage, WireMessageKind, WireMessages,
    OutAddr, InAddr,  Uid, InternalTx, Transaction};
use super::{Error, State, Handler};
use super::{TXN_BYTES, NEW_TXN_INTERVAL_MS, NEW_TXNS_PER_INTERVAL};



/// The `Arc` wrapped portion of `Hydrabadger`.
///
/// Shared all over the place.
struct Inner {
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
}


/// A `HoneyBadger` network node.
#[derive(Clone)]
pub struct Hydrabadger {
    inner: Arc<Inner>,
    handler: Arc<Mutex<Option<Handler>>>,
}

impl Hydrabadger {
    /// Returns a new Hydrabadger node.
    pub fn new(addr: SocketAddr) -> Self {
        let uid = Uid::new();
        let secret_key = SecretKey::rand(&mut rand::thread_rng());

        let (peer_internal_tx, peer_internal_rx) = mpsc::unbounded();

        let inner = Arc::new(Inner {
            uid,
            addr: InAddr(addr),
            secret_key,
            peers: RwLock::new(Peers::new()),
            state: RwLock::new(State::disconnected()),
            peer_internal_tx,
        });

        let hdb = Hydrabadger {
            inner,
            handler: Arc::new(Mutex::new(None)),
        };

        *hdb.handler.lock() = Some(Handler::new(hdb.clone(), peer_internal_rx));

        hdb
    }

    /// Returns the pre-created handler.
    pub fn handler(&self) -> Option<Handler> {
        self.handler.lock().take()
    }

    /// Returns a reference to the inner state.
    pub(crate) fn state(&self) -> RwLockReadGuard<State> {
        let state = self.inner.state.read();
        state
    }

    /// Returns a mutable reference to the inner state.
    pub(crate) fn state_mut(&self) -> RwLockWriteGuard<State> {
        let state = self.inner.state.write();
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
        if let Err(err) = self.inner.peer_internal_tx.unbounded_send(msg) {
            error!("Unable to send on internal tx. Internal rx has dropped: {}", err);
        }
    }

    /// Returns a future that handles incoming connections on `socket`.
    fn handle_incoming(self, socket: TcpStream)
            -> impl Future<Item = (), Error = ()> {
        info!("Incoming connection from '{}'", socket.peer_addr().unwrap());
        let wire_msgs = WireMessages::new(socket);

        wire_msgs.into_future()
            .map_err(|(e, _)| e)
            .and_then(move |(msg_opt, w_messages)| {
                // let _hdb = self.clone();

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
    pub(super) fn connect_outgoing(self, remote_addr: SocketAddr, local_pk: PublicKey,
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
        Interval::new(Instant::now(), Duration::from_millis(NEW_TXN_INTERVAL_MS))
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
                for (peer_addr, _peer) in peers.iter() {
                    trace!("     peer_addr: {}", peer_addr); }

                trace!("::node: Locking 'state' for reading...");
                if let Some(_) = &hdb.state().qhb() {
                    trace!("::node: 'state' locked for reading.");

                    // // If no other nodes are connected, panic:
                    // if qhb.dyn_hb().netinfo().num_nodes() > HB_PEER_MINIMUM_COUNT {
                    //     info!("Generating and inputting {} random transactions...", NEW_TXNS_PER_INTERVAL);
                    //     // Send some random transactions to our internal HB instance.
                    //     let txns: Vec<_> = (0..NEW_TXNS_PER_INTERVAL).map(|_| {
                    //         Transaction::random(TXN_BYTES)
                    //     }).collect();

                    //     hdb.send_internal(
                    //         InternalMessage::hb_input(hdb.inner.uid, OutAddr(*hdb.inner.addr), QhbInput::User(txns))
                    //     );
                    // } else {
                    //     error!("Not enough nodes connected. Remote peer count has dropped \
                    //         below threshold ({}). No", HB_PEER_MINIMUM_COUNT);
                    //     // panic!("Invalid state");
                    // }

                    info!("Generating and inputting {} random transactions...", NEW_TXNS_PER_INTERVAL);
                    // Send some random transactions to our internal HB instance.
                    let txns: Vec<_> = (0..NEW_TXNS_PER_INTERVAL).map(|_| {
                        Transaction::random(TXN_BYTES)
                    }).collect();

                    hdb.send_internal(
                        InternalMessage::hb_input(hdb.inner.uid, OutAddr(*hdb.inner.addr), QhbInput::User(txns))
                    );
                }
                trace!("::node: 'state' unlocked for reading.");

                Ok(())
            })
            .map_err(|err| error!("List connection inverval error: {:?}", err))
    }

    /// Binds to a host address and returns a future which starts the node.
    pub fn node(self, remotes: HashSet<SocketAddr>)
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
            .map_err(|err| error!("Handler internal error: {:?}", err));

        listen.join4(connect, generate_txns, hdb_handler).map(|(_, _, _, _)| ())
    }

    /// Starts a node.
    pub fn run_node(self, remotes: HashSet<SocketAddr>) {
        tokio::run(self.node(remotes));
    }

    pub fn addr(&self) -> &InAddr {
        &self.inner.addr
    }

    pub fn uid(&self) -> &Uid {
        &self.inner.uid
    }

    pub(super) fn secret_key(&self) -> &SecretKey {
        &self.inner.secret_key
    }
}
