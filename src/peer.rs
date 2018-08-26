//! A peer network node.

#![allow(unused_imports, dead_code, unused_variables, unused_mut)]

use std::{
    collections::{
        hash_map::{Iter as HashMapIter, Values as HashMapValues},
        HashMap,
    },
    borrow::Borrow,
};
use futures::sync::mpsc;
use tokio::prelude::*;
use hbbft::crypto::PublicKey;
use hbbft::queueing_honey_badger::{Input as HbInput};
use ::{InternalMessage, WireMessage, WireMessageKind, WireMessages, WireTx, WireRx,
    OutAddr, InAddr, Uid};
use hydrabadger::{Hydrabadger, Error,};


/// The state for each connected client.
pub struct PeerHandler {
    // Peer uid.
    uid: Option<Uid>,

    // The incoming stream of messages:
    wire_msgs: WireMessages,

    /// Handle to the shared message state.
    hdb: Hydrabadger,

    // TODO: Consider adding back a separate clone of `peer_internal_tx`. Is
    // there any difference if capacity isn't an issue? -- doubtful

    /// Receive half of the message channel.
    rx: WireRx,

    /// Peer socket address.
    out_addr: OutAddr,
}

impl PeerHandler {
    /// Create a new instance of `Peer`.
    pub fn new(pub_info: Option<(Uid, InAddr, PublicKey)>,
            hdb: Hydrabadger, wire_msgs: WireMessages) -> PeerHandler {
        // Get the client socket address
        let out_addr = OutAddr(wire_msgs.socket().peer_addr().unwrap());

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded();

        let uid = pub_info.as_ref().map(|(uid, _, _)| uid.clone());

        // Add an entry for this `Peer` in the shared state map.
        hdb.peers_mut().add(out_addr, tx, pub_info);

        PeerHandler {
            uid,
            wire_msgs,
            hdb,
            rx,
            out_addr,
        }
    }

    pub(crate) fn hdb(&self) -> &Hydrabadger {
        &self.hdb
    }

    pub(crate) fn out_addr(&self) -> &OutAddr {
        &self.out_addr
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
                    WireMessageKind::HelloRequestChangeAdd(src_uid, _in_addr, _pub_key) => {
                        error!("Duplicate `WireMessage::HelloRequestChangeAdd` \
                            received from '{}'", src_uid);
                    },
                    WireMessageKind::WelcomeReceivedChangeAdd(src_uid, pk, net_state) => {
                        self.uid = Some(src_uid);
                        self.hdb.send_internal(
                            InternalMessage::wire(Some(src_uid), self.out_addr,
                                WireMessage::welcome_received_change_add(src_uid, pk, net_state)
                            )
                        );
                    },
                    WireMessageKind::Message(src_uid, msg) => {
                        // let uid = self.uid.clone()
                        //     .expect("`WireMessageKind::Message` received before \
                        //         establishing peer");

                        if let Some(peer_uid) = self.uid.as_ref() {
                            debug_assert_eq!(src_uid, *peer_uid);
                        }

                        self.hdb.send_internal(
                            InternalMessage::hb_message(src_uid, self.out_addr, msg)
                        )
                    },
                    WireMessageKind::Transactions(src_uid, txns) => {
                        if let Some(peer_uid) = self.uid.as_ref() {
                            debug_assert_eq!(src_uid, *peer_uid);
                        }

                        self.hdb.send_internal(
                            InternalMessage::hb_input(src_uid, self.out_addr, HbInput::User(txns))
                        )
                    },
                    kind @ _ => {
                        self.hdb.send_internal(InternalMessage::wire(self.uid.clone(),
                            self.out_addr, kind.into()))
                    }
                }
            } else {
                // EOF was reached. The remote client has disconnected. There is
                // nothing more to do.
                warn!("Peer ({}: '{}') disconnected.", self.out_addr, self.uid.clone().unwrap());
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
            self.out_addr, self.uid.clone().unwrap());
        // Remove peer transmitter from the lists:
        self.hdb.peers_mut().remove(&self.out_addr);

        if let Some(uid) = self.uid.clone() {
            debug!("Sending peer ({}: '{}') disconnect internal message.",
                self.out_addr, self.uid.clone().unwrap());

            self.hdb.send_internal(InternalMessage::peer_disconnect(
                uid, self.out_addr));
        }
    }
}


#[derive(Clone, Debug)]
#[allow(dead_code)]
enum State {
    Handshaking,
    PendingJoinInfo {
        uid: Uid,
        in_addr: InAddr,
        pk: PublicKey,
    },
    EstablishedObserver {
        uid: Uid,
        in_addr: InAddr,
        pk: PublicKey,
    },
    EstablishedValidator {
        uid: Uid,
        in_addr: InAddr,
        pk: PublicKey,
    },
}


/// Nodes of the network.
#[derive(Clone, Debug)]
pub struct Peer {
    out_addr: OutAddr,
    tx: WireTx,
    state: State,
}

impl Peer {
    /// Returns a new `Peer`
    fn new(out_addr: OutAddr, tx: WireTx,
            // uid: Option<Uid>, in_addr: Option<InAddr>, pk: Option<PublicKey>
            pub_info: Option<(Uid, InAddr, PublicKey)>,
            ) -> Peer {
        // assert!(uid.is_some() == in_addr.is_some() && uid.is_some() == pk.is_some());
        let state = match pub_info {
            None => State::Handshaking,
            Some((uid, in_addr, pk)) => State::EstablishedValidator { uid, in_addr, pk },
        };

        Peer {
            out_addr,
            tx,
            state,
        }
    }

    /// Sets a peer state to `State::PendingJoinInfo` and stores public info.
    fn set_pending(&mut self, pub_info: (Uid, InAddr, PublicKey)) {
        self.state = match self.state {
            State::Handshaking => {
                State::PendingJoinInfo {
                    uid: pub_info.0,
                    in_addr: pub_info.1,
                    pk: pub_info.2
                }
            },
            _ => panic!("Peer::set_pending: Can only set pending when \
                peer state is `Handshaking`."),
        };
    }

    /// Sets a peer state to `State::EstablishedObserver` and stores public info.
    fn establish_observer(&mut self) {
        self.state = match self.state {
            State::PendingJoinInfo { uid, in_addr, pk } => {
                State::EstablishedObserver {
                    uid,
                    in_addr,
                    pk,
                }
            },
            _ => panic!("Peer::establish_observer: Can only establish observer when \
                peer state is`PendingJoinInfo`."),
        };
    }

    /// Sets a peer state to `State::EstablishedValidator` and stores public info.
    fn establish_validator(&mut self, pub_info: Option<(Uid, InAddr, PublicKey)>) {
        self.state = match self.state {
            State::Handshaking => match pub_info {
                Some(pi) => {
                    State::EstablishedValidator {
                        uid: pi.0,
                        in_addr: pi.1,
                        pk: pi.2
                    }
                },
                None => {
                    panic!("Peer::establish_validator: `pub_info` must be supplied \
                        when establishing a validator from `Handshaking`.");
                },
            },
            State::EstablishedObserver { uid, in_addr, pk } => {
                if let Some(_) = pub_info {
                    panic!("Peer::establish_validator: `pub_info` must be `None` \
                        when upgrading an observer node.");
                }
                State::EstablishedValidator {
                    uid,
                    in_addr,
                    pk,
                }
            },
            _ => panic!("Peer::establish_validator: Can only establish validator when \
                peer state is`Handshaking` or `EstablishedObserver`."),
        };
    }

    /// Returns the peer's unique identifier.
    pub fn uid(&self) -> Option<&Uid> {
        match self.state {
            State::Handshaking => None,
            State::PendingJoinInfo { ref uid, .. } => Some(uid),
            State::EstablishedObserver { ref uid, ..  } => Some(uid),
            State::EstablishedValidator { ref uid, .. } => Some(uid),
        }
    }

    /// Returns the peer's unique identifier.
    pub fn out_addr(&self) -> &OutAddr {
        &self.out_addr
    }

    /// Returns the peer's public key.
    pub fn public_key(&self) -> Option<&PublicKey> {
        match self.state {
            State::Handshaking => None,
            State::PendingJoinInfo { ref pk, .. } => Some(pk),
            State::EstablishedObserver { ref pk, .. } => Some(pk),
            State::EstablishedValidator { ref pk, .. } => Some(pk),
        }
    }

    /// Returns the peer's incoming (listening) socket address.
    pub fn in_addr(&self) -> Option<&InAddr> {
        match self.state {
            State::Handshaking => None,
            State::PendingJoinInfo { ref in_addr, .. } => Some(in_addr),
            State::EstablishedObserver { ref in_addr, .. } => Some(in_addr),
            State::EstablishedValidator { ref in_addr, .. } => Some(in_addr),
        }
    }

    /// Returns the peer's public info if established.
    pub fn pub_info(&self) -> Option<(&Uid, &InAddr, &PublicKey)> {
        match self.state {
            State::Handshaking => None,
            State::EstablishedObserver { ref uid, ref in_addr, ref pk } => Some((uid, in_addr, pk)),
            State::PendingJoinInfo { ref uid, ref in_addr, ref pk } => Some((uid, in_addr, pk)),
            State::EstablishedValidator { ref uid, ref in_addr, ref pk } => Some((uid, in_addr, pk)),
        }
    }

    /// Returns true if this peer is pending.
    pub fn is_pending(&self) -> bool {
        match self.state {
            State::PendingJoinInfo { .. } => true,
            _ => false,
        }
    }

    /// Returns true if this peer is an established observer.
    pub fn is_observer(&self) -> bool {
        match self.state {
            State::EstablishedObserver { .. } => true,
            _ => false,
        }
    }

    /// Returns true if this peer is an established validator.
    pub fn is_validator(&self) -> bool {
        match self.state {
            State::EstablishedValidator { .. } => true,
            _ => false,
        }
    }

    /// Returns the peer's wire transmitter.
    pub fn tx(&self) -> &WireTx {
        &self.tx
    }
}


/// Peer nodes of the network.
//
// TODO: Keep a separate `HashSet` of validator `OutAddrs` to avoid having to
// iterate through entire list.
#[derive(Debug)]
pub(crate) struct Peers {
    peers: HashMap<OutAddr, Peer>,
    out_addrs: HashMap<Uid, OutAddr>,
}

impl Peers {
    /// Returns a new empty list of peers.
    pub(crate) fn new() -> Peers {
        Peers {
            peers: HashMap::with_capacity(64),
            out_addrs: HashMap::with_capacity(64),
        }
    }

    /// Adds a peer to the list.
    pub(crate) fn add(&mut self, out_addr: OutAddr, tx: WireTx,
            // uid: Option<Uid>, in_addr: Option<InAddr>, pk: Option<PublicKey>
            pub_info: Option<(Uid, InAddr, PublicKey)>,
            ) {
        let peer = Peer::new(out_addr, tx, pub_info);
        if let State::EstablishedValidator { uid, .. } = peer.state {
            self.out_addrs.insert(uid, peer.out_addr);
        }
        self.peers.insert(peer.out_addr, peer);
    }

    /// Attempts to set peer as pending-join-info, storing `pub_info`.
    ///
    /// Returns `true` if the peer was already pending.
    ///
    /// ### Panics
    ///
    /// Peer state must be `Handshaking`.
    ///
    /// TODO: Error handling...
    pub(crate) fn set_pending<O: Borrow<OutAddr>>(&mut self, out_addr: O,
            pub_info: (Uid, InAddr, PublicKey)) -> bool {
        let peer = self.peers.get_mut(out_addr.borrow())
            .expect(&format!("Peers::set_pending: \
                No peer found with outgoing address: {}", out_addr.borrow()));
        match self.out_addrs.insert(pub_info.0, *out_addr.borrow()) {
            Some(_out_addr_pub) => {
                let pi_pub = peer.pub_info()
                    .expect("Peers::set_pending: internal consistency error");
                assert!(pub_info.0 == *pi_pub.0 && pub_info.1 == *pi_pub.1 && pub_info.2 == *pi_pub.2);
                assert!(peer.is_validator());
                return true;
            },
            None => peer.set_pending(pub_info),
        }

        // false
        panic!("Peer::set_pending: Do not use yet.");
    }

    /// Attempts to establish a peer as an observer.
    ///
    /// ### Panics
    ///
    /// Peer state must be `Handshaking`.
    ///
    /// TODO: Error handling...
    pub(crate) fn establish_observer<O: Borrow<OutAddr>>(&mut self, out_addr: O) {
        let peer = self.peers.get_mut(out_addr.borrow())
            .expect(&format!("Peers::establish_observer: \
                No peer found with outgoing address: {}", out_addr.borrow()));

        // peer.establish_observer()
        panic!("Peer::set_pending: Do not use yet.");
    }

    /// Attempts to establish a peer as a validator, storing `pub_info`.
    ///
    /// Returns `true` if the peer was already an established validator.
    ///
    /// ### Panics
    ///
    /// Peer state must be `Handshaking` or `EstablishedObserver`.
    ///
    /// TODO: Error handling...
    pub(crate) fn establish_validator<O: Borrow<OutAddr>>(&mut self, out_addr: O,
            pub_info: (Uid, InAddr, PublicKey)) -> bool {
        let peer = self.peers.get_mut(out_addr.borrow())
            .expect(&format!("Peers::establish_validator: \
                No peer found with outgoing address: {}", out_addr.borrow()));
        match self.out_addrs.insert(pub_info.0, *out_addr.borrow()) {
            Some(_out_addr_pub) => {
                let pi_pub = peer.pub_info()
                    .expect("Peers::establish_validator: internal consistency error");
                assert!(pub_info.0 == *pi_pub.0 && pub_info.1 == *pi_pub.1 && pub_info.2 == *pi_pub.2);
                assert!(peer.is_validator());
                return true;
            },
            None => peer.establish_validator(Some(pub_info)),
        }
        false
    }

    /// Removes a peer the list if it exists.
    pub(crate) fn remove<O: Borrow<OutAddr>>(&mut self, out_addr: O) {
        let peer = self.peers.remove(out_addr.borrow());
        if let Some(p) = peer {
            if let Some(uid) = p.uid() {
                self.out_addrs.remove(&uid);
            }
        }
    }

    pub(crate) fn get<O: Borrow<OutAddr>>(&self, out_addr: O) -> Option<&Peer> {
        self.peers.get(out_addr.borrow())
    }

    pub(crate) fn get_by_uid<U: Borrow<Uid>>(&self, uid: U) -> Option<&Peer> {
        // self.peers.get()
        self.out_addrs.get(uid.borrow()).and_then(|addr| self.get(addr))
    }

    /// Returns an Iterator over the list of peers.
    pub(crate) fn iter(&self) -> HashMapIter<OutAddr, Peer> {
        self.peers.iter()
    }

    /// Returns an Iterator over the list of peers.
    pub(crate) fn peers(&self) -> HashMapValues<OutAddr, Peer> {
        self.peers.values()
    }

    /// Returns an iterator over the list of validators.
    pub(crate) fn validators(&self) -> impl Iterator<Item = &Peer> {
        self.peers.values().filter(|p| p.is_validator())
    }

    /// Returns the current number of connected peers.
    pub(crate) fn count_total(&self) -> usize {
        self.peers.len()
    }

    /// Returns the current number of connected and established validators.
    ///
    /// This is semi-expensive (O(n)).
    pub(crate) fn count_validators(&self) -> usize {
        self.validators().count()
    }

    pub(crate) fn contains_in_addr<I: Borrow<InAddr>>(&self, in_addr: I) -> bool {
        for peer in self.peers.values() {
            if let Some(peer_in_addr) = peer.in_addr() {
                if peer_in_addr == in_addr.borrow() {
                    return true;
                }
            }
        }
        false
    }
}
