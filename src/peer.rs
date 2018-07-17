//! A peer network node.

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
use ::{
    hydrabadger::{
		Hydrabadger, InternalMessage, WireMessage, WireMessageKind, WireMessages, WireTx, WireRx,
		OutAddr, InAddr, NetworkState, Error,
	},
};




/// The state for each connected client.
pub struct PeerHandler {
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
    out_addr: OutAddr,
}

impl PeerHandler {
    /// Create a new instance of `Peer`.
    pub fn new(full_info: Option<(Uuid, InAddr, PublicKey)>,
            mut hdb: Hydrabadger, wire_msgs: WireMessages) -> PeerHandler {
        // Get the client socket address
        let out_addr = OutAddr(wire_msgs.socket().peer_addr().unwrap());

        // Create a channel for this peer
        let (tx, rx) = mpsc::unbounded();

        let uid = full_info.as_ref().map(|(uid, _, _)| uid.clone());

        // Add an entry for this `Peer` in the shared state map.
        let guard = hdb.peers_mut().add(out_addr, tx, full_info);

        PeerHandler {
            uid,
            wire_msgs,
            hdb,
            rx,
            out_addr,
        }
    }

    // /// Sends a message to all connected peers.
    // fn wire_to_all(&mut self, msg: &WireMessage) {
    //     // Now, send the message to all other peers
    //     for (p_addr, peer) in self.hdb.peers().iter() {
    //         // Don't send the message to ourselves
    //         if *p_addr != self.out_addr {
    //             // The send only fails if the rx half has been dropped,
    //             // however this is impossible as the `tx` half will be
    //             // removed from the map before the `rx` is dropped.
    //             peer.tx.unbounded_send(msg.clone()).unwrap();
    //         }
    //     }
    // }

    // /// Sends a hello response (welcome).
    // pub(crate) fn wire_welcome_received_change_add(&self, net_state: NetworkState) {
    //     self.hdb.peers().get(&self.out_addr).unwrap()
    //         .tx.unbounded_send(WireMessage::welcome_received_change_add(self.uid.clone().unwrap(), net_state))
    //         .unwrap();
    // }

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
                    WireMessageKind::HelloRequestChangeAdd(src_uid, in_addr, _pub_key) => {
                        error!("Duplicate `WireMessage::HelloRequestChangeAdd` \
                            received from '{}'", src_uid);
                    },
                    WireMessageKind::WelcomeReceivedChangeAdd(src_uid, net_state) => {
                        self.uid = Some(src_uid);
                        self.hdb.send_internal(
                            InternalMessage::wire(src_uid, self.out_addr,
                                WireMessage::welcome_received_change_add(src_uid, net_state)
                            )
                        );
                    },
                    WireMessageKind::Message(msg) => {
                        let uid = self.uid.clone()
                            .expect("`WireMessageKind::Message` received before \
                                `WireMessageKind::WelcomeReceivedChangeAdd`");
                        self.hdb.send_internal(
                            InternalMessage::incoming_hb_message(uid, self.out_addr, msg)
                        )
                    },
                    _ => unimplemented!(),
                }
            } else {
                // EOF was reached. The remote client has disconnected. There is
                // nothing more to do.
                info!("Peer ({}: '{}') disconnected.", self.out_addr, self.uid.clone().unwrap());
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

        // // FIXME: Consider simply sending the 'change' input through the
        // // internal channel.
        // self.hdb.qhb.write().input(Input::Change(Change::Remove(self.uid)))
        //     .expect("Error adding new peer to HB");
        if let Some(uid) = self.uid.clone() {
            debug!("Sending peer ({}: '{}') disconnect internal message.",
                self.out_addr, self.uid.clone().unwrap());

            // self.hdb.peer_internal_tx.unbounded_send(InternalMessage::input(
            //     uid, self.out_addr, Input::Change(Change::Remove(uid)))).unwrap();

            self.hdb.send_internal(InternalMessage::peer_disconnect(
                uid, self.out_addr));
        }
    }
}


#[derive(Clone, Debug)]
enum State {
	Handshaking,
	Established {
		uid: Uuid,
	    in_addr: InAddr,
	    pk: PublicKey,
	},
}


/// Nodes of the network.
#[derive(Clone, Debug)]
pub struct Peer {
    out_addr: OutAddr,
    tx: WireTx,
    // uid: Option<Uuid>,
    // in_addr: Option<InAddr>,
    // pk: Option<PublicKey>,
    state: State,
}

impl Peer {
    /// Returns a new `Peer`
    fn new(out_addr: OutAddr, tx: WireTx,
    		// uid: Option<Uuid>, in_addr: Option<InAddr>, pk: Option<PublicKey>
            full_info: Option<(Uuid, InAddr, PublicKey)>,
            ) -> Peer {
    	// assert!(uid.is_some() == in_addr.is_some() && uid.is_some() == pk.is_some());
    	let state = match full_info {
    		None => State::Handshaking,
    		Some((uid, in_addr, pk)) => State::Established { uid, in_addr, pk },
    	};

        Peer {
        	out_addr,
        	tx,
        	state,
        }
    }

    /// Returns the peer's unique identifier.
    pub fn uid(&self) -> Option<&Uuid> {
    	match self.state {
    		State::Handshaking => None,
    		State::Established { ref uid, .. } => Some(uid),
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
    		State::Established { ref pk, .. } => Some(pk),
    	}
    }

    /// Returns the peer's incoming (listening) socket address.
    pub fn in_addr(&self) -> Option<&InAddr> {
    	match self.state {
    		State::Handshaking => None,
    		State::Established { ref in_addr, .. } => Some(in_addr),
    	}
    }

    /// Returns the peer's wire transmitter.
    pub fn tx(&self) -> &WireTx {
    	&self.tx
    }
}


/// Peer nodes of the network.
#[derive(Debug)]
pub(crate) struct Peers {
    peers: HashMap<OutAddr, Peer>,
    out_addrs: HashMap<Uuid, OutAddr>,
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
    		// uid: Option<Uuid>, in_addr: Option<InAddr>, pk: Option<PublicKey>
    		full_info: Option<(Uuid, InAddr, PublicKey)>,
    		) {
        let peer = Peer::new(out_addr, tx, full_info);
        if let State::Established { uid, .. } = peer.state {
            self.out_addrs.insert(uid, peer.out_addr);
        }
        self.peers.insert(peer.out_addr, peer);
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

    pub(crate) fn get_by_uid<U: Borrow<Uuid>>(&self, uid: U) -> Option<&Peer> {
        // self.peers.get()
        self.out_addrs.get(uid.borrow()).and_then(|addr| self.get(addr))
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
