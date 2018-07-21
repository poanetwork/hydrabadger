
use crossbeam::sync::SegQueue;
use tokio::{
    self,
    prelude::*,
};
use hbbft::{
    crypto::{
        PublicKey,
    },
    sync_key_gen::{ Part, PartOutcome, Ack},
    messaging::{DistAlgorithm, Target, },
    queueing_honey_badger::{Input as QhbInput,
        Change},
};
use peer::Peers;
use ::{InternalMessage, InternalMessageKind, WireMessage, WireMessageKind,
    OutAddr, InAddr, Uid, NetworkState, InternalRx, Step};
use super::{Hydrabadger, Error, State, StateDsct,};
use super::{HB_PEER_MINIMUM_COUNT, WIRE_MESSAGE_RETRY_MAX};




/// Hydrabadger event (internal message) handler.
pub struct Handler {
    hdb: Hydrabadger,
    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_rx: InternalRx,
    // Outgoing wire message queue:
    wire_queue: SegQueue<(Uid, WireMessage, usize)>,
    // Output from HoneyBadger:
    step_queue: SegQueue<Step>,
}

impl Handler {
    pub(super) fn new(hdb: Hydrabadger, peer_internal_rx: InternalRx) -> Handler {
         Handler {
            hdb,
            peer_internal_rx,
            wire_queue: SegQueue::new(),
            step_queue: SegQueue::new(),
        }
    }

    fn wire_to_all(&self, msg: WireMessage, peers: &Peers) {
        for (_p_addr, peer) in peers.iter()
                .filter(|(&p_addr, _)| p_addr != OutAddr(self.hdb.addr().0)) {
            peer.tx().unbounded_send(msg.clone()).unwrap();
        }
    }

    fn wire_to_validators(&self, msg: WireMessage, peers: &Peers) {
        for peer in peers.validators()
                .filter(|p| p.out_addr() != &OutAddr(self.hdb.addr().0)) {
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

    fn handle_new_established_peer(&self, src_uid: Uid, _src_addr: OutAddr, src_pk: PublicKey,
            state: &mut State, peers: &Peers) {
        match state.discriminant() {
            StateDsct::Disconnected | StateDsct::DeterminingNetworkState => {
                // panic!("Handler::handle_new_established_peer: \
                //     Received `WireMessageKind::WelcomeRequestChangeAdd` or \
                //     `InternalMessageKind::NewIncomingConnection` while \
                //     `StateDsct::Disconnected` or `DeterminingNetworkState`.");
                state.update_peer_connection_added(&peers);
                self.hdb.set_state_discriminant(state.discriminant());
            },
            StateDsct::ConnectedAwaitingMorePeers => {
                if peers.count_validators() >= HB_PEER_MINIMUM_COUNT {
                    info!("== BEGINNING KEY GENERATION ==");

                    let local_uid = *self.hdb.uid();
                    let local_in_addr = *self.hdb.addr();
                    let local_sk = self.hdb.secret_key().public_key();

                    let (part, ack) = state.set_generating_keys(&local_uid,
                        self.hdb.secret_key().clone(), peers);
                    self.hdb.set_state_discriminant(state.discriminant());

                    info!("KEY GENERATION: Sending initial parts and our own ack.");
                    self.wire_to_validators(
                        WireMessage::hello_from_validator(
                            local_uid, local_in_addr, local_sk, state.network_state(&peers)),
                        peers);
                    self.wire_to_validators(WireMessage::key_gen_part(part), peers);
                    self.wire_to_validators(WireMessage::key_gen_part_ack(ack), peers);
                }
            },
            StateDsct::ConnectedGeneratingKeys { .. } => {
                // This *could* be called multiple times when initially
                // establishing outgoing connections. Do nothing for now.
                debug!("Handler::handle_new_established_peer: Ignoring new established \
                    peer signal while `StateDsct::ConnectedGeneratingKeys`.");
            },
            StateDsct::ConnectedObserver | StateDsct::ConnectedValidator => {
                let qhb = state.qhb_mut().unwrap();
                info!("Change-Adding ('{}') to honey badger.", src_uid);
                let step = qhb.input(QhbInput::Change(Change::Add(src_uid, src_pk)))
                    .expect("Error adding new peer to HB");
                self.step_queue.push(step);
            },
        }
    }

    fn handle_key_gen_part(&self, src_uid: &Uid, part: Part, state: &mut State) {
        match state {
            State::ConnectedGeneratingKeys { ref mut sync_key_gen, ref mut ack_count, .. } => {
                // TODO: Move this match block into a function somewhere for re-use:
                info!("KEY GENERATION: Processing part from {}...", src_uid);
                let ack = match sync_key_gen.as_mut().unwrap().handle_part(src_uid, part) {
                    Some(PartOutcome::Valid(ack)) => ack,
                    Some(PartOutcome::Invalid(faults)) => panic!("Invalid part \
                        (FIXME: handle): {:?}", faults),
                    None => {
                        error!("`QueueingHoneyBadger::handle_part` returned `None`.");
                        return;
                    }
                };

                // Handle our own ack (TODO: Move this for re-use):
                let fault_log = sync_key_gen.as_mut().unwrap().handle_ack(src_uid, ack.clone());
                if !fault_log.is_empty() {
                    error!("Errors acknowledging part:\n {:?}", fault_log);
                }
                *ack_count += 1;

                let peers = self.hdb.peers();
                info!("KEY GENERATION: Part from '{}' acknowledged. Broadcasting...", src_uid);
                self.wire_to_validators(WireMessage::key_gen_part_ack(ack), &peers);
            }
            _ => panic!("::handle_key_gen_part: State must be `ConnectedGeneratingKeys`."),
        }
    }

    fn handle_key_gen_part_ack(&self, src_uid: &Uid, ack: Ack, state: &mut State, peers: &Peers) {
        let mut complete = false;
        match state {
            State::ConnectedGeneratingKeys { ref mut sync_key_gen, ref mut ack_count, .. } => {
                let mut sync_key_gen = sync_key_gen.as_mut().unwrap();
                info!("KEY GENERATION: Processing ack from '{}'...", src_uid);
                let fault_log = sync_key_gen.handle_ack(src_uid, ack);
                if !fault_log.is_empty() {
                    error!("Errors acknowledging part:\n {:?}", fault_log);
                }
                *ack_count += 1;

                debug!("   Peers complete: {}", sync_key_gen.count_complete());
                debug!("   Part acks: {}", ack_count);

                const NODE_N: usize = HB_PEER_MINIMUM_COUNT + 1;
                if sync_key_gen.count_complete() >= NODE_N &&
                        *ack_count >= NODE_N * NODE_N {
                    assert!(sync_key_gen.is_ready());
                    complete = true;
                }
            },
            State::ConnectedValidator { .. } | State::ConnectedObserver { .. } => {
                error!("Additional unhandled `Ack` received from '{}': \n{:?}", src_uid, ack);
            }
            _ => panic!("::handle_key_gen_part_ack: State must be `ConnectedGeneratingKeys`."),
        }

        if complete {
            info!("== INITIALIZING HONEY BADGER ==");
            state.set_connected_validator(*self.hdb.uid(), self.hdb.secret_key().clone(), peers);
            self.hdb.set_state_discriminant(state.discriminant());
        }
    }

    fn handle_net_state(&self, net_state: NetworkState, state: &mut State, peers: &Peers) {
        let peer_infos;
        match net_state {
            NetworkState::Unknown(p_infos) => {
                peer_infos = p_infos;
                state.update_peer_connection_added(peers);
                self.hdb.set_state_discriminant(state.discriminant());
            }
            NetworkState::AwaitingMorePeers(p_infos) => {
                peer_infos = p_infos;
                state.set_connected_awaiting_more_peers();
                self.hdb.set_state_discriminant(state.discriminant());
            },
            NetworkState::GeneratingKeys(p_infos) => {
                peer_infos = p_infos;
                // state.set_connected_observer();
            },
            NetworkState::Active(p_infos) => {
                peer_infos = p_infos;
                // state.set_connected_observer();
            },
            NetworkState::None => panic!("`NetworkState::None` received."),
        }

        // Connect to all newly discovered peers.
        for peer_info in peer_infos.iter() {
            // Only connect with peers which are not already
            // connected (and are not us).
            if peer_info.in_addr != *self.hdb.addr() &&
                    !peers.contains_in_addr(&peer_info.in_addr) {
                let local_pk = self.hdb.secret_key().public_key();
                tokio::spawn(self.hdb.clone().connect_outgoing(
                    peer_info.in_addr.0, local_pk,
                    Some((peer_info.uid, peer_info.in_addr, peer_info.pk))));
            }
        }
    }

    fn handle_peer_disconnect(&self, src_uid: Uid, state: &mut State, peers: &Peers)
            -> Result<(), Error> {
        // self.hdb.qhb.write().input(Input::Change(Change::Remove(self.uid)))
        //     .expect("Error adding new peer to HB");

        // Input::Change(Change::Remove(NodeUid(0)))
                // self.hdb.peer_internal_tx.unbounded_send(InternalMessage::input(
        //     uid, self.out_addr, Input::Change(Change::Remove(uid)))).unwrap();

        state.update_peer_connection_dropped(peers);
        self.hdb.set_state_discriminant(state.discriminant());

        // TODO: Send a node removal (Change-Remove) vote?

        match state {
            State::Disconnected { .. } => {
                panic!("Received `WireMessageKind::PeerDisconnect` while disconnected.");
            },
            State::DeterminingNetworkState { .. } => {
                // unimplemented!();
            },
            State::ConnectedAwaitingMorePeers { .. } => {
                // info!("Removing peer ({}: '{}') from await list.",
                //     src_out_addr, src_uid.clone().unwrap());
                // state.peer_connection_dropped(&*self.hdb.peers());
            },
            State::ConnectedGeneratingKeys { .. } => {
                // Do something here (possibly panic).
            },
            State::ConnectedObserver { .. } => {
                // Do nothing?
            }
            State::ConnectedValidator { ref mut qhb } => {
                let step = qhb.as_mut().unwrap().input(QhbInput::Change(Change::Remove(src_uid)))?;
                self.step_queue.push(step);
            },
        }
        Ok(())
    }

    fn handle_internal_message(&self, i_msg: InternalMessage, state: &mut State)
            -> Result<(), Error> {
        let (src_uid, src_out_addr, w_msg) = i_msg.into_parts();

        // trace!("Handler::handle_internal_message: Locking 'state' for writing...");
        // let mut state = self.hdb.state_mut();
        // trace!("Handler::handle_internal_message: 'state' locked for writing.");

        match w_msg {
            // New incoming connection:
            InternalMessageKind::NewIncomingConnection(_src_in_addr, src_pk) => {
                let peers = self.hdb.peers();

                // if let StateDsct::Disconnected = state.discriminant() {
                //     state.set_connected_awaiting_more_peers();
                // }
                match state.discriminant() {
                    StateDsct::Disconnected | StateDsct::DeterminingNetworkState  => {
                        state.set_connected_awaiting_more_peers();
                        self.hdb.set_state_discriminant(state.discriminant());
                        // state.peer_connection_added(&peers);
                    },
                    _ => {},
                }

                // Get the current `NetworkState`:
                let net_state = state.network_state(&peers);

                // Send response to remote peer:
                peers.get(&src_out_addr).unwrap().tx().unbounded_send(
                    WireMessage::welcome_received_change_add(
                        self.hdb.uid().clone(), self.hdb.secret_key().public_key(),
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
                state.update_peer_connection_added(&peers);
                self.hdb.set_state_discriminant(state.discriminant());
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
                let dropped_src_uid = src_uid.clone().unwrap();
                info!("Peer disconnected: ({}: '{}').", src_out_addr, dropped_src_uid);
                let peers = self.hdb.peers();
                self.handle_peer_disconnect(dropped_src_uid, state, &peers)?;
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
                // New outgoing connection:
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
                WireMessageKind::KeyGenPart(part) => {
                    self.handle_key_gen_part(&src_uid.unwrap(), part, state);
                },
                WireMessageKind::KeyGenPartAck(ack) => {
                    let peers = self.hdb.peers();
                    self.handle_key_gen_part_ack(&src_uid.unwrap(), ack, state, &peers);
                },
                _ => {},
            },
        }

        // trace!("Handler::handle_internal_message: 'state' unlocked for writing.");
        Ok(())
    }
}

impl Future for Handler {
    type Item = ();
    type Error = Error;

    /// Polls the internal message receiver until all txs are dropped.
    fn poll(&mut self) -> Poll<(), Error> {
        // Ensure the loop can't hog the thread for too long:
        const MESSAGES_PER_TICK: usize = 50;

        trace!("Handler::poll: Locking 'state' for writing...");
        let mut state = self.hdb.state_mut();
        trace!("Handler::poll: 'state' locked for writing.");

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
                    info!("Shutting down Handler...");
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
                info!("NEW STEP OUTPUT:",);
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
        trace!("Handler::poll: 'state' unlocked for writing.");

        Ok(Async::NotReady)
    }
}

