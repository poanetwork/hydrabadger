//! Hydrabadger event handler.
//!
//! FIXME: Reorganize `Handler` and `State` to more clearly separate concerns.
//!     * Do not make state changes directly in this module (use closures, etc.).
//!

#![allow(unused_imports, dead_code, unused_variables, unused_mut, unused_assignments,
    unreachable_code)]

use std::collections::BTreeMap;
use crossbeam::sync::SegQueue;
use tokio::{
    self,
    prelude::*,
};
use hbbft::{
    crypto::{PublicKey, PublicKeySet},
    sync_key_gen::{ Part, PartOutcome, Ack},
    messaging::{DistAlgorithm, Target, },
    dynamic_honey_badger::{Message as DhbMessage, JoinPlan},
    queueing_honey_badger::{Input as QhbInput, Change as QhbChange},
};
use peer::Peers;
use ::{InternalMessage, InternalMessageKind, WireMessage, WireMessageKind,
    OutAddr, InAddr, Uid, NetworkState, InternalRx, Step, Input, Message, NetworkNodeInfo};
use super::{Hydrabadger, Error, State, StateDsct, InputOrMessage};
use super::{HB_PEER_MINIMUM_COUNT, WIRE_MESSAGE_RETRY_MAX, EXTRA_DELAY_MS};




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
             request_change_add: bool, state: &mut State, peers: &Peers) {
        match state.discriminant() {
            StateDsct::Disconnected | StateDsct::DeterminingNetworkState => {
                // panic!("Handler::handle_new_established_peer: \
                //     Received `WireMessageKind::WelcomeRequestChangeAdd` or \
                //     `InternalMessageKind::NewIncomingConnection` while \
                //     `StateDsct::Disconnected` or `DeterminingNetworkState`.");
                state.update_peer_connection_added(&peers);
                self.hdb.set_state_discriminant(state.discriminant());
            },
            StateDsct::AwaitingMorePeersForKeyGeneration => {
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
            StateDsct::GeneratingKeys { .. } => {
                // This *could* be called multiple times when initially
                // establishing outgoing connections. Do nothing for now.
                warn!("Handler::handle_new_established_peer: Ignoring new established \
                    peer signal while `StateDsct::GeneratingKeys`.");
            },
            StateDsct::Observer | StateDsct::Validator => {
                // If the new peer sends a request-change-add (to be a
                // validator), input the change into HB and broadcast, etc.
                if request_change_add {
                    let qhb = state.qhb_mut().unwrap();
                    info!("Change-Adding ('{}') to honey badger.", src_uid);
                    let step = qhb.input(QhbInput::Change(QhbChange::Add(src_uid, src_pk)))
                        .expect("Error adding new peer to HB");
                    self.step_queue.push(step);
                }
            },
        }
    }

    // TODO: Create a type for `net_info`.
    fn instantiate_hb(&self,
            // net_info: Option<(Vec<NetworkNodeInfo>, PublicKeySet, BTreeMap<Uid, PublicKey>)>,
            jp_opt: Option<JoinPlan<Uid>>,
            state: &mut State, peers: &Peers) -> Result<(), Error> {
        let mut iom_queue_opt = None;

        match state.discriminant() {
            StateDsct::Disconnected => { unimplemented!() },
            StateDsct::DeterminingNetworkState | StateDsct::GeneratingKeys => {
                info!("== INSTANTIATING HONEY BADGER ==");
                match jp_opt {
                    // Some((nni, pk_set, pk_map)) => {
                    //     iom_queue_opt = Some(state.set_observer(*self.hdb.uid(),
                    //         self.hdb.secret_key().clone(), nni, pk_set, pk_map));
                    // },
                    Some(jp) => {
                        iom_queue_opt = Some(state.set_observer(*self.hdb.uid(),
                            self.hdb.secret_key().clone(), jp)?);
                    },
                    None => {
                        iom_queue_opt = Some(state.set_validator(*self.hdb.uid(),
                            self.hdb.secret_key().clone(), peers)?);
                    }
                }
            },
            StateDsct::AwaitingMorePeersForKeyGeneration => { unimplemented!() },
            StateDsct::Observer => {
                // TODO: Add checks to ensure that `net_info` is consistent
                // with HB's netinfo.
                warn!("Handler::instantiate_hb: Called when `State::Observer`");
            },
            StateDsct::Validator => {
                // TODO: Add checks to ensure that `net_info` is consistent
                // with HB's netinfo.
                warn!("Handler::instantiate_hb: Called when `State::Validator`")
            },
        }

        self.hdb.set_state_discriminant(state.discriminant());

        // Handle previously queued input and messages:
        if let Some(iom_queue) = iom_queue_opt {
            while let Some(iom) = iom_queue.try_pop() {
                match iom {
                    InputOrMessage::Input(input) => {
                        self.handle_input(input, state)?;
                    },
                    InputOrMessage::Message(uid, msg) => {
                        self.handle_message(msg, &uid, state)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_input(&self, input: Input, state: &mut State) -> Result <(), Error> {
        match &input {
            QhbInput::User(_contrib) => {},
            QhbInput::Change(ref qhb_change) => match qhb_change {
                QhbChange::Add(uid, pk) => {
                    if uid == self.hdb.uid() {
                        assert!(*pk == self.hdb.secret_key().public_key());
                    }
                }
                QhbChange::Remove(_uid) => {},
            },
        }

        if let Some(step_res) = state.input(input) {
            let step = step_res.map_err(|err| {
                error!("Honey Badger input error: {:?}", err);
                Error::HbStepError
            })?;
            self.step_queue.push(step);
        }
        Ok(())
    }

    fn handle_message(&self, msg: Message, src_uid: &Uid, state: &mut State) -> Result <(), Error> {
        trace!("HB_MESSAGE: {:?}", msg);
        // match &msg {
        //     // A message belonging to the `HoneyBadger` algorithm started in
        //     // the given epoch.
        //     DhbMessage::HoneyBadger(start_epoch, ref msg) => {},
        //     // A transaction to be committed, signed by a node.
        //     DhbMessage::KeyGen(epoch, _key_gen_msg, _sig) => {},
        //     // A vote to be committed, signed by a validator.
        //     DhbMessage::SignedVote(signed_vote) => {},
        // }
        if let Some(step_res) = state.handle_message(src_uid, msg) {
            let step = step_res.map_err(|err| {
                error!("Honey Badger handle_message error: {:?}", err);
                Error::HbStepError
            })?;
            self.step_queue.push(step);
        }
        Ok(())
    }

    fn handle_key_gen_part(&self, src_uid: &Uid, part: Part, state: &mut State) {
        match state {
            State::GeneratingKeys { ref mut sync_key_gen, ref mut ack_count, .. } => {
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
                    error!("Errors acknowledging part:\n{:?}", fault_log);
                }
                *ack_count += 1;

                let peers = self.hdb.peers();
                info!("KEY GENERATION: Part from '{}' acknowledged. Broadcasting...", src_uid);
                self.wire_to_validators(WireMessage::key_gen_part_ack(ack), &peers);
            }
            s @ _ => panic!("::handle_key_gen_part: State must be `GeneratingKeys`. \
                State: \n{:?}", s.discriminant()),
        }
    }

    fn handle_key_gen_part_ack(&self, src_uid: &Uid, ack: Ack, state: &mut State, peers: &Peers)
            -> Result<(), Error> {

        // FIXME: Queue acks until all parts are received.

        let mut complete = false;
        match state {
            State::GeneratingKeys { ref mut sync_key_gen, ref mut ack_count, .. } => {
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
                if sync_key_gen.count_complete() >= NODE_N
                        && *ack_count >= NODE_N * NODE_N {
                    assert!(sync_key_gen.is_ready());
                    complete = true;
                }
            },
            State::Validator { .. } | State::Observer { .. } => {
                warn!("Additional unhandled `Ack` received from '{}': \n{:?}", src_uid, ack);
            }
            _ => panic!("::handle_key_gen_part_ack: State must be `GeneratingKeys`."),
        }

        if complete {
            self.instantiate_hb(None, state, peers)?;
        }
        Ok(())
    }

    // This may be called spuriously and only need be handled by
    // 'unestablished' nodes.
    fn handle_join_plan(&self, jp: JoinPlan<Uid>, state: &mut State, peers: &Peers)
            -> Result<(), Error> {
        info!("Received join plan: {:?}", jp);

        match state.discriminant() {
            StateDsct::Disconnected => unimplemented!("Handler::handle_join_plan: `Disconnected`"),
            StateDsct::DeterminingNetworkState => {
                self.instantiate_hb(Some(jp), state, peers)?;
            },
            StateDsct::AwaitingMorePeersForKeyGeneration | StateDsct::GeneratingKeys => {
                panic!("Handler::handle_join_plan: Received join plan while \
                    `AwaitingMorePeersForKeyGeneration` or `GeneratingKeys`");
            },
            StateDsct::Observer | StateDsct::Validator => {}, // Ignore
            // sd @ _ => unimplemented!("Handler::handle_join_plan: {:?}", sd),
        }

        Ok(())
    }

    fn handle_net_state(&self, net_state: NetworkState, state: &mut State, peers: &Peers)
            -> Result<(), Error> {
        let peer_infos;
        match net_state {
            NetworkState::Unknown(p_infos) => {
                peer_infos = p_infos;
                state.update_peer_connection_added(peers);
                self.hdb.set_state_discriminant(state.discriminant());
            }
            NetworkState::AwaitingMorePeers(p_infos) => {
                peer_infos = p_infos;
                state.set_awaiting_more_peers();
                self.hdb.set_state_discriminant(state.discriminant());
            },
            NetworkState::GeneratingKeys(p_infos) => {
                peer_infos = p_infos;
                // state.set_observer();
            },
            NetworkState::Active(net_info) => {
                peer_infos = net_info.0.clone();
                // self.instantiate_hb(Some(net_info), peers, state)?;
            },
            NetworkState::None => panic!("`NetworkState::None` received."),
        }

        // Connect to all newly discovered peers.
        for peer_info in peer_infos.iter() {
            // Only connect with peers which are not already
            // connected (and are not us).
            if peer_info.in_addr != *self.hdb.addr()
                    && !peers.contains_in_addr(&peer_info.in_addr)
                    && peers.get(&OutAddr(peer_info.in_addr.0)).is_none() {
                let local_pk = self.hdb.secret_key().public_key();
                tokio::spawn(self.hdb.clone().connect_outgoing(
                    peer_info.in_addr.0,
                    local_pk,
                    Some((peer_info.uid, peer_info.in_addr, peer_info.pk)),
                    false,
                ));
            }
        }
        Ok(())
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
            State::AwaitingMorePeersForKeyGeneration { .. } => {
                // info!("Removing peer ({}: '{}') from await list.",
                //     src_out_addr, src_uid.clone().unwrap());
                // state.peer_connection_dropped(&*self.hdb.peers());
            },
            State::GeneratingKeys { .. } => {
                // Do something here (possibly panic).
            },
            State::Observer { .. } => {
                // Do nothing?
            }
            State::Validator { ref mut qhb } => {
                let step = qhb.as_mut().unwrap().input(QhbInput::Change(QhbChange::Remove(src_uid)))?;
                self.step_queue.push(step);
            },
        }
        Ok(())
    }

    fn handle_internal_message(&self, i_msg: InternalMessage, state: &mut State)
            -> Result<(), Error> {
        let (src_uid, src_out_addr, w_msg) = i_msg.into_parts();

        match w_msg {
            // New incoming connection:
            InternalMessageKind::NewIncomingConnection(_src_in_addr, src_pk, request_change_add) => {
                let peers = self.hdb.peers();

                // if let StateDsct::Disconnected = state.discriminant() {
                //     state.set_awaiting_more_peers();
                // }
                match state.discriminant() {
                    StateDsct::Disconnected | StateDsct::DeterminingNetworkState  => {
                        state.set_awaiting_more_peers();
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
                self.handle_new_established_peer(src_uid.unwrap(), src_out_addr, src_pk,
                     request_change_add, state, &peers);
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

            InternalMessageKind::HbInput(input) => {
                self.handle_input(input, state)?;
            },

            InternalMessageKind::HbMessage(msg) => {
                self.handle_message(msg, src_uid.as_ref().unwrap(), state)?;
            },

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
                        true => debug_assert!(src_uid_new == src_uid.clone().unwrap()),
                        false => debug_assert!(src_uid.is_none()),
                    }

                    // Modify state accordingly:
                    self.handle_net_state(net_state, state, &peers)?;
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
                    self.handle_net_state(net_state, state, &peers)?;

                    // Modify state accordingly:
                    self.handle_new_established_peer(src_uid_new, src_out_addr, src_pk,
                        false, state, &peers);
                },

                // Key gen proposal:
                WireMessageKind::KeyGenPart(part) => {
                    self.handle_key_gen_part(&src_uid.unwrap(), part, state);
                },

                // Key gen proposal acknowledgement:
                //
                // FIXME: Queue until all parts have been sent.
                WireMessageKind::KeyGenPartAck(ack) => {
                    let peers = self.hdb.peers();
                    self.handle_key_gen_part_ack(&src_uid.unwrap(), ack, state, &peers)?;
                },

                // Output by validators when a batch with a `ChangeState`
                // other than `None` is output. Idempotent.
                WireMessageKind::JoinPlan(jp) => {
                    let peers = self.hdb.peers();
                    self.handle_join_plan(jp, state, &peers)?;
                },

                wm @ _ => warn!("Handler::handle_internal_message: Unhandled wire message: \
                    \n{:?}", wm,),
            },
        }
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

        // Process all honey badger output batches:
        while let Some(mut step) = self.step_queue.try_pop() {
            if step.output.len() > 0 { info!("NEW STEP OUTPUT:"); }

            for batch in step.output.drain(..) {
                info!("    BATCH: \n{:?}", batch);
                if let Some(jp) = batch.join_plan() {
                    debug!("Outputting join plan: {:?}", jp);
                    self.wire_to_all(WireMessage::join_plan(jp), &peers);
                }

                // if EXTRA_DELAY_MS > 0 {
                //     info!("Delaying thread for {}ms", EXTRA_DELAY_MS);
                //     ::std::thread::sleep(::std::time::Duration::from_millis(EXTRA_DELAY_MS));
                // }

                // TODO: Something useful!
            }
            if !step.fault_log.is_empty() {
                error!("    FAULT LOG: \n{:?}", step.fault_log);
            }

        }

        drop(peers);
        drop(state);
        trace!("Handler::poll: 'state' unlocked for writing.");

        Ok(Async::NotReady)
    }
}

