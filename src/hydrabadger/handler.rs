//! Hydrabadger event handler.
//!
//! FIXME: Reorganize `Handler` and `State` to more clearly separate concerns.
//!     * Do not make state changes directly in this module (use closures, etc.).
//!

use super::WIRE_MESSAGE_RETRY_MAX;
use super::{Error, Hydrabadger, InputOrMessage, State, StateDsct, StateMachine};
use crate::peer::Peers;
use crate::{
    key_gen, BatchTx, Contribution, InAddr, InternalMessage, InternalMessageKind, InternalRx,
    NetworkState, OutAddr, Step, Uid, WireMessage, WireMessageKind,
};
use crossbeam::queue::SegQueue;
use hbbft::{
    crypto::PublicKey,
    dynamic_honey_badger::{Change as DhbChange, ChangeState, JoinPlan},
    sync_key_gen::{Ack, Part},
    Target,
};
use std::{cell::RefCell, collections::HashMap};
use tokio::{self, prelude::*};

/// Hydrabadger event (internal message) handler.
pub struct Handler<T: Contribution> {
    hdb: Hydrabadger<T>,
    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    peer_internal_rx: InternalRx<T>,
    /// Outgoing wire message queue.
    wire_queue: SegQueue<(Uid, WireMessage<T>, usize)>,
    /// Output from HoneyBadger.
    step_queue: SegQueue<Step<T>>,
    // TODO: Use a bounded tx/rx (find a sensible upper bound):
    batch_tx: BatchTx<T>,
    /// Distributed synchronous key generation instances.
    //
    // TODO: Move these to separate threads/tasks.
    key_gens: RefCell<HashMap<Uid, key_gen::Machine>>,
}

impl<T: Contribution> Handler<T> {
    pub(super) fn new(
        hdb: Hydrabadger<T>,
        peer_internal_rx: InternalRx<T>,
        batch_tx: BatchTx<T>,
    ) -> Handler<T> {
        Handler {
            hdb,
            peer_internal_rx,
            wire_queue: SegQueue::new(),
            step_queue: SegQueue::new(),
            batch_tx,
            key_gens: RefCell::new(HashMap::new()),
        }
    }

    fn handle_new_established_peer(
        &self,
        src_uid: Uid,
        src_pk: PublicKey,
        request_change_add: bool,
        state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        match state.discriminant() {
            StateDsct::Disconnected | StateDsct::DeterminingNetworkState => {
                state.update_peer_connection_added(&peers);
            }
            StateDsct::KeyGen => {
                // TODO: Should network state simply be stored within key_gen?
                let net_state = state.network_state(&peers);
                state
                    .key_gen_mut()
                    .unwrap()
                    .add_peers(peers, &self.hdb, net_state)?;
            }
            StateDsct::Observer | StateDsct::Validator => {
                // If the new peer sends a request-change-add (to be a
                // validator), input the change into HB and broadcast, etc.
                if request_change_add {
                    let dhb = state.dhb_mut().unwrap();
                    info!("Change-Adding ('{}') to honey badger.", src_uid);
                    let step = dhb
                        .vote_to_add(src_uid, src_pk)
                        .expect("Error adding new peer to HB");
                    self.step_queue.push(step);
                }
            }
        }
        Ok(())
    }

    fn handle_iom(&self, iom: InputOrMessage<T>, state: &mut StateMachine<T>) -> Result<(), Error> {
        trace!("hydrabadger::Handler: About to handle_iom: {:?}", iom);
        if let Some(step_res) = state.handle_iom(iom) {
            let step = step_res.map_err(Error::HbStep)?;
            trace!("hydrabadger::Handler: Message step result added to queue....");
            self.step_queue.push(step);
        }
        Ok(())
    }

    /// Handles a received `Part`.
    fn handle_key_gen_part(
        &self,
        src_uid: &Uid,
        part: Part,
        state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        match state.state {
            State::KeyGen {
                ref mut key_gen, ..
            } => {
                key_gen.handle_key_gen_part(src_uid, part, peers);
            }
            State::DeterminingNetworkState {
                ref network_state, ..
            } => match network_state.is_some() {
                true => unimplemented!(),
                false => unimplemented!(),
            },
            ref s => panic!(
                "::handle_key_gen_part: State must be `GeneratingKeys`. \
                 State: \n{:?} \n\n[FIXME: Enqueue these parts!]\n\n",
                s.discriminant()
            ),
        }
        Ok(())
    }

    /// Handles a received `Ack`.
    fn handle_key_gen_ack(
        &self,
        src_uid: &Uid,
        ack: Ack,
        state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        let mut complete = false;

        match state.state {
            State::KeyGen {
                ref mut key_gen, ..
            } => {
                if key_gen.handle_key_gen_ack(src_uid, ack, peers)? {
                    complete = true;
                }
            }
            State::Validator { .. } | State::Observer { .. } => {
                error!(
                    "Additional unhandled `Ack` received from '{}': \n{:?}",
                    src_uid, ack
                );
            }
            _ => panic!("::handle_key_gen_ack: State must be `GeneratingKeys`."),
        }
        if complete {
            self.instantiate_hb(None, state, peers)?;
        }
        Ok(())
    }

    fn handle_key_gen_message(
        &self,
        instance_id: key_gen::InstanceId,
        msg: key_gen::Message,
        src_uid: &Uid,
        state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        use crate::key_gen::{InstanceId, MessageKind};

        match instance_id {
            InstanceId::User(id) => {
                let mut key_gens = self.key_gens.borrow_mut();
                match key_gens.get_mut(&id) {
                    Some(ref mut kg) => {
                        kg.event_tx().unwrap().unbounded_send(msg.clone()).unwrap();

                        match msg.into_kind() {
                            MessageKind::Part(part) => {
                                kg.handle_key_gen_part(src_uid, part, peers);
                            }
                            MessageKind::Ack(ack) => {
                                kg.handle_key_gen_ack(src_uid, ack, peers)?;
                            }
                        }
                    }
                    None => error!("KeyGen message received with invalid instance"),
                }
            }
            InstanceId::BuiltIn => match msg.into_kind() {
                MessageKind::Part(part) => {
                    self.handle_key_gen_part(src_uid, part, state, peers)?;
                }
                MessageKind::Ack(ack) => {
                    self.handle_key_gen_ack(src_uid, ack, state, peers)?;
                }
            },
        }

        Ok(())
    }

    // This may be called spuriously and only need be handled by
    // 'unestablished' nodes.
    fn handle_join_plan(
        &self,
        jp: JoinPlan<Uid>,
        state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        debug!("Join plan: \n{:?}", jp);

        match state.discriminant() {
            StateDsct::Disconnected => {
                unimplemented!("hydrabadger::Handler::handle_join_plan: `Disconnected`")
            }
            StateDsct::DeterminingNetworkState => {
                info!("Received join plan.");
                self.instantiate_hb(Some(jp), state, peers)?;
            }
            StateDsct::KeyGen => {
                panic!(
                    "hydrabadger::Handler::handle_join_plan: Received join plan while \
                     `{}`",
                    state.discriminant()
                );
            }
            StateDsct::Observer | StateDsct::Validator => {}
        }

        Ok(())
    }

    // TODO: Create a type for `net_info`.
    fn instantiate_hb(
        &self,
        jp_opt: Option<JoinPlan<Uid>>,
        state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        let mut iom_queue_opt = None;

        match state.discriminant() {
            StateDsct::Disconnected => unimplemented!(),
            StateDsct::DeterminingNetworkState | StateDsct::KeyGen => {
                info!("== INSTANTIATING HONEY BADGER ==");
                match jp_opt {
                    Some(jp) => {
                        let epoch = jp.next_epoch();
                        iom_queue_opt = Some(state.set_observer(
                            *self.hdb.uid(),
                            self.hdb.secret_key().clone(),
                            jp,
                            self.hdb.config(),
                            &self.step_queue,
                        )?);
                        self.hdb.set_current_epoch(epoch);
                    }
                    None => {
                        iom_queue_opt = Some(state.set_validator(
                            *self.hdb.uid(),
                            self.hdb.secret_key().clone(),
                            peers,
                            self.hdb.config(),
                            &self.step_queue,
                        )?);
                    }
                }
                for l in self.hdb.epoch_listeners().iter() {
                    l.unbounded_send(self.hdb.current_epoch())
                        .map_err(|_| Error::InstantiateHbListenerDropped)?;
                }
            }
            StateDsct::Observer => {
                // TODO: Add checks to ensure that `net_info` is consistent
                // with HB's netinfo.
                warn!("hydrabadger::Handler::instantiate_hb: Called when `State::Observer`");
            }
            StateDsct::Validator => {
                // TODO: Add checks to ensure that `net_info` is consistent
                // with HB's netinfo.
                warn!("hydrabadger::Handler::instantiate_hb: Called when `State::Validator`")
            }
        }

        // Handle previously queued input and messages:
        if let Some(iom_queue) = iom_queue_opt {
            while let Some(iom) = iom_queue.try_pop() {
                self.handle_iom(iom, state)?;
            }
        }
        Ok(())
    }

    /// Resets all connections with peers.
    ///
    /// Used when state gets out of sync such as when key generation completed
    /// without including this node.
    fn reset_peer_connections(
        &self,
        _state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        peers.wire_to_validators(WireMessage::hello_request_change_add(
            *self.hdb.uid(),
            *self.hdb.addr(),
            self.hdb.secret_key().public_key(),
        ));
        Ok(())
    }

    fn handle_net_state(
        &self,
        net_state: NetworkState,
        state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        let peer_infos;
        match net_state {
            NetworkState::Unknown(p_infos) => {
                peer_infos = p_infos;
                state.update_peer_connection_added(peers);
            }
            NetworkState::AwaitingMorePeersForKeyGeneration(p_infos) => {
                peer_infos = p_infos;
                state.set_awaiting_more_peers();
            }
            NetworkState::GeneratingKeys(p_infos, _public_keys) => {
                peer_infos = p_infos;
            }
            NetworkState::Active(net_info) => {
                peer_infos = net_info.0.clone();
                let mut reset_fresh = false;

                match state.state {
                    State::DeterminingNetworkState {
                        ref mut network_state,
                        ..
                    } => {
                        *network_state = Some(NetworkState::Active(net_info.clone()));
                    }
                    State::KeyGen { ref key_gen, .. } => {
                        if key_gen.is_awaiting_peers() {
                            reset_fresh = true;
                        } else {
                            panic!(
                                "Handler::net_state: Received `NetworkState::Active` while `{}`.",
                                state.discriminant()
                            );
                        }
                    }
                    State::Disconnected { .. } => {
                        panic!(
                            "Handler::net_state: Received `NetworkState::Active` while `{}`.",
                            state.discriminant()
                        );
                    }
                    _ => {}
                }
                if reset_fresh {
                    // Key generation has completed and we were not a part
                    // of it. Need to restart as a freshly connecting node.
                    state.set_determining_network_state_active(net_info);
                    self.reset_peer_connections(state, peers)?;
                }
            }
            NetworkState::None => panic!("`NetworkState::None` received."),
        }

        // Connect to all newly discovered peers.
        for peer_info in peer_infos.iter() {
            // Only connect with peers which are not already
            // connected (and are not us).
            if peer_info.in_addr != *self.hdb.addr()
                && !peers.contains_in_addr(&peer_info.in_addr)
                && peers.get(&OutAddr(peer_info.in_addr.0)).is_none()
            {
                let local_sk = self.hdb.secret_key().clone();
                tokio::spawn(self.hdb.clone().connect_outgoing(
                    peer_info.in_addr.0,
                    local_sk,
                    Some((peer_info.uid, peer_info.in_addr, peer_info.pk)),
                    false,
                ));
            }
        }
        Ok(())
    }

    fn handle_peer_disconnect(
        &self,
        src_uid: Uid,
        state: &mut StateMachine<T>,
        peers: &Peers<T>,
    ) -> Result<(), Error> {
        state.update_peer_connection_dropped(peers);

        // TODO: Send a node removal (Change-Remove) vote?

        match state.state {
            State::Disconnected { .. } => {
                panic!("Received `WireMessageKind::PeerDisconnect` while disconnected.");
            }
            State::DeterminingNetworkState { .. } => {
                // unimplemented!();
            }
            State::KeyGen { .. } => {
                // Do something here (possibly panic).
            }
            State::Observer { .. } => {
                // Observers cannot vote.
            }
            State::Validator { ref mut dhb } => {
                let step = dhb.as_mut().unwrap().vote_to_remove(&src_uid)?;
                self.step_queue.push(step);
            }
        }
        Ok(())
    }

    fn handle_internal_message(
        &self,
        i_msg: InternalMessage<T>,
        state: &mut StateMachine<T>,
    ) -> Result<(), Error> {
        // let mut state_guard = self.hdb.state_mut();
        // let mut state = &mut state_guard;

        let (src_uid, src_out_addr, w_msg) = i_msg.into_parts();

        match w_msg {
            // New incoming connection:
            InternalMessageKind::NewIncomingConnection(
                _src_in_addr,
                src_pk,
                request_change_add,
            ) => {
                let peers = self.hdb.peers();

                let net_state;

                match state.state {
                    State::Disconnected {} => {
                        state.set_awaiting_more_peers();
                        net_state = state.network_state(&peers);
                    }
                    State::DeterminingNetworkState {
                        ref network_state, ..
                    } => match network_state {
                        Some(ns) => net_state = ns.clone(),
                        None => net_state = state.network_state(&peers),
                    },
                    _ => net_state = state.network_state(&peers),
                }

                // Send response to remote peer:
                peers
                    .get(&src_out_addr)
                    .unwrap()
                    .tx()
                    .unbounded_send(WireMessage::welcome_received_change_add(
                        *self.hdb.uid(),
                        self.hdb.secret_key().public_key(),
                        net_state,
                    ))
                    .unwrap();

                // Modify state accordingly:
                self.handle_new_established_peer(
                    src_uid.unwrap(),
                    // src_out_addr,
                    src_pk,
                    request_change_add,
                    state,
                    &peers,
                )?;
            }

            // New outgoing connection (initial):
            InternalMessageKind::NewOutgoingConnection => {
                // This message must be immediately followed by either a
                // `WireMessage::HelloFromValidator` or
                // `WireMessage::WelcomeReceivedChangeAdd`.
                debug_assert!(src_uid.is_none());

                let peers = self.hdb.peers();
                state.update_peer_connection_added(&peers);
            }

            InternalMessageKind::HbContribution(contrib) => {
                self.handle_iom(InputOrMessage::Contribution(contrib), state)?;
            }

            InternalMessageKind::HbChange(change) => {
                self.handle_iom(InputOrMessage::Change(change), state)?;
            }

            InternalMessageKind::HbMessage(msg) => {
                self.handle_iom(InputOrMessage::Message(src_uid.unwrap(), msg), state)?;
            }

            InternalMessageKind::PeerDisconnect => {
                let dropped_src_uid = src_uid.unwrap();
                info!(
                    "Peer disconnected: ({}: '{}').",
                    src_out_addr, dropped_src_uid
                );
                let peers = self.hdb.peers();
                self.handle_peer_disconnect(dropped_src_uid, state, &peers)?;
            }

            InternalMessageKind::NewKeyGenInstance(tx) => {
                // TODO: Spawn these instances in a separate thread/task.

                let peers = self.hdb.peers();
                let new_id = Uid::new();
                // tx.unbounded_send(key_gen::Message::instance_id().unwrap();
                let instance_id = key_gen::InstanceId::User(new_id.clone());
                let key_gen = key_gen::Machine::generate(
                    self.hdb.uid(),
                    self.hdb.secret_key().clone(),
                    &peers,
                    tx,
                    instance_id,
                )?;
                self.key_gens.borrow_mut().insert(new_id, key_gen);
            }

            InternalMessageKind::Wire(w_msg) => match w_msg.into_kind() {
                // This is sent on the wire to ensure that we have all of the
                // relevant details for a peer (generally preceeding other
                // messages which may arrive before `Welcome...`.
                WireMessageKind::HelloFromValidator(
                    src_uid_new,
                    src_in_addr,
                    src_pk,
                    net_state,
                ) => {
                    debug!("Received hello from {}", src_uid_new);
                    let mut peers = self.hdb.peers_mut();
                    match peers
                        .establish_validator(src_out_addr, (src_uid_new, src_in_addr, src_pk))
                    {
                        true => debug_assert!(src_uid_new == src_uid.unwrap()),
                        false => debug_assert!(src_uid.is_none()),
                    }

                    // Modify state accordingly:
                    self.handle_net_state(net_state, state, &peers)?;
                }

                // New outgoing connection response:
                WireMessageKind::WelcomeReceivedChangeAdd(src_uid_new, src_pk, net_state) => {
                    debug!("Received NetworkState: \n{:?}", net_state);
                    assert!(src_uid_new == src_uid.unwrap());
                    let mut peers = self.hdb.peers_mut();

                    // Set new (outgoing-connection) peer's public info:
                    peers.establish_validator(
                        src_out_addr,
                        (src_uid_new, InAddr(src_out_addr.0), src_pk),
                    );

                    // Modify state accordingly:
                    self.handle_net_state(net_state, state, &peers)?;

                    // Modify state accordingly:
                    self.handle_new_established_peer(
                        src_uid_new,
                        // src_out_addr,
                        src_pk,
                        false,
                        state,
                        &peers,
                    )?;
                }

                WireMessageKind::KeyGen(instance_id, msg) => {
                    self.handle_key_gen_message(
                        instance_id,
                        msg,
                        &src_uid.unwrap(),
                        state,
                        &self.hdb.peers(),
                    )?;
                }

                // Output by validators when a batch with a `ChangeState`
                // other than `None` is output. Idempotent.
                WireMessageKind::JoinPlan(jp) => {
                    let peers = self.hdb.peers();
                    self.handle_join_plan(jp, state, &peers)?;
                }

                wm => warn!(
                    "hydrabadger::Handler::handle_internal_message: Unhandled wire message: \
                     \n{:?}",
                    wm,
                ),
            },
        }
        Ok(())
    }
}

impl<T: Contribution> Future for Handler<T> {
    type Item = ();
    type Error = Error;

    /// Polls the internal message receiver until all txs are dropped.
    fn poll(&mut self) -> Poll<(), Error> {
        // Ensure the loop can't hog the thread for too long:
        const MESSAGES_PER_TICK: usize = 50;

        let mut state = self.hdb.state_mut();

        // Handle incoming internal messages:
        for i in 0..MESSAGES_PER_TICK {
            match self.peer_internal_rx.poll() {
                Ok(Async::Ready(Some(i_msg))) => {
                    self.handle_internal_message(i_msg, &mut state)?;

                    // Exceeded max messages per tick, schedule notification:
                    if i + 1 == MESSAGES_PER_TICK {
                        task::current().notify();
                    }
                }
                Ok(Async::Ready(None)) => {
                    // The sending ends have all dropped.
                    info!("Shutting down Handler...");
                    return Ok(Async::Ready(()));
                }
                Ok(Async::NotReady) => {}
                Err(()) => return Err(Error::HydrabadgerHandlerPoll),
            };
        }

        let peers = self.hdb.peers();

        // Process outgoing wire queue:
        while let Some((tar_uid, msg, retry_count)) = self.wire_queue.try_pop() {
            if retry_count < WIRE_MESSAGE_RETRY_MAX {
                info!(
                    "Sending queued message from retry queue (retry_count: {})",
                    retry_count
                );
                peers.wire_to(tar_uid, msg, retry_count);
            } else {
                info!("Discarding queued message for '{}': {:?}", tar_uid, msg);
            }
        }

        trace!("hydrabadger::Handler: Processing step queue....");

        // let mut state = self.hdb.state_mut();

        // Process all honey badger output batches:
        while let Some(mut step) = self.step_queue.try_pop() {
            for batch in step.output.drain(..) {
                info!("A HONEY BADGER BATCH WITH CONTRIBUTIONS IS BEING STREAMED...");
                debug!("Batch:\n{:?}", batch);

                let batch_epoch = batch.epoch();
                let prev_epoch = self.hdb.set_current_epoch(batch_epoch + 1);
                assert_eq!(prev_epoch, batch_epoch);

                // TODO: Remove
                if cfg!(exit_upon_epoch_1000) && batch_epoch >= 1000 {
                    return Ok(Async::Ready(()));
                }

                if let Some(jp) = batch.join_plan() {
                    // FIXME: Only sent to unconnected nodes:
                    debug!("Outputting join plan: {:?}", jp);
                    peers.wire_to_all(WireMessage::join_plan(jp));
                }

                match batch.change() {
                    ChangeState::None => {}
                    ChangeState::InProgress(_change) => {}
                    ChangeState::Complete(change) => match change {
                        DhbChange::NodeChange(pub_keys) => {
                            if let Some(pk) = pub_keys.get(self.hdb.uid()) {
                                assert_eq!(*pk, self.hdb.secret_key().public_key());
                                assert!(state.dhb().unwrap().netinfo().is_validator());
                                if state.discriminant() == StateDsct::Observer {
                                    state.promote_to_validator()?;
                                }
                            }
                            // FIXME: Handle removed nodes.
                        }
                        // FIXME
                        DhbChange::EncryptionSchedule(_schedule) => {}
                    },
                }

                let extra_delay = self.hdb.config().output_extra_delay_ms;

                if extra_delay > 0 {
                    info!("Delaying batch processing thread for {}ms", extra_delay);
                    ::std::thread::sleep(::std::time::Duration::from_millis(extra_delay));
                }

                // Send the batch along its merry way:
                if !self.batch_tx.is_closed() {
                    if let Err(_err) = self.batch_tx.unbounded_send(batch) {
                        error!("Unable to send batch output. Shutting down...");
                        return Ok(Async::Ready(()));
                    } else {
                        // Notify epoch listeners that a batch has been output.
                        let mut dropped_listeners = Vec::new();
                        for (i, listener) in self.hdb.epoch_listeners().iter().enumerate() {
                            if let Err(_err) = listener.unbounded_send(batch_epoch + 1) {
                                dropped_listeners.push(i);
                                error!("Epoch listener {} has dropped.", i);
                            }
                        }
                        // TODO: Remove dropped listeners from the list (see
                        // comment on `Inner::epoch_listeners`).
                    }
                } else {
                    info!("Batch output receiver dropped. Shutting down...");
                    return Ok(Async::Ready(()));
                }
            }

            for hb_msg in step.messages.drain(..) {
                trace!("hydrabadger::Handler: Forwarding message: {:?}", hb_msg);
                match hb_msg.target {
                    Target::Node(p_uid) => {
                        peers.wire_to(
                            p_uid,
                            WireMessage::message(*self.hdb.uid(), hb_msg.message),
                            0,
                        );
                    }
                    Target::All => {
                        peers.wire_to_all(WireMessage::message(*self.hdb.uid(), hb_msg.message));
                    }
                }
            }

            if !step.fault_log.is_empty() {
                error!("    FAULT LOG: \n{:?}", step.fault_log);
            }
        }

        // TODO: Iterate through `state.dhb().unwrap().dyn_hb().netinfo()` and
        // `peers` to ensure that the lists match. Make adjustments where
        // necessary.

        trace!("hydrabadger::Handler: Step queue processing complete.");

        drop(peers);
        drop(state);
        trace!("hydrabadger::Handler::poll: 'state' unlocked for writing.");

        Ok(Async::NotReady)
    }
}
