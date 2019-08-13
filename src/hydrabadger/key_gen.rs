//! Synchronous distributed key generation.

use super::Error;
use crate::hydrabadger::hydrabadger::Hydrabadger;
use crate::peer::Peers;
use crate::{Contribution, NetworkState, NodeId, Uid, WireMessage};
use crossbeam::queue::SegQueue;
use futures::sync::mpsc;
use hbbft::{
    crypto::{PublicKey, SecretKey},
    sync_key_gen::{Ack, AckOutcome, Part, PartOutcome, SyncKeyGen},
};
use rand::{self, FromEntropy};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum InstanceId {
    BuiltIn,
    User(Uid),
}

/// Messages used during synchronous key generation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessageKind {
    Part(Part),
    Ack(Ack),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    kind: MessageKind,
}

impl Message {
    pub fn part(part: Part) -> Message {
        Message {
            kind: MessageKind::Part(part),
        }
    }

    pub fn ack(ack: Ack) -> Message {
        Message {
            kind: MessageKind::Ack(ack),
        }
    }

    pub fn kind(&self) -> &MessageKind {
        &self.kind
    }

    pub fn into_kind(self) -> MessageKind {
        self.kind
    }
}

/// Key generation state.
#[derive(Debug)]
pub(super) enum State<N> {
    AwaitingPeers {
        required_peers: Vec<N>,
        available_peers: Vec<N>,
    },
    Generating {
        sync_key_gen: Option<SyncKeyGen<N>>,
        public_key: Option<PublicKey>,
        public_keys: BTreeMap<N, PublicKey>,
        part_count: usize,
        ack_count: usize,
    },
    Complete {
        sync_key_gen: Option<SyncKeyGen<N>>,
        public_key: Option<PublicKey>,
    },
}

/// Forwards an `Ack` to a `SyncKeyGen` instance.
fn handle_ack<N: NodeId>(
    nid: &N,
    ack: Ack,
    ack_count: &mut usize,
    sync_key_gen: &mut SyncKeyGen<N>,
) {
    trace!("KEY GENERATION: Handling ack from '{:?}'...", nid);
    let ack_outcome = sync_key_gen
        .handle_ack(nid, ack.clone())
        .expect("Failed to handle Ack.");
    match ack_outcome {
        AckOutcome::Invalid(fault) => error!("Error handling ack: '{:?}':\n{:?}", ack, fault),
        AckOutcome::Valid => *ack_count += 1,
    }
}

/// Forwards all queued `Ack`s to a `SyncKeyGen` instance if `part_count` is
/// sufficient.
fn handle_queued_acks<C: Contribution, N: NodeId>(
    ack_queue: &SegQueue<(N, Ack)>,
    part_count: usize,
    ack_count: &mut usize,
    sync_key_gen: &mut SyncKeyGen<N>,
    peers: &Peers<C, N>,
) {
    if part_count == peers.count_validators() + 1 {
        trace!("KEY GENERATION: Handling queued acks...");

        debug!("   Peers complete: {}", sync_key_gen.count_complete());
        debug!("   Part count: {}", part_count);
        debug!("   Ack count: {}", ack_count);

        while let Some((nid, ack)) = ack_queue.try_pop() {
            handle_ack(&nid, ack, ack_count, sync_key_gen);
        }
    }
}

/// Manages the key generation state.
#[derive(Debug)]
pub struct Machine<N> {
    state: State<N>,
    ack_queue: SegQueue<(N, Ack)>,
    event_tx: Option<mpsc::UnboundedSender<Message>>,
    instance_id: InstanceId,
}

impl<N: NodeId> Machine<N> {
    /// Creates and returns a new `Machine` in the `AwaitingPeers`
    /// state.
    pub fn awaiting_peers(
        ack_queue: SegQueue<(N, Ack)>,
        event_tx: Option<mpsc::UnboundedSender<Message>>,
        instance_id: InstanceId,
    ) -> Machine<N> {
        Machine {
            state: State::AwaitingPeers {
                required_peers: Vec::new(),
                available_peers: Vec::new(),
            },
            ack_queue,
            event_tx,
            instance_id,
        }
    }

    /// Creates and returns a new `Machine` in the `Generating`
    /// state.
    pub fn generate<C: Contribution>(
        local_nid: &N,
        local_sk: SecretKey,
        peers: &Peers<C, N>,
        event_tx: mpsc::UnboundedSender<Message>,
        instance_id: InstanceId,
    ) -> Result<Machine<N>, Error> {
        let mut m = Machine {
            state: State::AwaitingPeers {
                required_peers: Vec::new(),
                available_peers: Vec::new(),
            },
            ack_queue: SegQueue::new(),
            event_tx: Some(event_tx),
            instance_id: instance_id.clone(),
        };

        let (part, ack) = m.set_generating_keys(local_nid, local_sk, peers)?;

        peers.wire_to_validators(WireMessage::key_gen_part(instance_id.clone(), part));
        peers.wire_to_validators(WireMessage::key_gen_ack(instance_id, ack));

        Ok(m)
    }

    /// Sets the state to `AwaitingMorePeersForKeyGeneration`.
    pub(super) fn set_generating_keys<C: Contribution>(
        &mut self,
        local_nid: &N,
        local_sk: SecretKey,
        peers: &Peers<C, N>,
    ) -> Result<(Part, Ack), Error> {
        let (part, ack);
        self.state = match self.state {
            State::AwaitingPeers { .. } => {
                // let threshold = config.keygen_peer_count / 3;
                let threshold = peers.count_validators() / 3;

                let mut public_keys: BTreeMap<N, PublicKey> = peers
                    .validators()
                    .map(|p| p.pub_info().map(|(nid, _, pk)| (nid.clone(), *pk)).unwrap())
                    .collect();

                let pk = local_sk.public_key();
                public_keys.insert(local_nid.clone(), pk);

                let mut rng = rand::rngs::StdRng::from_entropy();

                let (mut sync_key_gen, opt_part) = SyncKeyGen::new(
                    local_nid.clone(),
                    local_sk,
                    public_keys.clone(),
                    threshold,
                    &mut rng,
                )
                .map_err(Error::SyncKeyGenNew)?;
                part = opt_part.expect("This node is not a validator (somehow)!");

                trace!("KEY GENERATION: Handling our own `Part`...");
                ack = match sync_key_gen
                    .handle_part(&local_nid, part.clone(), &mut rng)
                    .expect("Handling our own Part has failed")
                {
                    PartOutcome::Valid(Some(ack)) => ack,
                    PartOutcome::Invalid(faults) => {
                        panic!("Invalid part (FIXME: handle): {:?}", faults)
                    }
                    PartOutcome::Valid(None) => panic!("No Ack produced when handling Part."),
                };

                trace!("KEY GENERATION: Queueing our own `Ack`...");
                self.ack_queue.push((local_nid.clone(), ack.clone()));

                State::Generating {
                    sync_key_gen: Some(sync_key_gen),
                    public_key: Some(pk),
                    public_keys,
                    part_count: 1,
                    ack_count: 0,
                }
            }
            _ => panic!(
                "State::set_generating_keys: \
                 Must be State::AwaitingMorePeersForKeyGeneration"
            ),
        };
        Ok((part, ack))
    }

    /// Notify this key generation instance that peers have been added.
    //
    // TODO: Move some of this logic back to handler.
    pub(super) fn add_peers<C: Contribution>(
        &mut self,
        peers: &Peers<C, N>,
        hdb: &Hydrabadger<C, N>,
        net_state: NetworkState<N>,
    ) -> Result<(), Error> {
        match self.state {
            State::AwaitingPeers { .. } => {
                if peers.count_validators() >= hdb.config().keygen_peer_count {
                    info!("BEGINNING KEY GENERATION");

                    let local_nid = hdb.node_id().clone();
                    let local_in_addr = *hdb.addr();
                    let local_pk = hdb.secret_key().public_key();

                    let (part, ack) =
                        self.set_generating_keys(&local_nid, hdb.secret_key().clone(), peers)?;

                    trace!("KEY GENERATION: Sending initial parts and our own ack.");
                    peers.wire_to_validators(WireMessage::hello_from_validator(
                        local_nid,
                        local_in_addr,
                        local_pk,
                        net_state,
                    ));
                    peers.wire_to_validators(WireMessage::key_gen_part(
                        self.instance_id.clone(),
                        part,
                    ));
                    peers.wire_to_validators(WireMessage::key_gen_ack(
                        self.instance_id.clone(),
                        ack,
                    ));
                }
            }
            State::Generating { .. } => {
                // This *could* be called multiple times when initially
                // establishing outgoing connections. Do nothing for now but
                // redesign this at some point.
                warn!("Ignoring new established peer signal while key gen `State::Generating`.");
            }
            State::Complete { .. } => {
                warn!("Ignoring new established peer signal while key gen `State::Complete`.");
            }
        }
        Ok(())
    }

    /// Handles a received `Part`.
    pub(super) fn handle_key_gen_part<C: Contribution>(
        &mut self,
        src_nid: &N,
        part: Part,
        peers: &Peers<C, N>,
    ) {
        match self.state {
            State::Generating {
                ref mut sync_key_gen,
                ref mut part_count,
                ref mut ack_count,
                ..
            } => {
                // TODO: Move this match block into a function somewhere for re-use:
                trace!("KEY GENERATION: Handling part from '{:?}'...", src_nid);
                let mut rng = rand::rngs::OsRng::new().expect("Creating OS Rng has failed");
                let skg = sync_key_gen.as_mut().unwrap();
                let ack = match skg.handle_part(src_nid, part, &mut rng) {
                    Ok(PartOutcome::Valid(Some(ack))) => ack,
                    Ok(PartOutcome::Invalid(faults)) => panic!(
                        "Invalid part \
                         (FIXME: handle): {:?}",
                        faults
                    ),
                    Ok(PartOutcome::Valid(None)) => {
                        error!("`DynamicHoneyBadger::handle_part` returned `None`.");
                        return;
                    }
                    Err(err) => {
                        error!("Error handling Part: {:?}", err);
                        return;
                    }
                };

                *part_count += 1;

                trace!("KEY GENERATION: Queueing `Ack`.");
                self.ack_queue.push((src_nid.clone(), ack.clone()));

                trace!(
                    "KEY GENERATION: Part from '{:?}' acknowledged. Broadcasting ack...",
                    src_nid
                );
                peers.wire_to_validators(WireMessage::key_gen_ack(self.instance_id.clone(), ack));

                debug!("   Peers complete: {}", skg.count_complete());
                debug!("   Part count: {}", part_count);
                debug!("   Ack count: {}", ack_count);

                handle_queued_acks(&self.ack_queue, *part_count, ack_count, skg, peers)
            }
            ref s => panic!(
                "::handle_key_gen_part: State must be `GeneratingKeys`. \
                 State: \n{:?} \n\n[FIXME: Enqueue these parts!]\n\n",
                s
            ),
        }
    }

    /// Handles a received `Ack`.
    pub(super) fn handle_key_gen_ack<C: Contribution>(
        &mut self,
        src_nid: &N,
        ack: Ack,
        peers: &Peers<C, N>,
    ) -> Result<bool, Error> {
        let mut complete: Option<(SyncKeyGen<N>, PublicKey)> = None;

        match self.state {
            State::Generating {
                ref mut sync_key_gen,
                ref mut public_key,
                ref part_count,
                ref mut ack_count,
                ..
            } => {
                {
                    let skg = sync_key_gen.as_mut().unwrap();

                    trace!("KEY GENERATION: Queueing `Ack`.");
                    self.ack_queue.push((src_nid.clone(), ack.clone()));

                    handle_queued_acks(&self.ack_queue, *part_count, ack_count, skg, peers);
                };

                let node_n = peers.count_validators() + 1;

                if sync_key_gen.as_ref().unwrap().count_complete() == node_n
                    && *ack_count >= node_n * node_n
                {
                    let skg = sync_key_gen.take().unwrap();
                    info!("KEY GENERATION: All acks received and handled.");
                    debug!("   Peers complete: {}", skg.count_complete());
                    debug!("   Part count: {}", part_count);
                    debug!("   Ack count: {}", ack_count);

                    assert!(skg.is_ready());
                    complete = public_key.take().map(|pk| (skg, pk))
                }
            }
            _ => panic!("::handle_key_gen_ack: KeyGen state must be `Generating`."),
        }

        match complete {
            Some((sync_key_gen, public_key)) => {
                self.state = State::Complete {
                    sync_key_gen: Some(sync_key_gen),
                    public_key: Some(public_key),
                };
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Returns the state of this key generation instance.
    pub(super) fn state(&self) -> &State<N> {
        &self.state
    }

    /// Returns true if this key generation instance is awaiting more peers.
    pub(super) fn is_awaiting_peers(&self) -> bool {
        match self.state {
            State::AwaitingPeers { .. } => true,
            _ => false,
        }
    }

    /// Returns the `SyncKeyGen` instance and `PublicKey` if this key
    /// generation instance is complete.
    pub(super) fn complete(&mut self) -> Option<(SyncKeyGen<N>, PublicKey)> {
        match self.state {
            State::Complete {
                ref mut sync_key_gen,
                ref mut public_key,
            } => sync_key_gen
                .take()
                .and_then(|skg| public_key.take().map(|pk| (skg, pk))),
            _ => None,
        }
    }

    pub(super) fn event_tx(&self) -> Option<&mpsc::UnboundedSender<Message>> {
        self.event_tx.as_ref()
    }
}
