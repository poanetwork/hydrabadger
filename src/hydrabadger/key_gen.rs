
use hydrabadger::hydrabadger::Hydrabadger;
use crossbeam::queue::SegQueue;
use hbbft::{
    crypto::{PublicKey, SecretKey},
    sync_key_gen::{Ack, AckOutcome, Part, PartOutcome, SyncKeyGen},
};
use peer::Peers;
use std::{collections::BTreeMap};
use rand;
use super::{Config, Error};
use {Contribution, NetworkState, Uid, WireMessage};


/// Key generation state.
#[derive(Debug)]
pub(super) enum State {
    AwaitingPeers {
        required_peers: Vec<Uid>,
        available_peers: Vec<Uid>,
    },
    Generating {
        sync_key_gen: Option<SyncKeyGen<Uid>>,
        public_key: Option<PublicKey>,
        public_keys: BTreeMap<Uid, PublicKey>,

        part_count: usize,
        ack_count: usize,
    },
    Complete {
    	sync_key_gen: Option<SyncKeyGen<Uid>>,
        public_key: Option<PublicKey>,
    },
}

/// Forwards an `Ack` to a `SyncKeyGen` instance.
fn handle_ack(
    uid: &Uid,
    ack: Ack,
    ack_count: &mut usize,
    sync_key_gen: &mut SyncKeyGen<Uid>,
) {
    info!("KEY GENERATION: Handling ack from '{}'...", uid);
    let ack_outcome = sync_key_gen.handle_ack(uid, ack.clone()).expect("Failed to handle Ack.");
    match ack_outcome {
        AckOutcome::Invalid(fault) => error!("Error handling ack: '{:?}':\n{:?}", ack, fault),
        AckOutcome::Valid => *ack_count += 1,
    }
}

/// Forwards all queued `Ack`s to a `SyncKeyGen` instance if `part_count` is
/// sufficient.
fn handle_queued_acks<T: Contribution>(
    ack_queue: &SegQueue<(Uid, Ack)>,
    part_count: usize,
    ack_count: &mut usize,
    sync_key_gen: &mut SyncKeyGen<Uid>,
    hdb: &Hydrabadger<T>,
) {
    if part_count == hdb.config().keygen_peer_count + 1 {
        info!("KEY GENERATION: Handling queued acks...");

        debug!("   Peers complete: {}", sync_key_gen.count_complete());
        debug!("   Part count: {}", part_count);
        debug!("   Ack count: {}", ack_count);

        while let Some((uid, ack)) = ack_queue.try_pop() {
            handle_ack(&uid, ack, ack_count, sync_key_gen);
        }
    }
}

/// Manages the key generation state.
#[derive(Debug)]
pub struct KeyGenMachine {
    state: State,
    ack_queue: SegQueue<(Uid, Ack)>,
}

impl KeyGenMachine {
	/// Creates and returns a new `KeyGenMachine` in the `AwaitingPeers`
	/// state.
	pub fn awaiting_peers(ack_queue: SegQueue<(Uid, Ack)>)
		-> KeyGenMachine
	{
		KeyGenMachine {
			state: State::AwaitingPeers {
				required_peers: Vec::new(),
				available_peers: Vec::new(),
			},
			ack_queue: ack_queue,
		}
	}

	/// Sets the state to `AwaitingMorePeersForKeyGeneration`.
    pub(super) fn set_generating_keys<T: Contribution>(
        &mut self,
        local_uid: &Uid,
        local_sk: SecretKey,
        peers: &Peers<T>,
        config: &Config,
    ) -> Result<(Part, Ack), Error> {
        let (part, ack);
        self.state = match self.state {
            State::AwaitingPeers {
                ..
            } => {
                let threshold = config.keygen_peer_count / 3;

                let mut public_keys: BTreeMap<Uid, PublicKey> = peers
                    .validators()
                    .map(|p| p.pub_info().map(|(uid, _, pk)| (*uid, *pk)).unwrap())
                    .collect();

                let pk = local_sk.public_key();
                public_keys.insert(*local_uid, pk);

                let mut rng = rand::OsRng::new().expect("Creating OS Rng has failed");

                let (mut sync_key_gen, opt_part) =
                    SyncKeyGen::new(&mut rng, *local_uid, local_sk, public_keys.clone(), threshold)
                        .map_err(Error::SyncKeyGenNew)?;
                part = opt_part.expect("This node is not a validator (somehow)!");

                info!("KEY GENERATION: Handling our own `Part`...");
                ack = match sync_key_gen.handle_part(&mut rng, &local_uid, part.clone())
                                        .expect("Handling our own Part has failed") {
                    PartOutcome::Valid(Some(ack)) => ack,
                    PartOutcome::Invalid(faults) => panic!(
                        "Invalid part \
                         (FIXME: handle): {:?}",
                        faults
                    ),
                    PartOutcome::Valid(None) => panic!("No Ack produced when handling Part."),
                };

                info!("KEY GENERATION: Queueing our own `Ack`...");
                self.ack_queue.push((*local_uid, ack.clone()));

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
	pub(super) fn add_peers<T: Contribution>(
        &mut self,
        peers: &Peers<T>,
        hdb: &Hydrabadger<T>,
        net_state: NetworkState,
    ) -> Result<(), Error> {
		match self.state {
			State::AwaitingPeers { .. } => {
                if peers.count_validators() >= hdb.config().keygen_peer_count {
                    info!("== BEGINNING KEY GENERATION ==");

                    let local_uid = *hdb.uid();
                    let local_in_addr = *hdb.addr();
                    let local_sk = hdb.secret_key().public_key();

                    let (part, ack) = self.set_generating_keys(
                        &local_uid,
                        hdb.secret_key().clone(),
                        peers,
                        hdb.config(),
                    )?;

                    info!("KEY GENERATION: Sending initial parts and our own ack.");
                    peers.wire_to_validators(
                        WireMessage::hello_from_validator(
                            local_uid,
                            local_in_addr,
                            local_sk,
                            net_state,
                        ),
                    );
                    peers.wire_to_validators(WireMessage::key_gen_part(part));
                    peers.wire_to_validators(WireMessage::key_gen_ack(ack));
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
	pub(super) fn handle_key_gen_part<T: Contribution>(&mut self, src_uid: &Uid, part: Part, hdb: &Hydrabadger<T>) {
        match self.state {
            State::Generating {
                ref mut sync_key_gen,
                ref mut part_count,
                ref mut ack_count,
                ..
            } => {
                // TODO: Move this match block into a function somewhere for re-use:
                info!("KEY GENERATION: Handling part from '{}'...", src_uid);
                let mut rng = rand::OsRng::new().expect("Creating OS Rng has failed");
                let mut skg = sync_key_gen.as_mut().unwrap();
                let ack = match skg.handle_part(&mut rng, src_uid, part) {
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

                info!("KEY GENERATION: Queueing `Ack`.");
                self.ack_queue.push((*src_uid, ack.clone()));

                let peers = hdb.peers();
                info!(
                    "KEY GENERATION: Part from '{}' acknowledged. Broadcasting ack...",
                    src_uid
                );
                peers.wire_to_validators(WireMessage::key_gen_ack(ack));

                debug!("   Peers complete: {}", skg.count_complete());
                debug!("   Part count: {}", part_count);
                debug!("   Ack count: {}", ack_count);

                handle_queued_acks(&self.ack_queue, *part_count, ack_count, skg, hdb)
            }
            ref s => panic!(
                "::handle_key_gen_part: State must be `GeneratingKeys`. \
                 State: \n{:?} \n\n[FIXME: Enqueue these parts!]\n\n",
                s
            ),
        }
    }

    /// Handles a received `Ack`.
    pub(super) fn handle_key_gen_ack<T: Contribution>(
        &mut self,
        src_uid: &Uid,
        ack: Ack,
        hdb: &Hydrabadger<T>,
    ) -> Result<bool, Error> {
        let mut complete: Option<(SyncKeyGen<Uid>, PublicKey)> = None;

        match self.state {
            State::Generating {
                ref mut sync_key_gen,
                ref mut public_key,
                ref part_count,
                ref mut ack_count,
                ..
            } => {
        		let node_n = {
	                let mut skg = sync_key_gen.as_mut().unwrap();

	                info!("KEY GENERATION: Queueing `Ack`.");
	                self.ack_queue.push((*src_uid, ack.clone()));

	                handle_queued_acks(&self.ack_queue, *part_count, ack_count, skg, hdb);

	                hdb.config().keygen_peer_count + 1
	            };

                if sync_key_gen.as_ref().unwrap().count_complete() == node_n && *ack_count >= node_n * node_n {
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
	        	self.state = State::Complete { sync_key_gen: Some(sync_key_gen), public_key: Some(public_key) };
	        	Ok(true)
	        },
	        None => Ok(false),
        }
    }

    /// Returns the state of this key generation instance.
    pub(super) fn state(&self) -> &State {
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
    pub(super) fn complete(&mut self) -> Option<(SyncKeyGen<Uid>, PublicKey)> {
    	match self.state {
    		State::Complete { ref mut sync_key_gen, ref mut public_key } => {
    			sync_key_gen.take().and_then(|skg| public_key.take().map(|pk| (skg, pk)))
    		}
    		_ => None
    	}
    }
}