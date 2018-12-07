//! Hydrabadger state.
//!
//! FIXME: Reorganize `Handler` and `State` to more clearly separate concerns.
//!

#![allow(dead_code)]

use super::{key_gen, Config, Error, InputOrMessage};
use crate::peer::Peers;
use crate::{ActiveNetworkInfo, Contribution, NetworkNodeInfo, NetworkState, Step, Uid};
use crossbeam::queue::SegQueue;
use hbbft::{
    crypto::{PublicKey, SecretKey},
    dynamic_honey_badger::{DynamicHoneyBadger, Error as DhbError, JoinPlan},
    sync_key_gen::Ack,
    NetworkInfo,
};
use rand::StdRng;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::{collections::BTreeMap, fmt};

/// A `State` discriminant.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StateDsct {
    Disconnected,
    DeterminingNetworkState,
    KeyGen,
    Observer,
    Validator,
}

impl fmt::Display for StateDsct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<StateDsct> for usize {
    fn from(dsct: StateDsct) -> usize {
        match dsct {
            StateDsct::Disconnected => 0,
            StateDsct::DeterminingNetworkState => 1,
            StateDsct::KeyGen => 4,
            StateDsct::Observer => 10,
            StateDsct::Validator => 11,
        }
    }
}

impl From<usize> for StateDsct {
    fn from(val: usize) -> StateDsct {
        match val {
            0 => StateDsct::Disconnected,
            1 => StateDsct::DeterminingNetworkState,
            4 => StateDsct::KeyGen,
            10 => StateDsct::Observer,
            11 => StateDsct::Validator,
            _ => panic!("Invalid state discriminant."),
        }
    }
}

/// The current hydrabadger state.
//
pub enum State<T: Contribution> {
    Disconnected {},
    DeterminingNetworkState {
        ack_queue: Option<SegQueue<(Uid, Ack)>>,
        iom_queue: Option<SegQueue<InputOrMessage<T>>>,
        network_state: Option<NetworkState>,
    },
    KeyGen {
        key_gen: key_gen::Machine,
        iom_queue: Option<SegQueue<InputOrMessage<T>>>,
    },
    Observer {
        dhb: Option<DynamicHoneyBadger<T, Uid>>,
    },
    Validator {
        dhb: Option<DynamicHoneyBadger<T, Uid>>,
    },
}

impl<T: Contribution> State<T> {
    /// Returns the state discriminant.
    pub(super) fn discriminant(&self) -> StateDsct {
        match self {
            State::Disconnected { .. } => StateDsct::Disconnected,
            State::DeterminingNetworkState { .. } => StateDsct::DeterminingNetworkState,
            State::KeyGen { .. } => StateDsct::KeyGen,
            State::Observer { .. } => StateDsct::Observer,
            State::Validator { .. } => StateDsct::Validator,
        }
    }
}

pub struct StateMachine<T: Contribution> {
    pub(crate) state: State<T>,
    pub(crate) dsct: Arc<AtomicUsize>,
}

impl<T: Contribution> StateMachine<T> {
    /// Returns a new `State::Disconnected`.
    pub(super) fn disconnected() -> StateMachine<T> {
        StateMachine {
            state: State::Disconnected {},
            dsct: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Sets the publicly visible state discriminant and returns the previous value.
    fn set_state_discriminant(&self) -> StateDsct {
        let sd = StateDsct::from(
            self.dsct
                .swap(self.state.discriminant().into(), Ordering::Release),
        );
        info!(
            "State has been set from '{}' to '{}'.",
            sd,
            self.state.discriminant()
        );
        sd
    }

    /// Sets the state to `AwaitingMorePeersForKeyGeneration`.
    pub(super) fn set_awaiting_more_peers(&mut self) {
        self.state = match self.state {
            State::Disconnected {} => {
                info!("Setting state: `KeyGen`.");
                State::KeyGen {
                    key_gen: key_gen::Machine::awaiting_peers(
                        SegQueue::new(),
                        None,
                        key_gen::InstanceId::BuiltIn,
                    ),
                    iom_queue: Some(SegQueue::new()),
                }
            }
            State::DeterminingNetworkState {
                ref mut iom_queue,
                ref mut ack_queue,
                ref network_state,
            } => {
                assert!(
                    !network_state.is_some(),
                    "State::set_awaiting_more_peers: Network is active!"
                );
                info!("Setting state: `KeyGen`.");
                State::KeyGen {
                    key_gen: key_gen::Machine::awaiting_peers(
                        ack_queue.take().unwrap(),
                        None,
                        key_gen::InstanceId::BuiltIn,
                    ),
                    iom_queue: iom_queue.take(),
                }
            }
            ref s => {
                debug!(
                    "State::set_awaiting_more_peers: Attempted to set \
                     `State::AwaitingMorePeersForKeyGeneration` while {}.",
                    s.discriminant()
                );
                return;
            }
        };
        self.set_state_discriminant();
    }

    /// Sets state to `DeterminingNetworkState` if
    /// `AwaitingMorePeersForKeyGeneration`, otherwise panics.
    pub(super) fn set_determining_network_state_active(&mut self, net_info: ActiveNetworkInfo) {
        self.state = match self.state {
            State::KeyGen {
                ref mut iom_queue, ..
            } => {
                info!("Setting state: `DeterminingNetworkState`.");
                State::DeterminingNetworkState {
                    ack_queue: Some(SegQueue::new()),
                    iom_queue: iom_queue.take(),
                    network_state: Some(NetworkState::Active(net_info)),
                }
            }
            _ => panic!(
                "Cannot reset network state when state is not `AwaitingMorePeersForKeyGeneration`."
            ),
        };
        self.set_state_discriminant();
    }

    /// Changes the variant (in-place) of this `State` to `Observer`.
    //
    // TODO: Add proper error handling:
    #[must_use]
    pub(super) fn set_observer(
        &mut self,
        local_uid: Uid,
        local_sk: SecretKey,
        jp: JoinPlan<Uid>,
        _cfg: &Config,
        step_queue: &SegQueue<Step<T>>,
    ) -> Result<SegQueue<InputOrMessage<T>>, Error> {
        let iom_queue_ret;
        self.state = match self.state {
            State::DeterminingNetworkState {
                ref mut iom_queue, ..
            } => {
                let (dhb, dhb_step) =
                    DynamicHoneyBadger::new_joining(local_uid, local_sk, jp, StdRng::new()?)?;
                step_queue.push(dhb_step);

                iom_queue_ret = iom_queue.take().unwrap();

                info!("");
                info!("== HONEY BADGER INITIALIZED ==");
                info!("");

                {
                    // TODO: Consolidate or remove:
                    let pk_set = dhb.netinfo().public_key_set();
                    let pk_map = dhb.netinfo().public_key_map();
                    info!("");
                    info!("");
                    info!("PUBLIC KEY: {:?}", pk_set.public_key());
                    info!("PUBLIC KEY SET: \n{:?}", pk_set);
                    info!("PUBLIC KEY MAP: \n{:?}", pk_map);
                    info!("");
                    info!("");
                }

                State::Observer { dhb: Some(dhb) }
            }
            ref s => panic!(
                "State::set_observer: State must be `GeneratingKeys`. \
                 State: {}",
                s.discriminant()
            ),
        };
        self.set_state_discriminant();
        Ok(iom_queue_ret)
    }

    /// Changes the variant (in-place) of this `State` to `Observer`.
    //
    // TODO: Add proper error handling:
    #[must_use]
    pub(super) fn set_validator(
        &mut self,
        local_uid: Uid,
        local_sk: SecretKey,
        peers: &Peers<T>,
        cfg: &Config,
        _step_queue: &SegQueue<Step<T>>,
    ) -> Result<SegQueue<InputOrMessage<T>>, Error> {
        let iom_queue_ret;
        self.state = match self.state {
            State::KeyGen {
                ref mut key_gen,
                ref mut iom_queue,
                ..
            } => {
                let (sync_key_gen, public_key) =
                    key_gen.complete().expect("Key generation incomplete");
                // let mut sync_key_gen = sync_key_gen.take().unwrap();
                assert_eq!(public_key, local_sk.public_key());

                let (pk_set, sk_share_opt) =
                    sync_key_gen.generate().map_err(Error::SyncKeyGenGenerate)?;
                let sk_share = sk_share_opt.unwrap();

                assert!(peers.count_validators() >= cfg.keygen_peer_count);

                let mut node_ids: BTreeMap<Uid, PublicKey> = peers
                    .validators()
                    .map(|p| (p.uid().cloned().unwrap(), p.public_key().cloned().unwrap()))
                    .collect();
                node_ids.insert(local_uid, local_sk.public_key());

                let netinfo = NetworkInfo::new(local_uid, sk_share, pk_set, local_sk, node_ids);

                let dhb = DynamicHoneyBadger::builder()
                    .era(cfg.start_epoch)
                    .build(netinfo);

                info!("");
                info!("== HONEY BADGER INITIALIZED ==");
                info!("");

                {
                    // TODO: Consolidate or remove:
                    let pk_set = dhb.netinfo().public_key_set();
                    let pk_map = dhb.netinfo().public_key_map();
                    info!("");
                    info!("");
                    info!("PUBLIC KEY: {:?}", pk_set.public_key());
                    info!("PUBLIC KEY SET: \n{:?}", pk_set);
                    info!("PUBLIC KEY MAP: \n{:?}", pk_map);
                    info!("");
                    info!("");
                }

                iom_queue_ret = iom_queue.take().unwrap();
                State::Validator { dhb: Some(dhb) }
            }
            ref s => panic!(
                "State::set_validator: State must be `GeneratingKeys`. State: {}",
                s.discriminant()
            ),
        };
        self.set_state_discriminant();
        Ok(iom_queue_ret)
    }

    pub(super) fn promote_to_validator(&mut self) -> Result<(), Error> {
        self.state = match self.state {
            State::Observer { ref mut dhb } => {
                info!("=== PROMOTING NODE TO VALIDATOR ===");
                State::Validator { dhb: dhb.take() }
            }
            ref s => panic!(
                "State::promote_to_validator: State must be `Observer`. State: {}",
                s.discriminant()
            ),
        };
        self.set_state_discriminant();
        Ok(())
    }

    /// Sets state to `DeterminingNetworkState` if `Disconnected`, otherwise does
    /// nothing.
    pub(super) fn update_peer_connection_added(&mut self, _peers: &Peers<T>) {
        self.state = match self.state {
            State::Disconnected {} => {
                info!("Setting state: `DeterminingNetworkState`.");
                State::DeterminingNetworkState {
                    ack_queue: Some(SegQueue::new()),
                    iom_queue: Some(SegQueue::new()),
                    network_state: None,
                }
            }
            _ => return,
        };
        self.set_state_discriminant();
    }

    /// Sets state to `Disconnected` if peer count is zero, otherwise does nothing.
    pub(super) fn update_peer_connection_dropped(&mut self, peers: &Peers<T>) {
        self.state = match self.state {
            State::DeterminingNetworkState { .. } => {
                if peers.count_total() == 0 {
                    State::Disconnected {}
                } else {
                    return;
                }
            }
            State::Disconnected { .. } => {
                error!("Received peer disconnection when `State::Disconnected`.");
                assert_eq!(peers.count_total(), 0);
                return;
            }
            State::KeyGen { ref key_gen, .. } => {
                if key_gen.is_awaiting_peers() {
                    debug!(
                        "Ignoring peer disconnection when \
                         `State::AwaitingMorePeersForKeyGeneration`."
                    );
                    return;
                } else {
                    panic!("FIXME: RESTART KEY GENERATION PROCESS AFTER PEER DISCONNECTS.");
                }
            }
            State::Observer { .. } => {
                debug!("Ignoring peer disconnection when `State::Observer`.");
                return;
            }
            State::Validator { .. } => {
                debug!("Ignoring peer disconnection when `State::Validator`.");
                return;
            }
        };
        self.set_state_discriminant();
    }

    /// Returns the network state, if possible.
    pub(super) fn network_state(&self, peers: &Peers<T>) -> NetworkState {
        use super::key_gen::State as KeyGenState;

        let peer_infos = peers
            .peers()
            .filter_map(|peer| {
                peer.pub_info()
                    .map(|(&uid, &in_addr, &pk)| NetworkNodeInfo { uid, in_addr, pk })
            })
            .collect::<Vec<_>>();
        match self.state {
            State::KeyGen { ref key_gen, .. } => match key_gen.state() {
                KeyGenState::AwaitingPeers { .. } => {
                    NetworkState::AwaitingMorePeersForKeyGeneration(peer_infos)
                }
                KeyGenState::Generating {
                    ref public_keys, ..
                } => NetworkState::GeneratingKeys(peer_infos, public_keys.clone()),
                _ => NetworkState::Unknown(peer_infos),
            },
            State::Observer { ref dhb } | State::Validator { ref dhb } => {
                // FIXME: Ensure that `peer_info` matches `NetworkInfo` from HB.
                let pk_set = dhb.as_ref().unwrap().netinfo().public_key_set().clone();
                let pk_map = dhb.as_ref().unwrap().netinfo().public_key_map().clone();
                NetworkState::Active((peer_infos, pk_set, pk_map))
            }
            _ => NetworkState::Unknown(peer_infos),
        }
    }

    /// Returns a reference to the internal HB instance.
    pub fn dhb(&self) -> Option<&DynamicHoneyBadger<T, Uid>> {
        match self.state {
            State::Observer { ref dhb, .. } => dhb.as_ref(),
            State::Validator { ref dhb, .. } => dhb.as_ref(),
            _ => None,
        }
    }

    /// Returns a reference to the internal HB instance.
    pub(super) fn dhb_mut(&mut self) -> Option<&mut DynamicHoneyBadger<T, Uid>> {
        match self.state {
            State::Observer { ref mut dhb, .. } => dhb.as_mut(),
            State::Validator { ref mut dhb, .. } => dhb.as_mut(),
            _ => None,
        }
    }

    /// Returns a reference to the key generation instance.
    pub(super) fn key_gen(&self) -> Option<&key_gen::Machine> {
        match self.state {
            State::KeyGen { ref key_gen, .. } => Some(key_gen),
            _ => None,
        }
    }

    /// Returns a reference to the key generation instance.
    pub(super) fn key_gen_mut(&mut self) -> Option<&mut key_gen::Machine> {
        match self.state {
            State::KeyGen {
                ref mut key_gen, ..
            } => Some(key_gen),
            _ => None,
        }
    }

    /// Presents a message, vote or contribution to HoneyBadger or queues it for later.
    ///
    /// Cannot be called while disconnected or connection-pending.
    pub(super) fn handle_iom(
        &mut self,
        iom: InputOrMessage<T>,
    ) -> Option<Result<Step<T>, DhbError>> {
        match self.state {
            State::Observer { ref mut dhb, .. } | State::Validator { ref mut dhb, .. } => {
                trace!("State::handle_iom: Handling: {:?}", iom);
                let step_opt = Some({
                    let dhb = dhb.as_mut().unwrap();
                    match iom {
                        InputOrMessage::Contribution(contrib) => dhb.propose(contrib),
                        InputOrMessage::Change(change) => dhb.vote_for(change),
                        InputOrMessage::Message(src_uid, msg) => dhb.handle_message(&src_uid, msg),
                    }
                });

                match step_opt {
                    Some(ref step) => match step {
                        Ok(s) => trace!("State::handle_iom: DHB output: {:?}", s.output),
                        Err(err) => error!("State::handle_iom: DHB output error: {:?}", err),
                    },
                    None => trace!("State::handle_iom: DHB Output is `None`"),
                }

                return step_opt;
            }
            State::KeyGen { ref iom_queue, .. }
            | State::DeterminingNetworkState { ref iom_queue, .. } => {
                trace!("State::handle_iom: Queueing: {:?}", iom);
                iom_queue.as_ref().unwrap().push(iom);
            }
            ref s => panic!(
                "State::handle_iom: Must be connected in order to input to \
                 honey badger. State: {}",
                s.discriminant()
            ),
        }
        None
    }

    /// Returns the state discriminant.
    pub(super) fn discriminant(&self) -> StateDsct {
        self.state.discriminant()
    }
}
