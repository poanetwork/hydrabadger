//! Hydrabadger state.
//!
//! FIXME: Reorganize `Handler` and `State` to more clearly separate concerns.
//!

#![allow(dead_code)]

use std::{
    fmt,
    collections::BTreeMap,
};
use crossbeam::sync::SegQueue;
use hbbft::{
    crypto::{PublicKey, SecretKey, /*PublicKeySet*/},
    sync_key_gen::{SyncKeyGen, Part, PartOutcome, Ack},
    messaging::{DistAlgorithm, NetworkInfo},
    queueing_honey_badger::{Error as QhbError, QueueingHoneyBadger},
    dynamic_honey_badger::{DynamicHoneyBadger, DynamicHoneyBadgerBuilder, JoinPlan},

};
use peer::Peers;
use ::{Uid, NetworkState, NetworkNodeInfo, Message, Transaction, Step, Input};
use super::{InputOrMessage, Error};
use super::{BATCH_SIZE, HB_PEER_MINIMUM_COUNT};


/// A `State` discriminant.
#[derive(Copy, Clone, Debug)]
pub enum StateDsct {
    Disconnected,
    DeterminingNetworkState,
    AwaitingMorePeersForKeyGeneration,
    GeneratingKeys,
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
            StateDsct::AwaitingMorePeersForKeyGeneration => 2,
            StateDsct::GeneratingKeys => 3,
            StateDsct::Observer => 4,
            StateDsct::Validator => 5,
        }
    }
}

impl From<usize> for StateDsct {
    fn from(val: usize) -> StateDsct {
        match val {
            0 => StateDsct::Disconnected,
            1 => StateDsct::DeterminingNetworkState,
            2 => StateDsct::AwaitingMorePeersForKeyGeneration,
            3 => StateDsct::GeneratingKeys,
            4 => StateDsct::Observer,
            5 => StateDsct::Validator,
            _ => panic!("Invalid state discriminant."),
        }
    }
}


// The current hydrabadger state.
pub(crate) enum State {
    Disconnected { },
    DeterminingNetworkState {
        iom_queue: Option<SegQueue<InputOrMessage>>,
    },
    AwaitingMorePeersForKeyGeneration {
        // Queued input to HoneyBadger:
        // FIXME: ACTUALLY USE THIS QUEUED INPUT!
        iom_queue: Option<SegQueue<InputOrMessage>>,
    },
    GeneratingKeys {
        sync_key_gen: Option<SyncKeyGen<Uid>>,
        public_key: Option<PublicKey>,
        ack_count: usize,
        // Queued input to HoneyBadger:
        // FIXME: ACTUALLY USE THIS QUEUED INPUT!
        iom_queue: Option<SegQueue<InputOrMessage>>,
    },
    Observer {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uid>>,
    },
    Validator {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uid>>,
    },
}

impl State {
    /// Returns the state discriminant.
    pub(super) fn discriminant(&self) -> StateDsct {
        match self {
            State::Disconnected { .. } => StateDsct::Disconnected,
            State::DeterminingNetworkState { .. } => StateDsct::DeterminingNetworkState,
            State::AwaitingMorePeersForKeyGeneration { .. } =>
                StateDsct::AwaitingMorePeersForKeyGeneration,
            State::GeneratingKeys{ .. } => StateDsct::GeneratingKeys,
            State::Observer { .. } => StateDsct::Observer,
            State::Validator { .. } => StateDsct::Validator,
        }
    }

    /// Returns a new `State::Disconnected`.
    pub(super) fn disconnected(/*local_uid: Uid, local_addr: InAddr,*/ /*secret_key: SecretKey*/)
            -> State {
        State::Disconnected { /*secret_key: secret_key*/ }
    }

    // /// Sets the state to `DeterminingNetworkState`.
    // //
    // // TODO: Add proper error handling:
    // fn set_determining_network_state(&mut self) {
    //     *self = match self {
    //         State::Disconnected { } => {
    //             info!("Setting state: `DeterminingNetworkState`.");
    //             State::DeterminingNetworkState { }
    //         },
    //         _ => panic!("Must be disconnected before calling `::peer_connection_added`."),
    //     };
    // }

    /// Sets the state to `AwaitingMorePeersForKeyGeneration`.
    pub(super) fn set_awaiting_more_peers(&mut self) {
        *self = match self {
            State::Disconnected { } => {
                info!("Setting state: `AwaitingMorePeersForKeyGeneration`.");
                State::AwaitingMorePeersForKeyGeneration { iom_queue: Some(SegQueue::new()) }
            },
            State::DeterminingNetworkState { ref mut iom_queue } => {
                info!("Setting state: `AwaitingMorePeersForKeyGeneration`.");
                State::AwaitingMorePeersForKeyGeneration { iom_queue: iom_queue.take() }
            }
            _ => {
                debug!("Attempted to set `State::AwaitingMorePeersForKeyGeneration` \
                    while connected.");
                return
            }
        };
    }

    /// Sets the state to `AwaitingMorePeersForKeyGeneration`.
    pub(super) fn set_generating_keys(&mut self, local_uid: &Uid, local_sk: SecretKey, peers: &Peers)
            -> (Part, Ack) {
        let (part, ack);
        *self = match self {
            State::AwaitingMorePeersForKeyGeneration { ref mut iom_queue } => {
                // let secret_key = secret_key.clone();
                let threshold = HB_PEER_MINIMUM_COUNT / 3;

                let mut public_keys: BTreeMap<Uid, PublicKey> = peers.validators().map(|p| {
                    p.pub_info().map(|(uid, _, pk)| (*uid, *pk)).unwrap()
                }).collect();

                let pk = local_sk.public_key();
                public_keys.insert(*local_uid, pk);

                let (mut sync_key_gen, opt_part) = SyncKeyGen::new(*local_uid, local_sk,
                    public_keys.clone(), threshold);
                part = opt_part.expect("This node is not a validator (somehow)!");

                info!("KEY GENERATION: Handling our own `Part`...");
                ack = match sync_key_gen.handle_part(&local_uid, part.clone()) {
                    Some(PartOutcome::Valid(ack)) => ack,
                    Some(PartOutcome::Invalid(faults)) => panic!("Invalid part \
                        (FIXME: handle): {:?}", faults),
                    None => unimplemented!(),
                };

                info!("KEY GENERATION: Handling our own `Ack`...");
                let fault_log = sync_key_gen.handle_ack(local_uid, ack.clone());
                if !fault_log.is_empty() {
                    error!("Errors acknowledging part (from self):\n {:?}", fault_log);
                }

                State::GeneratingKeys { sync_key_gen: Some(sync_key_gen),
                    public_key: Some(pk), ack_count: 1, iom_queue: iom_queue.take() }
            },
            _ => panic!("State::set_generating_keys: \
                Must be State::AwaitingMorePeersForKeyGeneration"),
        };

        (part, ack)
    }


    // /// Changes the variant (in-place) of this `State` to `Observer`.
    // //
    // // TODO: Add proper error handling:
    // pub(super) fn set_observer(&mut self) {
    //     *self = match self {
    //         State::DeterminingNetworkState { .. } => {
    //             State::Observer { qhb: None }
    //         },
    //         _ => panic!("Must be `State::DeterminingNetworkState` before becoming \
    //             `State::Observer`."),
    //     };
    //     // unimplemented!()
    // }

    /// Changes the variant (in-place) of this `State` to `Observer`.
    //
    // TODO: Add proper error handling:
    #[must_use]
    pub(super) fn set_observer(&mut self, local_uid: Uid, local_sk: SecretKey,
            // _net_node_info: Vec<NetworkNodeInfo>, pk_set: PublicKeySet, pk_map: BTreeMap<Uid, PublicKey>
            jp: JoinPlan<Uid>) -> Result<SegQueue<InputOrMessage>, Error> {
        let iom_queue_ret;
        *self = match self {
            State::DeterminingNetworkState { ref mut iom_queue } => {
                // let sk_share = local_sk.clone().into();

                // let netinfo = NetworkInfo::new(
                //     local_uid,
                //     sk_share,
                //     pk_set,
                //     local_sk,
                //     pk_map,
                // );

                // let dhb = DynamicHoneyBadger::builder(netinfo)
                //     .build()
                //     .expect("Error instantiating DynamicHoneyBadger");

                let dhb = DynamicHoneyBadgerBuilder::new_joining(local_uid, local_sk, jp)
                    .build()?;

                let qhb = QueueingHoneyBadger::builder(dhb).batch_size(BATCH_SIZE).build();

                info!("== HONEY BADGER INITIALIZED ==");
                iom_queue_ret = iom_queue.take().unwrap();
                State::Observer { qhb: Some(qhb) }
            },
            s @ _ => panic!("State::set_observer: State must be `GeneratingKeys`. \
                State: {}", s.discriminant()),
        };
        Ok(iom_queue_ret)
    }

    /// Changes the variant (in-place) of this `State` to `Observer`.
    //
    // TODO: Add proper error handling:
    #[must_use]
    pub(super) fn set_validator(&mut self, local_uid: Uid, local_sk: SecretKey, peers: &Peers)
            -> Result<SegQueue<InputOrMessage>, Error> {
        let iom_queue_ret;
        *self = match self {
            State::GeneratingKeys { ref mut sync_key_gen, mut public_key,
                    ref mut iom_queue, .. } => {
                let mut sync_key_gen = sync_key_gen.take().unwrap();
                assert_eq!(public_key.take().unwrap(), local_sk.public_key());

                let (pk_set, sk_share_opt) = sync_key_gen.generate();
                let sk_share = sk_share_opt.unwrap();

                assert!(peers.count_validators() >= HB_PEER_MINIMUM_COUNT);

                let mut node_ids: BTreeMap<Uid, PublicKey> = peers.validators().map(|p| {
                    (p.uid().cloned().unwrap(), p.public_key().cloned().unwrap())
                }).collect();
                node_ids.insert(local_uid, local_sk.public_key());

                let netinfo = NetworkInfo::new(
                    local_uid,
                    sk_share,
                    pk_set,
                    local_sk,
                    node_ids,
                );

                let dhb = DynamicHoneyBadger::builder(netinfo)
                    .build()
                    .expect("instantiate DHB");
                let qhb = QueueingHoneyBadger::builder(dhb).batch_size(BATCH_SIZE).build();

                info!("== HONEY BADGER INITIALIZED ==");
                iom_queue_ret = iom_queue.take().unwrap();
                State::Validator { qhb: Some(qhb) }
            }
            s @ _ => panic!("State::set_validator: State must be `GeneratingKeys`. \
                State: {}", s.discriminant()),
        };
        Ok(iom_queue_ret)
    }

    /// Sets state to `DeterminingNetworkState` if `Disconnected`, otherwise does
    /// nothing.
    pub(super) fn update_peer_connection_added(&mut self, _peers: &Peers) {
        let _dsct = self.discriminant();
        *self = match self {
            State::Disconnected { } => {
                info!("Setting state: `DeterminingNetworkState`.");
                State::DeterminingNetworkState { iom_queue: Some(SegQueue::new()) }
            },
            _ => return,
        };
    }

    /// Sets state to `Disconnected` if peer count is zero, otherwise does nothing.
    pub(super) fn update_peer_connection_dropped(&mut self, peers: &Peers) {
        *self = match self {
            State::DeterminingNetworkState { .. } => {
                if peers.count_total() == 0 {
                    State::Disconnected { }
                } else {
                    return;
                }
            },
            State::Disconnected { .. } => {
                error!("Received peer disconnection when `State::Disconnected`.");
                assert_eq!(peers.count_total(), 0);
                return;
            },
            State::AwaitingMorePeersForKeyGeneration { .. } => {
                debug!("Ignoring peer disconnection when \
                    `State::AwaitingMorePeersForKeyGeneration`.");
                return;
            },
            State::GeneratingKeys { .. } => {
                panic!("FIXME: RESTART KEY GENERATION PROCESS AFTER PEER DISCONNECTS.");
            }
            State::Observer { qhb: _, .. } => {
                debug!("Ignoring peer disconnection when `State::Observer`.");
                return;
            },
            State::Validator { qhb: _, .. } => {
                debug!("Ignoring peer disconnection when `State::Validator`.");
                return;
            },
        }
    }

    /// Returns the network state, if possible.
    pub(super) fn network_state(&self, peers: &Peers) -> NetworkState {
        let peer_infos = peers.peers().filter_map(|peer| {
            peer.pub_info().map(|(&uid, &in_addr, &pk)| {
                NetworkNodeInfo { uid, in_addr, pk }
            })
        }).collect::<Vec<_>>();
        match self {
            State::AwaitingMorePeersForKeyGeneration { .. } => {
                NetworkState::AwaitingMorePeers(peer_infos)
            },
            State::GeneratingKeys{ .. } => {
                NetworkState::GeneratingKeys(peer_infos)
            },
            State::Observer { ref qhb } | State::Validator { ref qhb } => {
                // FIXME: Ensure that `peer_info` matches `NetworkInfo` from HB.
                let pk_set = qhb.as_ref().unwrap().dyn_hb().netinfo().public_key_set().clone();
                let pk_map = qhb.as_ref().unwrap().dyn_hb().netinfo().public_key_map().clone();
                NetworkState::Active((peer_infos, pk_set, pk_map))
            },
            _ => NetworkState::Unknown(peer_infos),
        }
    }

    /// Returns a reference to the internal HB instance.
    pub(super) fn qhb(&self) -> Option<&QueueingHoneyBadger<Vec<Transaction>, Uid>> {
        match self {
            State::Observer { ref qhb, .. } => qhb.as_ref(),
            State::Validator { ref qhb, .. } => qhb.as_ref(),
            _ => None,
        }
    }

    /// Returns a reference to the internal HB instance.
    pub(super) fn qhb_mut(&mut self) -> Option<&mut QueueingHoneyBadger<Vec<Transaction>, Uid>> {
        match self {
            State::Observer { ref mut qhb, .. } => qhb.as_mut(),
            State::Validator { ref mut qhb, .. } => qhb.as_mut(),
            _ => None,
        }
    }

    /// Presents input to HoneyBadger or queues it for later.
    ///
    /// Cannot be called while disconnected or connection-pending.
    pub(super) fn input(&mut self, input: Input) -> Option<Result<Step, QhbError>> {
        match self {
            State::Observer { ref mut qhb, .. } | State::Validator { ref mut qhb, .. } => {
                trace!("State::input: Inputting: {:?}", input);
                let step_opt = Some(qhb.as_mut().unwrap().input(input));

                match step_opt {
                    Some(ref step) => match step {
                        Ok(s) => trace!("State::input: QHB output: {:?}", s.output),
                        Err(err) => error!("State::input: QHB output error: {:?}", err),
                    },
                    None => trace!("State::input: QHB Output is `None`"),
                }

                return step_opt;
            },
            State::AwaitingMorePeersForKeyGeneration { ref iom_queue }
                    | State::GeneratingKeys { ref iom_queue, .. }
                    | State::DeterminingNetworkState { ref iom_queue, .. } => {
                trace!("State::input: Queueing input: {:?}", input);
                iom_queue.as_ref().unwrap().push(InputOrMessage::Input(input));
            },
            s @ _ => panic!("State::handle_message: Must be connected in order to input to \
                honey badger. State: {}", s.discriminant()),
        }
        None
    }

    /// Presents a message to HoneyBadger or queues it for later.
    ///
    /// Cannot be called while disconnected or connection-pending.
    pub(super) fn handle_message(&mut self, src_uid: &Uid, msg: Message)
            -> Option<Result<Step, QhbError>> {
        match self {
            State::Observer { ref mut qhb, .. }
                    | State::Validator { ref mut qhb, .. } => {
                trace!("State::handle_message: Handling message: {:?}", msg);
                let step_opt = Some(qhb.as_mut().unwrap().handle_message(src_uid, msg));

                match step_opt {
                    Some(ref step) => match step {
                        Ok(s) => trace!("State::handle_message: QHB output: {:?}", s.output),
                        Err(err) => error!("State::handle_message: QHB output error: {:?}", err),
                    },
                    None => trace!("State::handle_message: QHB Output is `None`"),
                }

                return step_opt;
            },
            State::AwaitingMorePeersForKeyGeneration { ref iom_queue }
                    | State::GeneratingKeys { ref iom_queue, .. }
                    | State::DeterminingNetworkState { ref iom_queue, .. } => {
                trace!("State::handle_message: Queueing message: {:?}", msg);
                iom_queue.as_ref().unwrap().push(InputOrMessage::Message(*src_uid, msg));
            },
            // State::GeneratingKeys { ref iom_queue, .. } => {
            //     iom_queue.as_ref().unwrap().push(InputOrMessage::Message(msg));
            // },
            s @ _ => panic!("State::handle_message: Must be connected in order to input to \
                honey badger. State: {}", s.discriminant()),
        }
        None
    }
}
