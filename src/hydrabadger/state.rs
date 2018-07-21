#![allow(dead_code)]

use std::collections::BTreeMap;
use crossbeam::sync::SegQueue;
use hbbft::{
    crypto::{PublicKey, SecretKey},
    sync_key_gen::{SyncKeyGen, Part, PartOutcome, Ack},
    messaging::{DistAlgorithm, NetworkInfo},
    queueing_honey_badger::{Error as QhbError, QueueingHoneyBadger},
    dynamic_honey_badger::{DynamicHoneyBadger},

};
use peer::Peers;
use ::{Uid, NetworkState, NetworkNodeInfo, Message, Transaction, Step, Input};
use super::{InputOrMessage};
use super::{BATCH_SIZE, HB_PEER_MINIMUM_COUNT};


/// A `State` discriminant.
#[derive(Copy, Clone, Debug)]
pub enum StateDsct {
    Disconnected,
    DeterminingNetworkState,
    ConnectedAwaitingMorePeers,
    ConnectedGeneratingKeys,
    ConnectedObserver,
    ConnectedValidator,
}


// The current hydrabadger state.
pub(crate) enum State {
    Disconnected { },
    DeterminingNetworkState { },
    ConnectedAwaitingMorePeers {
        // Queued input to HoneyBadger:
        // FIXME: ACTUALLY USE THIS QUEUED INPUT!
        iom_queue: Option<SegQueue<InputOrMessage>>,
    },
    ConnectedGeneratingKeys {
        sync_key_gen: Option<SyncKeyGen<Uid>>,
        public_key: Option<PublicKey>,
        ack_count: usize,
        // Queued input to HoneyBadger:
        // FIXME: ACTUALLY USE THIS QUEUED INPUT!
        iom_queue: SegQueue<InputOrMessage>,
    },
    ConnectedObserver {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uid>>,
    },
    ConnectedValidator {
        qhb: Option<QueueingHoneyBadger<Vec<Transaction>, Uid>>,
    },
}

impl State {
    /// Returns the state discriminant.
    pub(super) fn discriminant(&self) -> StateDsct {
        match self {
            State::Disconnected { .. } => StateDsct::Disconnected,
            State::DeterminingNetworkState { .. } => StateDsct::DeterminingNetworkState,
            State::ConnectedAwaitingMorePeers { .. } => StateDsct::ConnectedAwaitingMorePeers,
            State::ConnectedGeneratingKeys{ .. } => StateDsct::ConnectedGeneratingKeys,
            State::ConnectedObserver { .. } => StateDsct::ConnectedObserver,
            State::ConnectedValidator { .. } => StateDsct::ConnectedValidator,
        }
    }

    /// Returns a new `State::Disconnected`.
    pub(super) fn disconnected(/*local_uid: Uid, local_addr: InAddr,*/ /*secret_key: SecretKey*/) -> State {
        State::Disconnected { /*secret_key: secret_key*/ }
    }

    // /// Sets the state to `DeterminingNetworkState`.
    // //
    // // TODO: Add proper error handling:
    // fn set_determining_network_state(&mut self) {
    //     *self = match *self {
    //         State::Disconnected { } => {
    //             info!("Setting state: `DeterminingNetworkState`.");
    //             State::DeterminingNetworkState { }
    //         },
    //         _ => panic!("Must be disconnected before calling `::peer_connection_added`."),
    //     };
    // }

    /// Sets the state to `ConnectedAwaitingMorePeers`.
    pub(super) fn set_connected_awaiting_more_peers(&mut self) {
        *self = match *self {
            State::Disconnected { }
                    | State::DeterminingNetworkState { } => {
                info!("Setting state: `ConnectedAwaitingMorePeers`.");
                State::ConnectedAwaitingMorePeers { iom_queue: Some(SegQueue::new()) }
            },
            _ => {
                error!("Attempted to set `State::ConnectedAwaitingMorePeers` while connected.");
                return
            }
        };
    }

    /// Sets the state to `ConnectedAwaitingMorePeers`.
    pub(super) fn set_generating_keys(&mut self, local_uid: &Uid, local_sk: SecretKey, peers: &Peers)
            -> (Part, Ack) {
        let (part, ack);
        *self = match *self {
            State::ConnectedAwaitingMorePeers { ref mut iom_queue } => {
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

                State::ConnectedGeneratingKeys { sync_key_gen: Some(sync_key_gen),
                    public_key: Some(pk), ack_count: 1, iom_queue: iom_queue.take().unwrap() }
            },
            _ => panic!("State::set_generating_keys: \
                Must be State::ConnectedAwaitingMorePeers"),
        };

        (part, ack)
    }


    /// Changes the variant (in-place) of this `State` to `ConnectedObserver`.
    //
    // TODO: Add proper error handling:
    pub(super) fn set_connected_observer(&mut self) {
        *self = match *self {
            State::DeterminingNetworkState { .. } => {
                State::ConnectedObserver { qhb: None }
            },
            _ => panic!("Must be `State::DeterminingNetworkState` before becoming \
                `State::ConnectedObserver`."),
        };
        // unimplemented!()
    }

    /// Changes the variant (in-place) of this `State` to `ConnectedObserver`.
    //
    // TODO: Add proper error handling:
    pub(super) fn set_connected_validator(&mut self, local_uid: Uid, local_sk: SecretKey, peers: &Peers) {
        *self = match *self {
            State::ConnectedGeneratingKeys { ref mut sync_key_gen, mut public_key, .. } => {
                let mut sync_key_gen = sync_key_gen.take().unwrap();
                let _pk = public_key.take().unwrap();

                // generate(&self) -> (PublicKeySet, Option<SecretKeyShare>)
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

                State::ConnectedValidator { qhb: Some(qhb) }
            }
            _ => panic!("State::set_connected_validator: \
                State must be `ConnectedGeneratingKeys`."),
        };
    }

    /// Sets state to `DeterminingNetworkState` if `Disconnected`, otherwise does
    /// nothing.
    pub(super) fn peer_connection_added(&mut self, _peers: &Peers) {
        let _dsct = self.discriminant();
        *self = match *self {
            State::Disconnected { } => {
                info!("Setting state: `DeterminingNetworkState`.");
                State::DeterminingNetworkState { }
            },
            _ => return,
        };
    }

    /// Sets state to `Disconnected` if peer count is zero, otherwise does nothing.
    pub(super) fn peer_connection_dropped(&mut self, peers: &Peers) {
        *self = match *self {
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
            State::ConnectedAwaitingMorePeers { .. } => {
                debug!("Ignoring peer disconnection when `State::ConnectedAwaitingMorePeers`.");
                return;
            },
            State::ConnectedGeneratingKeys { .. } => {
                panic!("FIXME: RESTART KEY GENERATION PROCESS AFTER PEER DISCONNECTS.");
            }
            State::ConnectedObserver { qhb: _, .. } => {
                debug!("Ignoring peer disconnection when `State::ConnectedObserver`.");
                return;
            },
            State::ConnectedValidator { qhb: _, .. } => {
                debug!("Ignoring peer disconnection when `State::ConnectedValidator`.");
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
            State::ConnectedAwaitingMorePeers { .. } => {
                NetworkState::AwaitingMorePeers(peer_infos)
            },
            State::ConnectedGeneratingKeys{ .. } => {
                NetworkState::GeneratingKeys(peer_infos)
            },
            State::ConnectedObserver { .. } | State::ConnectedValidator { .. } => {
                NetworkState::Active(peer_infos)
            },
            _ => NetworkState::Unknown(peer_infos),
        }
    }

    /// Returns a reference to the internal HB instance.
    pub(super) fn qhb(&self) -> Option<&QueueingHoneyBadger<Vec<Transaction>, Uid>> {
        match self {
            State::ConnectedObserver { ref qhb, .. } => qhb.as_ref(),
            State::ConnectedValidator { ref qhb, .. } => qhb.as_ref(),
            _ => None,
        }
    }

    /// Returns a reference to the internal HB instance.
    pub(super) fn qhb_mut(&mut self) -> Option<&mut QueueingHoneyBadger<Vec<Transaction>, Uid>> {
        match self {
            State::ConnectedObserver { ref mut qhb, .. } => qhb.as_mut(),
            State::ConnectedValidator { ref mut qhb, .. } => qhb.as_mut(),
            _ => None,
        }
    }

    /// Presents input to HoneyBadger or queues it for later.
    ///
    /// Cannot be called while disconnected or connection-pending.
    pub(super) fn input(&mut self, input: Input) -> Option<Result<Step, QhbError>> {
        match self {
            State::ConnectedObserver { ref mut qhb, .. }
                    | State::ConnectedValidator { ref mut qhb, .. } => {
                return Some(qhb.as_mut().unwrap().input(input))
            },
            State::ConnectedAwaitingMorePeers { ref iom_queue } => {
                iom_queue.as_ref().unwrap().push(InputOrMessage::Input(input));
            },
            State::ConnectedGeneratingKeys { ref iom_queue, .. } => {
                iom_queue.push(InputOrMessage::Input(input));
            },
            _ => panic!("State::input: Must be connected to input to honey badger."),
        }
        None
    }

    /// Presents a message to HoneyBadger or queues it for later.
    ///
    /// Cannot be called while disconnected or connection-pending.
    pub(super) fn handle_message(&mut self, src_uid: &Uid, msg: Message) -> Option<Result<Step, QhbError>> {
        match self {
            State::ConnectedObserver { ref mut qhb, .. }
                    | State::ConnectedValidator { ref mut qhb, .. } => {
                return Some(qhb.as_mut().unwrap().handle_message(src_uid, msg))
            },
            State::ConnectedAwaitingMorePeers { ref iom_queue } => {
                iom_queue.as_ref().unwrap().push(InputOrMessage::Message(msg));
            },
            State::ConnectedGeneratingKeys { ref iom_queue, .. } => {
                iom_queue.push(InputOrMessage::Message(msg));
            },
            _ => panic!("State::input: Must be connected to input to honey badger."),
        }
        None
    }
}
