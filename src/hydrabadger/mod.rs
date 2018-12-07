mod handler;
mod hydrabadger;
pub mod key_gen;
mod state;

use self::handler::Handler;
use self::state::{State, StateMachine};
use crate::{Change, Message, Uid};
use bincode;
use hbbft::{dynamic_honey_badger::Error as DhbError, sync_key_gen::Error as SyncKeyGenError};
use std;

pub use self::hydrabadger::{Config, Hydrabadger, HydrabadgerWeak};
pub use self::state::StateDsct;

// Number of times to attempt wire message re-send.
pub const WIRE_MESSAGE_RETRY_MAX: usize = 10;

/// A HoneyBadger input or message.
#[derive(Clone, Debug)]
pub enum InputOrMessage<T> {
    Change(Change),
    Contribution(T),
    Message(Uid, Message),
}

// TODO: Move this up to `lib.rs` or, preferably, create another error type
// for general (lib) use.
#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Io error: {}", _0)]
    Io(std::io::Error),
    #[fail(display = "Serde error: {}", _0)]
    Serde(bincode::Error),
    #[fail(display = "Received a message with invalid signature")]
    InvalidSignature,
    #[fail(display = "Error polling hydrabadger internal receiver")]
    HydrabadgerHandlerPoll,
    #[fail(display = "DynamicHoneyBadger error")]
    Dhb(DhbError),
    #[fail(display = "DynamicHoneyBadger step error")]
    HbStep(DhbError),
    #[fail(display = "Error creating SyncKeyGen: {}", _0)]
    SyncKeyGenNew(SyncKeyGenError),
    #[fail(display = "Error generating keys: {}", _0)]
    SyncKeyGenGenerate(SyncKeyGenError),
    #[fail(display = "Unable to push user transactions, this node is not a validator")]
    ProposeUserContributionNotValidator,
    #[fail(display = "Unable to vote for a change, this node is not a validator")]
    VoteForNotValidator,
    #[fail(display = "Unable to transmit epoch status to listener, listener receiver dropped")]
    InstantiateHbListenerDropped,
    #[fail(display = "Message received from unknown peer while attempting to verify")]
    VerificationMessageReceivedUnknownPeer,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<DhbError> for Error {
    fn from(err: DhbError) -> Error {
        Error::Dhb(err)
    }
}
