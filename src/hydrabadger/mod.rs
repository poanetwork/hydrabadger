mod handler;
mod hydrabadger;
mod state;

use self::handler::Handler;
use self::state::State;
use bincode;
use hbbft::{
    dynamic_honey_badger::Error as DhbError,
    // queueing_honey_badger::Error as QhbError,
    sync_key_gen::Error as SyncKeyGenError,
};
use std;
use {Input, Message, Uid};

pub use self::hydrabadger::{Config, Hydrabadger, HydrabadgerWeak};
pub use self::state::StateDsct;

// Number of times to attempt wire message re-send.
pub const WIRE_MESSAGE_RETRY_MAX: usize = 10;

/// A HoneyBadger input or message.
#[derive(Clone, Debug)]
pub enum InputOrMessage<T> {
    Input(Input<T>),
    Message(Uid, Message),
}

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Io error: {}", _0)]
    Io(std::io::Error),
    #[fail(display = "Serde error: {}", _0)]
    Serde(bincode::Error),
    #[fail(display = "Error polling hydrabadger internal receiver")]
    HydrabadgerHandlerPoll,
    // FIXME: Make honeybadger error thread safe.
    #[fail(display = "QueuingHoneyBadger propose error")]
    QhbPart,
    /// TEMPORARY UNTIL WE FIX HB ERROR TYPES:
    #[fail(display = "DynamicHoneyBadger error")]
    Dhb(()),
    /// TEMPORARY UNTIL WE FIX HB ERROR TYPES:
    #[fail(display = "QueuingHoneyBadger error [FIXME]")]
    Qhb(()),
    /// TEMPORARY UNTIL WE FIX HB ERROR TYPES:
    #[fail(display = "QueuingHoneyBadger step error")]
    HbStepError,
    #[fail(display = "Error creating SyncKeyGen: {}", _0)]
    SyncKeyGenNew(SyncKeyGenError),
    #[fail(display = "Error generating keys: {}", _0)]
    SyncKeyGenGenerate(SyncKeyGenError),
    #[fail(display = "Unable to push user transactions, this node is not a validator")]
    ProposeUserContributionNotValidator,
    #[fail(display = "Unable to transmit epoch status to listener, listener receiver dropped")]
    InstantiateHbListenerDropped,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<DhbError> for Error {
    fn from(_err: DhbError) -> Error {
        Error::Dhb(())
    }
}
