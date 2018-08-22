mod state;
mod handler;
mod hydrabadger;

use std;
use bincode;
use hbbft::{
    dynamic_honey_badger::{Error as DhbError},
    queueing_honey_badger::{Error as QhbError},
    sync_key_gen::{Error as SyncKeyGenError},
};
use ::{Message, Input, Uid};
use self::state::{State, StateDsct};
use self::handler::{Handler};

pub use self::hydrabadger::{Hydrabadger, Config};

// Number of times to attempt wire message re-send.
pub const WIRE_MESSAGE_RETRY_MAX: usize = 10;


/// A HoneyBadger input or message.
#[derive(Clone, Debug)]
pub(crate) enum InputOrMessage {
    Input(Input),
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
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<QhbError> for Error {
    fn from(_err: QhbError) -> Error {
        Error::Qhb(())
    }
}

impl From<DhbError> for Error {
    fn from(_err: DhbError) -> Error {
        Error::Dhb(())
    }
}

