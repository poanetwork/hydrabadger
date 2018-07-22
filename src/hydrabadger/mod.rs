mod state;
mod handler;
mod hydrabadger;

use std;
use bincode;
use hbbft::{
    dynamic_honey_badger::{Error as DhbError},
    queueing_honey_badger::{Error as QhbError},
};
use ::{Message, Input, Uid};
use self::state::{State, StateDsct};
use self::handler::{Handler};

pub use self::hydrabadger::Hydrabadger;

// If `true`, adds a delay to
#[allow(dead_code)]
const EXTRA_DELAY_MS: u64 = 3000;

const BATCH_SIZE: usize = 50;
const TXN_BYTES: usize = 4;
const NEW_TXN_INTERVAL_MS: u64 = 5000;
const NEW_TXNS_PER_INTERVAL: usize = 10;

// The minimum number of peers needed to spawn a HB instance.
const HB_PEER_MINIMUM_COUNT: usize = 4;

// Number of times to attempt wire message re-send.
const WIRE_MESSAGE_RETRY_MAX: usize = 10;


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

