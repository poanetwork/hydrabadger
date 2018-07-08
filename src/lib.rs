
extern crate clap;
extern crate env_logger;
#[macro_use] extern crate log;
#[macro_use] extern crate failure;
extern crate crossbeam;
// #[macro_use] extern crate crossbeam_channel;
extern crate crypto;
extern crate chrono;
extern crate num_traits;
extern crate num_bigint;
#[macro_use] extern crate futures;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io;
extern crate rand;
extern crate bytes;
extern crate uuid;
extern crate byteorder;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate bincode;
extern crate tokio_serde_bincode;
extern crate hbbft;

// pub mod network;
pub mod hydrabadger;
pub mod blockchain;

pub use hydrabadger::{Hydrabadger};
pub use blockchain::{Blockchain, MiningError};
