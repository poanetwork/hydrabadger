#![allow(unused_imports, dead_code, unused_variables)]

extern crate clap;
extern crate env_logger;
extern crate hydrabadger;
extern crate chrono;

use std::net::{SocketAddr, ToSocketAddrs};
use std::collections::HashSet;
use std::env;
use std::io::Write;
use chrono::Local;
use clap::{App, Arg, ArgMatches};
use hydrabadger::{Hydrabadger, Blockchain, MiningError};


/// Returns parsed command line arguments.
fn arg_matches<'a>() -> ArgMatches<'a> {
    App::new("hydrabadger")
        .version("0.1")
        .author("Nick Sanders <cogciprocate@gmail.com>")
        .about("Evaluation and testing for hbbft")
        .arg(Arg::with_name("bind-address")
            .short("b")
            .long("bind-address")
            .value_name("<HOST:PORT>")
            .help("Specifies the local address to listen on.")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("remote-address")
            .short("r")
            .long("remote-address")
            .help("Specifies a list of remote node addresses to connect to.")
            .value_name("<HOST:PORT>")
            .takes_value(true)
            .multiple(true)
            .number_of_values(1))
        // .arg(Arg::with_name("broadcast-value")
        //     .long("broadcast-value")
        //     .value_name("BROADCAST_VALUE")
        //     .help("Specifies a value to propose to other nodes.")
        //     .takes_value(true))
        .get_matches()
}


/// Begins mining.
fn mine() -> Result<(), MiningError> {
    let mut chain = Blockchain::new()?;
    println!("Send 1 Hydradollar to Bob");
    chain.add_block("1HD->Bob")?;
    chain.add_block("0.5HD->Bob")?;
    chain.add_block("1.5HD->Bob")?;

    println!("Traversing blockchain:\n");
    chain.traverse();

    Ok(())
}


fn main() {
    env_logger::Builder::new()
        .format(|buf, record| {
            write!(buf,
                "{} [{}] - {}\n",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .parse(&env::var("HYDRABADGER_LOG").unwrap_or_default())
        .init();

    let matches = arg_matches();
    let bind_address: SocketAddr = matches.value_of("bind-address")
        .expect("No bind address provided")
        .to_socket_addrs()
        .expect("Invalid bind address")
        .next().unwrap();

    let remote_addresses: HashSet<SocketAddr> = match matches.values_of("remote-address") {
        Some(addrs) => addrs.flat_map(|addr| addr.to_socket_addrs()
            .expect("Invalid remote bind address"))
            .collect(),
        None => HashSet::new(),
    };

    // let broadcast_value = matches.value_of("broadcast-value")
    //     .map(|bv| bv.as_bytes().to_vec());

    let hb = Hydrabadger::new(bind_address, None);
    hydrabadger::run_node(hb, remote_addresses);

    // match mine() {
    //     Ok(_) => {},
    //     Err(err) => println!("Error: {}", err),
    // }
}