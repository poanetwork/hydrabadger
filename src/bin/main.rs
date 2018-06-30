
extern crate clap;
extern crate pretty_env_logger;
extern crate crossbeam;
extern crate hydrabadger;

use std::net::{SocketAddr, ToSocketAddrs};
use std::collections::HashSet;
use clap::{App, Arg, ArgMatches};
use hydrabadger::Node;


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
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("broadcast-value")
            .long("broadcast-value")
            .value_name("BROADCAST_VALUE")
            .takes_value(true))
        .arg(Arg::with_name("remote-address")
            .short("r")
            .long("remote-address")
            .value_name("<HOST:PORT>")
            .takes_value(true)
            .multiple(true)
            .number_of_values(1))
        .arg(Arg::with_name("help")
            .short("h")
            .long("--help"))
        .arg(Arg::with_name("version")
            .short("v")
            .long("version"))
        .get_matches()
}


fn main() {
    pretty_env_logger::init();

    let matches = arg_matches();
    let bind_address: SocketAddr = matches.value_of("bind-address").unwrap()
        .to_socket_addrs().expect("Invalid bind address").next().unwrap();

    let remote_addresses: HashSet<SocketAddr> = match matches.values_of("remote-address") {
        Some(addrs) => addrs.flat_map(|addr| addr.to_socket_addrs().expect("Invalid bind address"))
            .collect(),
        None => HashSet::new(),
    };

    let broadcast_value = matches.value_of("broadcast-value")
        .map(|bv| bv.as_bytes().to_vec());

    let node = Node::new(bind_address, remote_addresses, broadcast_value);
    node.run().expect("Node failed");

}