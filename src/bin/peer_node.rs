#![allow(unused_imports, dead_code, unused_variables)]

extern crate chrono;
extern crate clap;
extern crate env_logger;
extern crate hydrabadger;
extern crate rand;
#[macro_use]
extern crate serde_derive;

use chrono::Local;
use clap::{App, Arg, ArgMatches};
use hydrabadger::{Blockchain, Config, Hydrabadger, MiningError, Uid};
use rand::{distributions::Standard, Rng};
use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};

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
        .arg(Arg::with_name("batch-size")
            .long("batch-size")
            .value_name("BATCH_SIZE")
            .help("Specifies the number of transactions per batch.")
            .takes_value(true))
        .arg(Arg::with_name("txn-gen-count")
            .long("txn-gen-count")
            .value_name("TXN_GEN_COUNT")
            .help("Specifies the number of random transactions to generate each interval.")
            .takes_value(true))
        .arg(Arg::with_name("txn-gen-interval")
            .long("txn-gen-interval")
            .value_name("TXN_GEN_INTERVAL")
            .help("Specifies amount of time in milliseconds between each round of random \
                transaction input generation.")
            .takes_value(true))
        .arg(Arg::with_name("txn-bytes")
            .long("txn-bytes")
            .value_name("TXN_BYTES")
            .help("Specifies the size of each randomly generated transaction in bytes.")
            .takes_value(true))
        .arg(Arg::with_name("keygen-node-count")
            .long("keygen-node-count")
            .value_name("KEYGEN_NODE_COUNT")
            .help("Specifies the minimum number of nodes that must be connected, including the \
                local node, before a consensus network will initialize. All nodes participating in \
                network bootstrapping must have the same value. This setting has no effect when \
                when connecting to pre-existing networks which have already undergone the \
                bootstrap process.")
            .takes_value(true))
        .arg(Arg::with_name("output-extra-delay")
            .long("output-extra-delay")
            .value_name("EXTRA_DELAY")
            .help("Specifies the amount of time to wait after outputting a batch in \
                milliseconds. This can make reading or parsing logs more managable.")
            .takes_value(true))
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

/// A transaction.
#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd, Debug, Clone)]
pub struct Transaction(pub Vec<u8>);

impl Transaction {
    fn random(len: usize) -> Transaction {
        Transaction(
            rand::thread_rng()
                .sample_iter(&Standard)
                .take(len)
                .collect(),
        )
    }
}

fn main() {
    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}]: {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .parse(&env::var("HYDRABADGER_LOG").unwrap_or_default())
        .try_init()
        .ok();

    let matches = arg_matches();
    let bind_address: SocketAddr = matches
        .value_of("bind-address")
        // TODO: Consider providing a default (and add to help info above).
        .expect("No bind address provided")
        // .unwrap_or("localhost::3070")
        .to_socket_addrs()
        .expect("Invalid bind address")
        .next()
        .unwrap();

    let remote_addresses: HashSet<SocketAddr> = match matches.values_of("remote-address") {
        Some(addrs) => addrs
            .flat_map(|addr| addr.to_socket_addrs().expect("Invalid remote address"))
            .collect(),
        None => HashSet::new(),
    };

    let mut cfg = Config::default();

    if let Some(tgc) = matches.value_of("txn-gen-count") {
        cfg.txn_gen_count = tgc.parse().expect("Invalid transaction generation count.");
    }

    if let Some(tgi) = matches.value_of("txn-gen-interval") {
        cfg.txn_gen_interval = tgi
            .parse()
            .expect("Invalid transaction generation interval.");
    }

    if let Some(tgb) = matches.value_of("txn-bytes") {
        cfg.txn_gen_bytes = tgb.parse().expect("Invalid transaction size (bytes).");
    }

    if let Some(knc) = matches.value_of("keygen-node-count") {
        cfg.keygen_peer_count = knc
            .parse::<usize>()
            .expect("Invalid minimum keygen node count.")
            - 1;
    }

    if let Some(oed) = matches.value_of("output-extra-delay") {
        cfg.output_extra_delay_ms = oed.parse().expect("Invalid output extra delay.");
    }

    let hb = Hydrabadger::new(bind_address, cfg, Uid::new());

    let gen_txn = |txn_gen_count, txn_gen_bytes| {
        (0..txn_gen_count)
            .map(|_| Transaction::random(txn_gen_bytes))
            .collect::<Vec<_>>()
    };

    hb.run_node(Some(remote_addresses), Some(gen_txn));

    // match mine() {
    //     Ok(_) => {},
    //     Err(err) => println!("Error: {}", err),
    // }
}
