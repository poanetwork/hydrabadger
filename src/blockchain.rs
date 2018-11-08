//! An incredibly simple blockchain implementation.
//!

#![allow(unused_imports, dead_code, unused_variables)]

use chrono::prelude::*;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use num_bigint::BigUint;
use num_traits::One;

const HASH_BYTE_SIZE: usize = 32;
const DIFFICULTY: usize = 4;
const MAX_NONCE: u64 = 1_000_000;

pub type Sha256Hash = [u8; HASH_BYTE_SIZE];

/// Transforms a u64 into a little endian array of u8.
pub fn convert_u64_to_u8_array(val: u64) -> [u8; 8] {
    [
        val as u8,
        (val >> 8) as u8,
        (val >> 16) as u8,
        (val >> 24) as u8,
        (val >> 32) as u8,
        (val >> 40) as u8,
        (val >> 48) as u8,
        (val >> 56) as u8,
    ]
}

/// A mining error
#[derive(Debug, Fail)]
pub enum MiningError {
    #[fail(display = "Could not mine block, hit iteration limit")]
    Iteration,
    #[fail(display = "Block has no parent")]
    NoParent,
}

/// Calculates the hash for the provided block and nonce.
pub fn calculate_hash(block: &Block, nonce: u64) -> Sha256Hash {
    let mut headers = block.headers();
    headers.extend_from_slice(&convert_u64_to_u8_array(nonce));

    let mut hasher = Sha256::new();
    hasher.input(&headers);
    let mut hash = Sha256Hash::default();

    hasher.result(&mut hash);

    hash
}

/// Attemts to find a satisfactory nonce.
fn try_hash(block: &Block) -> Option<(u64, Sha256Hash)> {
    // The target is a number we compare the hash to. It is a 256bit
    // binary with `DIFFICULTY` leading zeroes.
    let target = BigUint::one() << (256 - 4 * DIFFICULTY);

    for nonce in 0..MAX_NONCE {
        let hash = calculate_hash(block, nonce);
        let hash_int = BigUint::from_bytes_be(&hash);

        if hash_int < target {
            return Some((nonce, hash));
        }
    }
    None
}

/// A block header.
#[derive(Debug)]
pub struct Header {
    timestamp: i64,
    prev_block_hash: Sha256Hash,
    nonce: u64,
}

/// A block.
#[derive(Debug)]
pub struct Block {
    header: Header,
    // Body: Instead of transactions, blocks contain bytes:
    data: Vec<u8>,
    // Hash of the block:
    hash: Option<Sha256Hash>,
}

impl Block {
    // Creates a genesis block, which is a block with no parent.
    //
    // The `prev_block_hash` field is set to all zeroes.
    pub fn genesis() -> Result<Self, MiningError> {
        Self::new("Genesis block", Sha256Hash::default())
    }

    /// Creates a new block.
    pub fn new(data: &str, prev_hash: Sha256Hash) -> Result<Self, MiningError> {
        let mut b = Self {
            header: Header {
                timestamp: Utc::now().timestamp(),
                prev_block_hash: prev_hash,
                nonce: 0,
            },
            data: data.to_owned().into(),
            hash: None,
        };

        try_hash(&b)
            .ok_or(MiningError::Iteration)
            .and_then(|(nonce, hash)| {
                b.header.nonce = nonce;
                b.hash = Some(hash);
                Ok(b)
            })
    }

    /// Returns the block headers.
    pub fn headers(&self) -> Vec<u8> {
        let mut vec = Vec::new();

        vec.extend(&convert_u64_to_u8_array(self.header.timestamp as u64));
        vec.extend_from_slice(&self.header.prev_block_hash);

        vec
    }

    /// Returns this block's nonce.
    pub fn nonce(&self) -> u64 {
        self.header.nonce
    }

    /// Returns this block's hash.
    pub fn hash(&self) -> Option<Sha256Hash> {
        self.hash
    }

    /// Returns this block's hash.
    pub fn prev_block_hash(&self) -> Sha256Hash {
        self.header.prev_block_hash
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// A sequence of blocks.
pub struct Blockchain {
    blocks: Vec<Block>,
}

impl Blockchain {
    // Initializes a new blockchain with a genesis block.
    pub fn new() -> Result<Self, MiningError> {
        let blocks = Block::genesis()?;

        Ok(Self {
            blocks: vec![blocks],
        })
    }

    // Adds a newly-mined block to the chain.
    pub fn add_block(&mut self, data: &str) -> Result<(), MiningError> {
        let block: Block;
        {
            match self.blocks.last() {
                Some(prev) => {
                    block = Block::new(data, prev.hash().unwrap())?;
                }
                // Adding a block to an empty blockchain is an error, a genesis block needs to be
                // created first.
                None => return Err(MiningError::NoParent),
            }
        }

        self.blocks.push(block);

        Ok(())
    }

    // A method that iterates over the blockchain's blocks and prints out information for each.
    pub fn traverse(&self) {
        for (i, block) in self.blocks.iter().enumerate() {
            println!("block: {}", i);
            println!("hash: {:?}", block.hash());
            println!("parent: {:?}", block.prev_block_hash());
            println!("data: {:?}", block.data());
            println!()
        }
    }
}
