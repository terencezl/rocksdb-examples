//! Write and read one entry to RocksDB.
//!
//! Usage:
//! ```
//! cargo run --example write_and_read_one -- --db-dir data.rocksdb
//! ```
//!
//! This will write a random key and value to the DB and then read the value back.
//! The DB is expected to be in the format of write_hex_hashes.rs.
//! Key and value are random raw bytes encoded as hex strings.

use anyhow::Result;
use clap::Parser;
use rand::Fill;
use rocksdb_examples::rocksdb_utils::open_rocksdb_for_write;
use rocksdb_examples::utils::bytes_to_hex;

const RAND_BYTES_LEN: usize = 16;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    db_dir: String,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let db = open_rocksdb_for_write(&args.db_dir)?;

    let mut rng = rand::rng();
    let key = {
        let mut key_bytes = [0u8; RAND_BYTES_LEN];
        Fill::fill_slice(&mut key_bytes, &mut rng);
        bytes_to_hex(&key_bytes)
    };
    let val = {
        let mut val_bytes = [0u8; RAND_BYTES_LEN];
        Fill::fill_slice(&mut val_bytes, &mut rng);
        bytes_to_hex(&val_bytes)
    };
    db.put(key.as_bytes(), val.as_bytes())?;

    println!("key: {}", key);
    let value = db.get(key.as_bytes())?;
    if let Some(value) = value {
        println!("val: {}", std::str::from_utf8(&value)?);
    } else {
        println!("key not found");
    }
    Ok(())
}
