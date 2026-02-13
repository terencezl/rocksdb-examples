//! Write and read one entry to RocksDB.
//!
//! Usage:
//! ```
//! cargo run --example write_and_read_one -- --db-dir data.rocksdb
//! ```
//!
//! This will write a random key and value to the DB and then read the value back.
//! Key and value are random raw bytes encoded as hex strings.

use anyhow::Result;
use clap::Parser;
use rocksdb_examples::rocksdb_utils::open_rocksdb_for_write;
use rocksdb_examples::utils::generate_random_hex_string;

const KEY_LEN: usize = 16;
const VAL_LEN: usize = 3;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    db_dir: String,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let db = open_rocksdb_for_write(&args.db_dir)?;

    let key = generate_random_hex_string(KEY_LEN);
    let val = generate_random_hex_string(VAL_LEN);
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
