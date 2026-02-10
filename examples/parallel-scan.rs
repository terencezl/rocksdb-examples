//! Parallel scan of hex keys.
//!
//! Usage:
//! ```
//! cargo run --example parallel_scan -- --db-dir data.rocksdb
//! ```
//!
//! This will scan the DB for all keys that start with the first 4 characters of the hex string.
//! The DB is expected to be in the format of write_hex_hashes.rs.
//! Parallelized by rayon's default thread pool (RAYON_NUM_THREADS); each thread scans the DB for keys that start with the first 4 characters of the hex string.

use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use rocksdb_examples::rocksdb_utils::open_rocksdb_for_read_only;
use rocksdb_examples::utils::{generate_hex_strings, make_progress_bar};
use rust_rocksdb::{Direction, IteratorMode};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    db_dir: String,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let db = open_rocksdb_for_read_only(&args.db_dir, true)?;

    let prefixes = generate_hex_strings(4);
    let pb = make_progress_bar(Some(prefixes.len() as u64));

    let count = prefixes
        .into_par_iter()
        .map(|prefix| {
            let prefix = prefix.as_bytes();
            let mut db_iter = db.full_iterator(IteratorMode::From(prefix, Direction::Forward));
            let mut count = 0;
            while let Some(item) = db_iter.next() {
                let (key, _value) = item.unwrap();
                if &key[..prefix.len()] != prefix {
                    break;
                }
                count += 1;
            }
            pb.inc(1);
            count
        })
        .reduce(|| 0_usize, |acc, c| acc + c);

    pb.finish_with_message("done");
    println!("Count: {}", count);
    Ok(())
}
