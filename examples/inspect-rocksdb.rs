//! Inspect RocksDB.
//!
//! Usage:
//! ```
//! cargo run --example inspect-rocksdb -- --db-dir data.rocksdb --one-by-one
//! cargo run --example inspect-rocksdb -- --db-dir data.rocksdb --print-stats
//! cargo run --example inspect-rocksdb -- --db-dir data.rocksdb --count
//! cargo run --example inspect-rocksdb -- --db-dir data.rocksdb --key 00000a2865d3d6f2792de5adf5cc9193
//! ```
//!
//! This will inspect the DB.
//! The DB is expected to be in the format of write_hex_hashes.rs.
//! Key and value are random raw bytes encoded as hex strings.
//! You can inspect the DB by key, one by one, printing stats, or counting the number of keys that start with a given prefix.
use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use rust_rocksdb::{Direction, IteratorMode};
use rocksdb_examples::rocksdb_utils::{open_rocksdb_for_read_only, print_rocksdb_stats};
use rocksdb_examples::utils::{generate_hex_strings, handle_input, make_progress_bar};

#[derive(Parser)]
struct Cli {
    #[clap(long)]
    db_dir: String,
    #[clap(long)]
    key: Option<String>,
    #[clap(long)]
    one_by_one: bool,
    #[clap(long)]
    print_stats: bool,
    #[clap(long)]
    count: bool,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let db = open_rocksdb_for_read_only(&args.db_dir, true)?;

    if let Some(key) = args.key {
        let key = key.as_bytes();
        let value = db.get(key)?.ok_or(anyhow::anyhow!("key not found"))?;
        println!(
            "key: {} value: {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(&value)
        );
    } else if args.one_by_one {
        // iterator from start
        let mut db_iter = db.full_iterator(IteratorMode::Start);
        while let Some(item) = db_iter.next() {
            let (key, value) = item.unwrap();
            println!(
                "key: {} value: {}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&value)
            );
            handle_input();
        }
    } else if args.print_stats {
        print_rocksdb_stats(&db)?;
    } else if args.count {
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
    } else {
        println!("Invalid command");
        std::process::exit(1);
    }

    Ok(())
}
