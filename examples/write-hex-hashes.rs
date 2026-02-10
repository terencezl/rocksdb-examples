//! Write hex keys and values to RocksDB.
//!
//! Usage:
//! ```
//! cargo run --example write_hex_hashes -- --db-dir data.rocksdb
//! ```
//!
//! This will write NUM_ENTRIES entries to the DB.
//! The DB is expected to be in the format of write_hex_hashes.rs.
//! Keys and values are random raw bytes encoded as hex strings.
//! Parallelized by NUM_THREADS chunks; each thread uses WriteBatch and write without WAL; flush at end. Then compact the DB.

use anyhow::Result;
use clap::Parser;
use rand::Fill;
use rayon::prelude::*;
use rocksdb_examples::rocksdb_utils::{open_rocksdb_for_bulk_ingestion, print_rocksdb_stats};
use rocksdb_examples::utils::{bytes_to_hex, make_progress_bar};
use rust_rocksdb::WriteBatch;

const NUM_THREADS: usize = 8;
const NUM_ENTRIES: usize = NUM_THREADS * 100_000;
const ENTRIES_PER_THREAD: usize = NUM_ENTRIES / NUM_THREADS;
const RAND_BYTES_LEN: usize = 16;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    db_dir: String,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let db = open_rocksdb_for_bulk_ingestion(&args.db_dir, Some(7), None)?;

    let pb = make_progress_bar(Some(NUM_ENTRIES as u64));

    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build_global()?;

    (0..NUM_THREADS).into_par_iter().for_each(|_| {
        let mut rng = rand::rng();
        let mut write_batch = WriteBatch::default();

        for _ in 0..ENTRIES_PER_THREAD {
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
            write_batch.put(key.as_bytes(), val.as_bytes());
            pb.inc(1);
        }

        db.write_without_wal(&write_batch).unwrap();
    });

    pb.finish_with_message("done");

    db.flush()?;

    println!(
        "Wrote {} entries to {} (hex keys and values from random bytes)",
        NUM_ENTRIES, args.db_dir
    );

    println!("========================================");
    println!("========== Before compaction: ==========");
    println!("========================================");
    print_rocksdb_stats(&db)?;

    // Compaction
    let target_level = 6; // default bottommost level
    let mut compaction_opts = rust_rocksdb::CompactOptions::default();
    compaction_opts.set_exclusive_manual_compaction(true);
    compaction_opts.set_change_level(true);
    compaction_opts.set_target_level(target_level);
    compaction_opts
        .set_bottommost_level_compaction(rust_rocksdb::BottommostLevelCompaction::ForceOptimized);
    db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compaction_opts);

    println!("========================================");
    println!("========== After compaction: ==========");
    println!("========================================");
    print_rocksdb_stats(&db)?;

    Ok(())
}
