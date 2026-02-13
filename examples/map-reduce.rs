//! Map-reduce of hex keys.
//!
//! Usage:
//! ```
//! cargo run --example map-reduce -- --step map --db-dir data.rocksdb --output-db-dir data-mapped.rocksdb
//! cargo run --example map-reduce -- --step reduce --db-dir data-mapped.rocksdb --output-db-dir data-reduced.rocksdb
//! ```
//!
//! Map step: (key, value) -> (value.hex(key), key).
//! Reduce step: group by value (strip the .hex(key) suffix) and join grouped keys with '|'.

use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use rocksdb_examples::rocksdb_utils::{
    open_rocksdb_for_bulk_ingestion, open_rocksdb_for_read_only,
};
use rocksdb_examples::utils::{generate_consecutive_hex_strings, make_progress_bar};
use rust_rocksdb::{Direction, IteratorMode};

const ROCKSDB_NUM_LEVELS: i32 = 7;

#[derive(Parser)]
struct Cli {
    /// Step to run (map, reduce)
    step: String,
    #[clap(long)]
    db_dir: String,
    #[clap(long)]
    output_db_dir: String,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let db = open_rocksdb_for_read_only(&args.db_dir, true)?;
    let output_db =
        open_rocksdb_for_bulk_ingestion(&args.output_db_dir, Some(ROCKSDB_NUM_LEVELS), None)?;

    match args.step.as_str() {
        "map" => {
            let prefixes = generate_consecutive_hex_strings(3);
            let pb = make_progress_bar(Some(prefixes.len() as u64));

            let count = prefixes
                .into_par_iter()
                .map(|prefix| {
                    let prefix = prefix.as_bytes();
                    let mut db_iter =
                        db.full_iterator(IteratorMode::From(prefix, Direction::Forward));
                    let mut count = 0;
                    let mut write_batch = rust_rocksdb::WriteBatch::default();
                    while let Some(item) = db_iter.next() {
                        let (key, value) = item.unwrap();
                        if &key[..prefix.len()] != prefix {
                            break;
                        }

                        let value_str = String::from_utf8_lossy(value.as_ref());
                        let key_hex = hex::encode(key.as_ref());
                        let new_key = format!("{}.{}", value_str, key_hex);
                        let new_value = key;

                        write_batch.put(new_key.as_bytes(), &new_value);
                        count += 1;
                    }
                    output_db.write_without_wal(&write_batch).unwrap();
                    pb.inc(1);
                    count
                })
                .reduce(|| 0_usize, |acc, c| acc + c);

            output_db.flush()?;

            pb.finish_with_message("done");
            println!("Count: {}", count);
        }
        "reduce" => {
            let prefixes = generate_consecutive_hex_strings(3);
            let pb = make_progress_bar(Some(prefixes.len() as u64));

            let counts = prefixes
                .into_par_iter()
                .map(|prefix| {
                    let prefix = prefix.as_bytes();
                    let mut db_iter =
                        db.full_iterator(IteratorMode::From(prefix, Direction::Forward));
                    let mut write_batch = rust_rocksdb::WriteBatch::default();
                    let mut count = 0;
                    let mut count_grouped = 0;
                    let mut prev_key = Vec::<u8>::new();
                    let mut blobs_vec: Vec<Vec<u8>> = vec![];
                    while let Some(item) = db_iter.next() {
                        let (key, value) = item.unwrap();
                        if &key[..prefix.len()] != prefix {
                            break;
                        }

                        // key is "value_str.key_hex"; group by value_str = everything before last '.'
                        let dot = key.iter().rposition(|&b| b == b'.').unwrap_or_else(|| {
                            panic!("Invalid key: {}", String::from_utf8_lossy(&key))
                        });
                        let new_key = key[..dot].to_vec();

                        if new_key != prev_key {
                            if !prev_key.is_empty() {
                                // concatenate with '|'
                                // can use protobuf or anything else to serialize
                                let new_value: Vec<u8> = blobs_vec.join(&b"|"[..]);
                                write_batch.put(prev_key, new_value);
                                count_grouped += 1;
                            }
                            blobs_vec = vec![];
                            prev_key = new_key;
                        }

                        blobs_vec.push(value.to_vec());
                        count += 1;
                    }

                    if !blobs_vec.is_empty() {
                        let new_value: Vec<u8> = blobs_vec.join(&b"|"[..]);
                        write_batch.put(prev_key, new_value);
                        count_grouped += 1;
                    }
                    output_db.write_without_wal(&write_batch).unwrap();
                    pb.inc(1);
                    (count, count_grouped)
                })
                .reduce(
                    || (0_usize, 0_usize),
                    |accs, counts| (accs.0 + counts.0, accs.1 + counts.1),
                );

            output_db.flush()?;

            pb.finish_with_message("done");
            println!("Count: {} count_grouped: {}", counts.0, counts.1);
        }
        _ => {
            panic!("Invalid step");
        }
    }

    // Compaction
    println!("========== Compacting ==========");
    let mut compaction_opts = rust_rocksdb::CompactOptions::default();
    compaction_opts.set_exclusive_manual_compaction(true);
    compaction_opts.set_change_level(true);
    compaction_opts.set_target_level(ROCKSDB_NUM_LEVELS - 1);
    compaction_opts
        .set_bottommost_level_compaction(rust_rocksdb::BottommostLevelCompaction::ForceOptimized);
    output_db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compaction_opts);

    Ok(())
}
