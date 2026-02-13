//! Two pointer serial scan of hex keys.
//!
//! Usage:
//! ```
//! cargo run --example two-pointer-serial -- --db-dir-left data1.rocksdb --db-dir-right data2.rocksdb
//! ```
//!
//! This will scan the two DBs for all keys in each DB.
//! Key and value are random raw bytes encoded as hex strings.
//! It will print the total number of keys in each DB and the number of keys in the intersection.

use anyhow::Result;
use clap::Parser;
use rocksdb_examples::rocksdb_utils::open_rocksdb_for_read_only;
use rocksdb_examples::utils::make_progress_bar;
use rust_rocksdb::IteratorMode;

#[derive(Parser)]
struct Cli {
    #[clap(long)]
    db_dir_left: String,
    #[clap(long)]
    db_dir_right: String,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let db_left = open_rocksdb_for_read_only(&args.db_dir_left, true)?;
    let db_right = open_rocksdb_for_read_only(&args.db_dir_right, true)?;

    let pb = make_progress_bar(None);

    let mut db_iter_left = db_left.full_iterator(IteratorMode::Start);
    let mut db_iter_right = db_right.full_iterator(IteratorMode::Start);

    let mut count_left = 0;
    let mut count_right = 0;
    let mut count_intersection = 0;
    let mut item_left = db_iter_left.next();
    let mut item_right = db_iter_right.next();

    // Don't use take() — we must keep the item we don't advance so it's compared again next iteration.
    while let (Some(Ok((blob_left, _))), Some(Ok((blob_right, _)))) =
        (item_left.as_ref(), item_right.as_ref())
    {
        if blob_left == blob_right {
            count_left += 1;
            count_right += 1;
            count_intersection += 1;
            item_left = db_iter_left.next();
            item_right = db_iter_right.next();
        } else if blob_left < blob_right {
            count_left += 1;
            item_left = db_iter_left.next();
        } else {
            count_right += 1;
            item_right = db_iter_right.next();
        }
        pb.inc(1);
    }

    while item_left.is_some() {
        count_left += 1;
        item_left = db_iter_left.next();
        pb.inc(1);
    }

    while item_right.is_some() {
        count_right += 1;
        item_right = db_iter_right.next();
        pb.inc(1);
    }

    pb.finish_with_message("done");

    let count_left_unique = count_left - count_intersection;
    let count_right_unique = count_right - count_intersection;
    println!(
        "Totals:\nleft: {}\nright: {}\nintersection: {}",
        count_left, count_right, count_intersection
    );
    println!("Unique:\nleft: {count_left_unique}\nright: {count_right_unique}");

    Ok(())
}
