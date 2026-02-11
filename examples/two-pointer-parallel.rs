use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use rocksdb_examples::rocksdb_utils::open_rocksdb_for_read_only;
use rocksdb_examples::utils::{generate_consecutive_hex_strings, make_progress_bar};
use rust_rocksdb::{Direction, IteratorMode};

#[derive(Parser)]
struct Cli {
    #[clap(long)]
    db_dir_left: String,
    #[clap(long)]
    db_dir_right: String,
}

struct Counts {
    count_left: usize,
    count_right: usize,
    count_intersection: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let db_left = open_rocksdb_for_read_only(&args.db_dir_left, true)?;
    let db_right = open_rocksdb_for_read_only(&args.db_dir_right, true)?;

    let prefixes = generate_consecutive_hex_strings(4);
    let pb = make_progress_bar(Some(prefixes.len() as u64));

    let counts = prefixes
        .into_par_iter()
        .map(|prefix_str| {
            let prefix = prefix_str.as_bytes();

            let mut db_iter_left =
                db_left.full_iterator(IteratorMode::From(prefix, Direction::Forward));
            let mut db_iter_right =
                db_right.full_iterator(IteratorMode::From(prefix, Direction::Forward));

            // two pointers
            let mut count_left = 0;
            let mut count_right = 0;
            let mut count_intersection = 0;
            let mut item_left = db_iter_left.next();
            let mut item_right = db_iter_right.next();

            // Don't use take() — keep the item we don't advance for the next comparison.
            while let (Some(Ok((blob_left, _))), Some(Ok((blob_right, _)))) =
                (item_left.as_ref(), item_right.as_ref())
            {
                if &blob_left[..prefix.len()] != prefix || &blob_right[..prefix.len()] != prefix {
                    break;
                }

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
            }

            while let Some(Ok((blob_left, _))) = item_left.as_ref() {
                if &blob_left[..prefix.len()] != prefix {
                    break;
                }
                count_left += 1;
                item_left = db_iter_left.next();
            }

            while let Some(Ok((blob_right, _))) = item_right.as_ref() {
                if &blob_right[..prefix.len()] != prefix {
                    break;
                }
                count_right += 1;
                item_right = db_iter_right.next();
            }

            pb.inc(1);
            Counts {
                count_left,
                count_right,
                count_intersection,
            }
        })
        .reduce(
            || Counts {
                count_left: 0,
                count_right: 0,
                count_intersection: 0,
            },
            |accs, counts| Counts {
                count_left: accs.count_left + counts.count_left,
                count_right: accs.count_right + counts.count_right,
                count_intersection: accs.count_intersection + counts.count_intersection,
            },
        );

    pb.finish_with_message("done");

    let count_left_unique = counts.count_left - counts.count_intersection;
    let count_right_unique = counts.count_right - counts.count_intersection;
    println!(
        "Totals:\nleft: {}\nright: {}\nintersection: {}",
        counts.count_left, counts.count_right, counts.count_intersection
    );
    println!("Unique:\nleft: {count_left_unique}\nright: {count_right_unique}");

    Ok(())
}
