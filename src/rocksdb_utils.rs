use anyhow::Result;
use rust_rocksdb::{DB, Options};

/// Open a DB for read-only access.
///
/// If `fast_open_for_iteration` is true, the DB will be opened without loading the index and filter blocks into memory.
/// It will make opening faster, but random reads will be slow.
pub fn open_rocksdb_for_read_only(db_dir: &str, fast_open_for_iteration: bool) -> Result<DB> {
    let mut opts = Options::default();
    let mut table_options = rust_rocksdb::BlockBasedOptions::default();
    if fast_open_for_iteration {
        table_options.set_cache_index_and_filter_blocks(true);
        // this may blow up memory usage if the DB is uncompacted and full of L0 files, but good for random reads
        // table_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

        // this is useful for the TwoLevelIndexSearch index type, good for random reads
        // table_options.set_pin_top_level_index_and_filter(true);
    } else {
        // use bloom filter to improve lookup speed
        table_options.set_bloom_filter(10.0, false);
    }

    opts.set_block_based_table_factory(&table_options);
    opts.set_max_file_opening_threads(num_cpus::get() as i32);
    Ok(DB::open_for_read_only(&opts, db_dir, false)?)
}

/// Open a DB for regular writing with sane settings.
pub fn open_rocksdb_for_write(db_dir: &str) -> Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_unordered_write(true);
    opts.set_compression_type(rust_rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rust_rocksdb::DBCompressionType::Zstd);

    // 256MB base file size
    opts.set_target_file_size_base(256 * 1024 * 1024);

    let mut table_options = rust_rocksdb::BlockBasedOptions::default();

    // 8KB block size instead of the default 4KB to strike a good balance between memory usage and lookup speed
    table_options.set_block_size(8 * 1024);

    /*
    // use two-level index search to reduce memory usage by a lot
    table_options.set_index_type(rust_rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
    table_options.set_partition_filters(true);
    // but for within-block, use binary and hash index to improve lookup speed
    table_options.set_data_block_index_type(rust_rocksdb::DataBlockIndexType::BinaryAndHash);
    */

    // use bloom filter to improve lookup speed
    table_options.set_bloom_filter(10.0, false);
    opts.set_block_based_table_factory(&table_options);

    opts.set_max_file_opening_threads(num_cpus::get() as i32);
    Ok(DB::open(&opts, db_dir)?)
}

/// Open a DB for bulk loading and compaction.
///
/// If `num_levels` is provided, it will be used as the number of levels.
/// Otherwise, the default bulk loading setting of 2 will be used.
///
/// If `max_subcompactions` is provided, it will be used as the max number of subcompactions.
/// Otherwise, the default number of subcompactions of num_cpus::get() will be used.
pub fn open_rocksdb_for_bulk_ingestion(
    db_dir: &str,
    num_levels: Option<i32>,
    max_subcompactions: Option<u32>,
) -> Result<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_unordered_write(true);
    opts.set_compression_type(rust_rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rust_rocksdb::DBCompressionType::Zstd);

    // the wonders of bulk loading - https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
    // https://github.com/facebook/rocksdb/blob/v10.10.1/options/options.cc#L486
    opts.prepare_for_bulk_load();

    // need to override prepare_for_bulk_load's values because for existing DBs with non-L0 levels,
    // prepare_for_bulk_load will set num_levels to 1 and db open will fail.
    num_levels.map(|num_levels| opts.set_num_levels(num_levels));

    opts.set_max_write_buffer_number(24);

    let max_flushes = 24;
    opts.set_max_background_jobs(max_flushes);

    // these two are deprecated, in favor of the env settings below - we set them just in case
    #[allow(deprecated)]
    opts.set_max_background_compactions(0);
    #[allow(deprecated)]
    opts.set_max_background_flushes(max_flushes);

    let mut env = rust_rocksdb::Env::new()?;
    env.set_low_priority_background_threads(0);
    env.set_high_priority_background_threads(max_flushes);
    opts.set_env(&env);

    //********************************************************** */
    // final compaction settings
    //********************************************************** */
    // 256MB base file size
    opts.set_target_file_size_base(256 * 1024 * 1024);

    let mut table_options = rust_rocksdb::BlockBasedOptions::default();

    // 8KB block size instead of the default 4KB to strike a good balance between memory usage and lookup speed
    table_options.set_block_size(8 * 1024);

    /*
    // use two-level index search to reduce memory usage by a lot
    table_options.set_index_type(rust_rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
    table_options.set_partition_filters(true);
    // but for within-block, use binary and hash index to improve lookup speed
    table_options.set_data_block_index_type(rust_rocksdb::DataBlockIndexType::BinaryAndHash);
    */

    // use bloom filter to improve lookup speed
    table_options.set_bloom_filter(10.0, false);
    opts.set_block_based_table_factory(&table_options);

    opts.set_disable_auto_compactions(true);
    if let Some(max_subcompactions) = max_subcompactions {
        opts.set_max_subcompactions(max_subcompactions);
    } else {
        opts.set_max_subcompactions(num_cpus::get() as u32);
    }
    // essentially unlimited upper bound
    opts.set_max_compaction_bytes(nbytes::bytes![1; PB]);

    opts.set_max_file_opening_threads(num_cpus::get() as i32);
    Ok(DB::open(&opts, db_dir)?)
}

/// Print RocksDB stats.
pub fn print_rocksdb_stats(db: &DB) -> Result<()> {
    db.property_value("rocksdb.stats")?.map(|stats| {
        println!("stats: {}", stats);
    });

    db.property_value("rocksdb.block-cache-capacity")?
        .map(|stats| {
            println!("block-cache-capacity: {}", stats);
        });

    db.property_value("rocksdb.block-cache-usage")?
        .map(|stats| {
            println!("block-cache-usage: {}", stats);
        });

    db.property_value("rocksdb.block-cache-pinned-usage")?
        .map(|stats| {
            println!("block-cache-pinned-usage: {}", stats);
        });

    db.property_value("rocksdb.estimate-table-readers-mem")?
        .map(|stats| {
            println!("estimate-table-readers-mem: {}", stats);
        });

    Ok(())
}
