use clap::{Arg, App};
use io::ActionsFileReader;
use std::time::Instant;
use jemalloc_ctl::{stats, epoch};
use io::channel::{ContextAction};

use merkle::prelude::{MerkleStorageStats, MerklePerfStats, MerkleError};

use std::sync::{RwLock, Arc};
use std::convert::TryInto;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;

use jemallocator::Jemalloc;
use std::fs::{File, OpenOptions, read};


fn main() {
    let matches = clap::App::new("Tezedge Action Bin Tool")
        .author("mambisi.zempare@simplestaking.com")
        .subcommand(App::new("print")
            .about("provides print option for actions file")
            .arg(Arg::with_name("head")
                .short("h")
                .long("head")
                .value_name("FILE NAME")
                .help("Prints the action file header")
                .takes_value(true)
                .conflicts_with("block")
            )
            .arg(Arg::with_name("block")
                .short("b")
                .long("block")
                .value_name("FILE NAME")
                .help("Prints block hashes")
                .takes_value(true)
                .conflicts_with("head")
            )
        )
        .subcommand(App::new("benchmark")
            .about("benchmarks read speed")
            .arg(Arg::with_name("file")
                .short("f")
                .long("file")
                .value_name("FILE NAME")
                .help("Action bin file")
                .takes_value(true)
            )
        ).subcommand(App::new("validate")
        .about("validates actions by storing it in tezedge merkle storage [https://github.com/mambisi/merkle-storage-ds]")
        .arg(Arg::with_name("file")
            .short("f")
            .long("file")
            .value_name("FILE NAME")
            .help("Action bin file")
            .takes_value(true)
        )
    )
        .subcommand(App::new("compress")
            .about("Compress bin file with flate2")
            .arg(Arg::with_name("input")
                .short("i")
                .long("input")
                .value_name("FILE NAME")
                .help("input file")
                .takes_value(true)
            ).arg(Arg::with_name("output")
            .short("o")
            .long("output")
            .value_name("FILE NAME")
            .help("output file")
            .takes_value(true)
        )
        )
        .subcommand(App::new("uncompress")
            .about("Compress bin file with flate2")
            .arg(Arg::with_name("input")
                .short("i")
                .long("input")
                .value_name("FILE NAME")
                .help("input file")
                .takes_value(true)
            ).arg(Arg::with_name("output")
            .short("o")
            .long("output")
            .value_name("FILE NAME")
            .help("output file")
            .takes_value(true)
        )
        )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("print") {
        if let Some(file) = matches.value_of("head") {
            let reader = ActionsFileReader::new(file).unwrap();
            println!("{}", reader.header());
        }

        if let Some(file) = matches.value_of("block") {
            let reader = ActionsFileReader::new(file).unwrap();
            reader.for_each(|(block, _)| {
                println!("[{:<10}] {}", block.block_level, block.block_hash_hex)
            })
        }

        return;
    }

    if let Some(matches) = matches.subcommand_matches("benchmark") {
        let file = matches.value_of("file").unwrap();
        let mut ac = 0;
        let mut counter = 0;
        for _ in 0..10 {
            let mut reader = ActionsFileReader::new(file).unwrap();
            for _ in 0..100_u32 {
                let instant = Instant::now();
                if let Some(_) = reader.next() {} else {
                    break;
                }
                ac += instant.elapsed().as_millis();
                counter += 1
            }
        }
        println!("Avg read time: {} ms", ac / counter);
        return;
    }
    if let Some(matches) = matches.subcommand_matches("validate") {
        let file = matches.value_of("file").unwrap();
        let mut reader = ActionsFileReader::new(file).unwrap();
        // Todo make cycle user defined
        let stats = match validate_blocks_merkle_gc_enabled(reader, 4092) {
            Ok(stats) => {
                stats
            }
            Err(e) => {
                panic!(format!("{:?}", e))
            }
        };
        print_stats(stats);
        return;
    }
    if let Some(matches) = matches.subcommand_matches("compress") {
        let input_path = matches.value_of("input").unwrap();
        let output_path = matches.value_of("output").unwrap();

        let mut input_file = OpenOptions::new().read(true).write(false).create(false).open(input_path).expect("Error opening input file");
        let mut output_file = OpenOptions::new().read(true).write(true).create(true).open(output_path).unwrap();

        let mut writer = DeflateEncoder::new(output_file, Compression::best());
        std::io::copy(&mut input_file, &mut writer).expect("Error coping file");
        return;
    }
    if let Some(matches) = matches.subcommand_matches("uncompress") {
        let input_path = matches.value_of("input").unwrap();
        let output_path = matches.value_of("output").unwrap();

        let input_file = OpenOptions::new().read(true).write(false).create(false).open(input_path).expect("Error opening input file");
        let mut output_file = OpenOptions::new().read(true).write(true).create(true).open(output_path).unwrap();

        let mut reader = DeflateDecoder::new(input_file);
        std::io::copy(&mut reader, &mut output_file).expect("Error coping file");
        return;
    }
}


fn validate_blocks_merkle_gc_enabled(reader: ActionsFileReader, cycle: u32) -> Result<MerkleStorageStats, MerkleError> {
    use merkle::prelude::*;
    let db = Arc::new(RwLock::new(DB::new()));

    let mut storage = MerkleStorage::new(db.clone());
    reader.for_each(|(block, actions)| {
        let block_level = block.block_level;
        for msg in &actions {
            if msg.perform {
                match &msg.action {
                    ContextAction::Set { key, value, context_hash, .. } =>
                        {
                            storage.set(key, value);
                        }
                    ContextAction::Copy { to_key: key, from_key, context_hash, .. } =>
                        {
                            storage.copy(from_key, key);
                        }
                    ContextAction::Delete { key, context_hash, .. } =>
                        {
                            storage.delete(key);
                        }

                    ContextAction::RemoveRecursively { key, context_hash, .. } =>
                        {
                            storage.delete(key)
                        }

                    ContextAction::Commit {
                        parent_context_hash, new_context_hash, block_hash: Some(block_hash),
                        author, message, date, ..
                    } => {
                        let date = *date as u64;
                        let hash = storage.commit(date, author.to_owned(), message.to_owned()).unwrap();
                        let commit_hash = hash[..].to_vec();
                        assert_eq!(
                            &commit_hash,
                            new_context_hash,
                            "Invalid context_hash for block: {}, expected: {}, but was: {}",
                            HashType::BlockHash.hash_to_b58check(block_hash),
                            HashType::ContextHash.hash_to_b58check(new_context_hash),
                            HashType::ContextHash.hash_to_b58check(&hash),
                        );
                    }

                    ContextAction::Checkout { context_hash, .. } => {
                        let context_hash_arr: EntryHash = context_hash.as_slice().try_into().unwrap();
                        storage.checkout(&context_hash_arr);
                    }
                    _ => (),
                };
            }
        }
        if block_level != 0 && block_level % cycle == 0 {
            storage.gc();
        }
    });
    storage.get_merkle_stats()
}

fn print_stats(stats: MerkleStorageStats) {
    println!("{:<35}{}", "KEYS:", stats.db_stats.keys);
    println!();
    println!("--------------------------------------------------------------------------------------");
    println!("GLOBAL PREF STATS ");
    println!("--------------------------------------------------------------------------------------");
    for (k, v) in stats.perf_stats.global.iter() {
        println!("{:<35}{}", format!("{} AVG EXEC TIME:", k.to_uppercase()), v.avg_exec_time);
        println!("{:<35}{}", format!("{} OP EXEC TIME MAX:", k.to_uppercase()), v.op_exec_time_max);
        println!("{:<35}{}", format!("{} OP EXEC TIME MIN:", k.to_uppercase()), v.op_exec_time_min);
        println!("{:<35}{}", format!("{} OP EXEC TIMES:", k.to_uppercase()), v.op_exec_times);
    }
    println!();
    println!("--------------------------------------------------------------------------------------");
    println!("PER-PATH PREF STATS ");
    println!("--------------------------------------------------------------------------------------");
    for (k, v) in stats.perf_stats.perpath.iter() {
        println!("{}", k);
        for (k, v) in v {
            println!("          {:<35}{}", format!("{} AVG EXEC TIME:", k.to_uppercase()), v.avg_exec_time);
            println!("          {:<35}{}", format!("{} OP EXEC TIME MAX:", k.to_uppercase()), v.op_exec_time_max);
            println!("          {:<35}{}", format!("{} OP EXEC TIME MIN:", k.to_uppercase()), v.op_exec_time_min);
            println!("          {:<35}{}", format!("{} OP EXEC TIMES:", k.to_uppercase()), v.op_exec_times);
        }
    }
}