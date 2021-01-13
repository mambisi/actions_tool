use clap::{Arg, App};
use io::ActionsFileReader;
use std::time::Instant;
use jemalloc_ctl::{stats, epoch};
use io::channel::{Block, ContextAction};

use merkle::prelude::{MerkleStorageStats,MerklePerfStats,MerkleError};
use std::sync::{RwLock, Arc};
use std::convert::TryInto;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;


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
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("print") {
        if let Some(file) = matches.value_of("head") {
            let reader = ActionsFileReader::new(file).unwrap();
            println!("{}", reader.header());
        }

        if let Some(file) = matches.value_of("block") {
            let reader = ActionsFileReader::new(file).unwrap();
            reader.for_each(|(block,_)|{
                println!("[{:<10}] {}", block.block_level, block.block_hash)
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
        let stats = match validate_blocks_merkle_gc_enabled(reader,4092) {
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
}


fn validate_blocks_merkle_gc_enabled(reader : ActionsFileReader, cycle : u32) -> Result<MerkleStorageStats,MerkleError> {
    use merkle::prelude::*;
    let db = Arc::new(RwLock::new(DB::new()));

    let mut storage = MerkleStorage::new(db.clone());
    reader.for_each(|(block,actions)|{
        let block_level = block.block_level;
        for action in &actions {
            match action {
                ContextAction::Set { key, value, context_hash, ignored, .. } =>
                    if !ignored {
                        storage.set(key, value);
                    }
                ContextAction::Copy { to_key: key, from_key, context_hash, ignored, .. } =>
                    if !ignored {
                        storage.copy(from_key, key);
                    }
                ContextAction::Delete { key, context_hash, ignored, .. } =>
                    if !ignored {
                        storage.delete(key);
                    }
                ContextAction::RemoveRecursively { key, context_hash, ignored, .. } =>
                    if !ignored {
                        storage.delete(key);
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
        for (k,v) in v {
            println!("          {:<35}{}", format!("{} AVG EXEC TIME:", k.to_uppercase()), v.avg_exec_time);
            println!("          {:<35}{}", format!("{} OP EXEC TIME MAX:", k.to_uppercase()), v.op_exec_time_max);
            println!("          {:<35}{}", format!("{} OP EXEC TIME MIN:", k.to_uppercase()), v.op_exec_time_min);
            println!("          {:<35}{}", format!("{} OP EXEC TIMES:", k.to_uppercase()), v.op_exec_times);
        }
    }

}