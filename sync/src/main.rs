use io::channel::{ContextActionJson};
use serde_json::Value;
use io::{ActionsFileWriter, ActionsFileReader, Block};
use std::env;

use tracing::{info, error, debug, warn, trace};
use tracing_subscriber;
use std::path::Path;

use clap::{App, Arg};
use std::fs::OpenOptions;

use cluFlock::ExclusiveFlock;
use cluFlock::ToFlock;


fn main() -> anyhow::Result<()> {
    env::set_var("RUST_LOG", "info");

    tracing_subscriber::fmt::init();
    let matches = clap::App::new("Tezedge Action Sync Tool")
        .author("mambisi.zempare@simplestaking.com")
        .arg(Arg::with_name("node")
            .short("n")
            .long("node")
            .value_name("NODE")
            .takes_value(true)
            .default_value("http://127.0.0.1:18732")
            .help("Node base url")
        )
        .arg(Arg::with_name("limit")
            .short("l")
            .long("limit")
            .value_name("LIMIT")
            .takes_value(true)
            .default_value("500000")
            .help("Set the number of block to sync from the current block")
        )
        .arg(Arg::with_name("file")
            .short("f")
            .long("file")
            .value_name("FILE")
            .takes_value(true)
            .default_value("./actions.bin")
            .help("output file path")
        )
        .get_matches();


    let node = matches.value_of("node").unwrap();
    let block_limit = matches.value_of("limit").unwrap().parse::<u32>().unwrap_or(25000);
    let file_path = matches.value_of("file").unwrap();

    start_syncing(node, block_limit, file_path)
}

fn start_syncing<P: AsRef<Path>>(node: &str, limit: u32, file_path: P) -> anyhow::Result<()> {

    let mut writer = ActionsFileWriter::new(file_path).unwrap();
    let current_block_height = writer.header().block_height;
    let mut next_block_id = if current_block_height == 0 { 0 } else { current_block_height + 1 };
    info!("Syncing Blocks");
    loop {
        if next_block_id > (limit + current_block_height) {
            break;
        }

        let blocks_url = format!("{}/dev/chains/main/blocks?limit={}&from_block_id={}", node, 1, next_block_id);
        let mut blocks : Vec<Value> = ureq::get(&blocks_url).call()?.into_json()?;

        let block = blocks.first().unwrap().as_object().unwrap();
        let block_hash = block.get("hash").unwrap().as_str();

        let block_header = block.get("header").unwrap().as_object().unwrap();
        let block_level = block_header.get("level").unwrap().as_u64().unwrap();
        let block_hash = block_hash.unwrap();
        let predecessor_hash = block_header.get("predecessor").unwrap().as_str().unwrap();
        let actions_url = format!("{}/dev/chains/main/actions/blocks/{}", node, block_hash);

        let block_hash_raw = hex::decode(block_hash)?;
        let predecessor_hash_raw = hex::decode(predecessor_hash)?;
        let block = Block::new(block_level as u32, block_hash_raw, predecessor_hash_raw );


        let mut messages : Vec<ContextActionJson>= ureq::get(&actions_url).call()?.into_json()?;

        let actions: Vec<_> = messages.iter().map(|action_json| {
            action_json.clone().action
        }).collect();

        match writer.update(block, actions) {
            Ok(i) => {
                next_block_id = i;
                info!("Synced Block Level: {} Hash: {}", block_level, block_hash);
            }
            Err(r) => {
                warn!("{}", r);
                next_block_id += 1;
            }
        };
    }

    Ok(())
}