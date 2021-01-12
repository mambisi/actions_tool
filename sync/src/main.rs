use common::{ContextActionJson, Block};
use serde_json::Value;
use io::{ActionsFileWriter, ActionsFileReader};
use std::env;

use tracing::{info, error, debug, warn, trace};
use tracing_subscriber;
use std::path::Path;

use clap::App;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");

    tracing_subscriber::fmt::init();

    //let matches = App::new("Sync").author("mambisi")

    let block_limit: u32 = 10;
    let node = "http://master.dev.tezedge.com:18732";
    let file_path = "./actions.bin";
    start_syncing(node, block_limit, file_path).await?;
    Ok(())
}

async fn start_syncing<P: AsRef<Path>>(node: &str, limit: u32, file_path: P) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = ActionsFileWriter::new(file_path).unwrap();
    let current_block_height = writer.header().block_height;
    let mut next_block_id = if current_block_height == 0 { 0 } else { current_block_height + 1 };
    info!("Syncing Blocks");
    loop {
        if next_block_id > (limit + current_block_height) {
            break
        }

        let blocks_url = format!("{}/dev/chains/main/blocks?limit={}&from_block_id={}", node, 1, next_block_id);
        let mut blocks = reqwest::get(&blocks_url)
            .await?
            .json::<Vec<Value>>()
            .await?;

        let block = blocks.first().unwrap().as_object().unwrap();
        let block_hash = block.get("hash").unwrap().as_str();
        let block_header = block.get("header").unwrap().as_object().unwrap();
        let block_level = block_header.get("level").unwrap().as_u64().unwrap();
        let block_hash = block_hash.unwrap();
        let actions_url = format!("{}/dev/chains/main/actions/blocks/{}", node, block_hash);


        let block = Block::new(block_level as u32, block_hash.to_string());


        let mut messages = reqwest::get(&actions_url)
            .await?
            .json::<Vec<ContextActionJson>>()
            .await?;

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