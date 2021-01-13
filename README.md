# Tezedge Action Tool

Tool for rapid testing of tezos action on merkle storage

#### Modules
- [io](#io)
- [sync](#sync)
- [bintool](#bintool)
## IO
`ActionsFileReader` reads blocks and actions from file

`ActionsFileWriter` writes blocks and actions to a file
### Example
`ActionsFileWriter`
```rust
let mut writer = ActionsFileWriter::new("./actions.bin").unwrap();
let blocks_url = "http://127.0.0.1:18732/dev/chains/main/blocks?limit=1&from_block_id=1"
let mut blocks = reqwest::get(&blocks_url)
    .await?
    .json::<Vec<Value>>()
    .await?;

let block = blocks.first().unwrap().as_object().unwrap();
let block_hash = block.get("hash").unwrap().as_str();
let block_header = block.get("header").unwrap().as_object().unwrap();
let block_level = block_header.get("level").unwrap().as_u64().unwrap();
let block_hash = block_hash.unwrap();
let actions_url = format!("http://127.0.0.1:18732/dev/chains/main/actions/blocks/{}", block_hash);


let block = Block::new(block_level as u32, block_hash.to_string());


let mut messages = reqwest::get(&actions_url)
    .await?
    .json::<Vec<ContextActionJson>>()
    .await?;

let actions: Vec<_> = messages.iter().map(|action_json| {
    action_json.clone().action
}).collect();

writer.update(block, actions).unwrap();
```
`ActionsFileReader`
```rust
let reader = ActionsFileReader::new("./actions.bin").unwrap();
println!("{}", reader.header());
```
`ActionsFileReader` implements the `Iterator` trait
````rust
let reader = ActionsFileReader::new("./actions.bin").unwrap();
reader.for_each(|(block,actions)|{
    //Do something
})
````


## Sync
Syncs action from rpc endpoint and stores on disk
```
Tezedge Action Sync Tool 
mambisi.zempare@simplestaking.com

USAGE:
    sync [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -f, --file <FILE>      output file path [default: ./actions.bin]
    -l, --limit <LIMIT>    Set the number of block to sync from the current block [default: 500000]
    -n, --node <NODE>      Node base url [default: http://127.0.0.1:18732]

```

## Bintool
This tool let you perform operations on the actions bin file, it can be used to print,
validate and benchmark the action bin file.
```
Tezedge Action Bin Tool 
mambisi.zempare@simplestaking.com

USAGE:
    bintool [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    benchmark    benchmarks read speed
    help         Prints this message or the help of the given subcommand(s)
    print        provides print option for actions file
    validate     validates actions by storing it in tezedge merkle storage

```
### Sub Commands
#### Print
```
bintool-print 
provides print option for actions file

USAGE:
    bintool print [OPTIONS]

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --block <FILE NAME>    Prints block hashes
    -h, --head <FILE NAME>     Prints the action file header

```
#### Benchmark
```
bintool-benchmark 
benchmarks read speed

USAGE:
    bintool benchmark [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -f, --file <FILE NAME>    Action bin file

```
#### Validate
```
bintool-validate 
validates actions by storing it in tezedge merkle storage [https://github.com/mambisi/merkle-storage-ds]

USAGE:
    bintool validate [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -f, --file <FILE NAME>    Action bin file


```