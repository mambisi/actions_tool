```shell
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
## Sub Commands
### Print
```shell
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
### Benchmark
```shell
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
