use clap::{Arg, App};
use io::ActionsFileReader;
use std::time::Instant;

fn main() {
    let matches = clap::App::new("Tezedge Action Bin Tool")
        .author("mambisi.zempare@simplestaking.com")
        .arg(Arg::with_name("head")
            .short("h")
            .long("head")
            .value_name("HEAD")
            .takes_value(true)
            .help("Prints the header of actions file")
        )
        .subcommand(App::new("benchmark")
            .about("benchmarks read speed")
            .arg(Arg::with_name("file")
                .short("f")
                .long("file")
                .value_name("FILE")
                .help("Action bin file")
                .takes_value(true)
            )
        )
        .get_matches();

    if let Some(file) = matches.value_of("head") {
        let reader = ActionsFileReader::new(file).unwrap();
        println!("{}", reader.header());
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
        println!("Avg read: {} ms", ac / counter);
        return;
    }
}
