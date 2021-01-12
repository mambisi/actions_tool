use clap::{Arg, App};
use io::ActionsFileReader;

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

        return;
    }
}
