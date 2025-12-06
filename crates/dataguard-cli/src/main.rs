mod constructor;
mod parser;
use parser::Config;

use clap::Parser;

use crate::constructor::construct_validator;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long)]
    config: String,

    /// Number of times to greet
    #[arg(short, long)]
    output: String,
}

fn main() {
    let args = Args::parse();

    let config_path = args.config;
    let config_str = std::fs::read_to_string(config_path.clone())
        .expect(format!("Failed to read from path: {}", config_path).as_str());
    let config: Config = toml::from_str(config_str.as_str()).unwrap();
    println!("{:?}", config);

    for t in config.table {
        construct_validator(&t);
    }
}
