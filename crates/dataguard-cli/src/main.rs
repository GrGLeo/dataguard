mod constructor;
mod errors;
mod parser;
use crate::constructor::construct_validator;
use anyhow::{Context, Result};
use clap::Parser;
use parser::Config;

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

fn run() -> Result<()> {
    let args = Args::parse();

    let config_path = std::path::PathBuf::from(args.config);
    let config_str = std::fs::read_to_string(config_path.clone())
        .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;
    let config: Config = toml::from_str(config_str.as_str())
        .with_context(|| format!("Failed to parse config file: {}", config_path.display()))?;
    if config.table.is_empty() {
        anyhow::bail!("Configuration file contains no table");
    }
    for t in config.table {
        println!("Validation on: {}", t.name);
        construct_validator(&t)
            .with_context(|| format!("Failed to validate table: '{}'", t.name))?;
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {:?}", err);
        std::process::exit(1);
    }
}
