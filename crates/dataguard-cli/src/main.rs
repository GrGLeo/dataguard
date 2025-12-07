mod constructor;
mod errors;
mod parser;
use crate::constructor::construct_validator;
use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use parser::Config;

/// Output format for validation results
#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    /// Print results to standard output (human-readable)
    Stdout,
    /// Output results in JSON format
    Json,
}

#[derive(Parser, Debug)]
#[command(
    name = "dataguard",
    version,
    author = "DataGuard Contributors",
    about = "DataGuard CLI - Data validation tool for CSV/table files",
    long_about = "DataGuard is a high-performance data validation tool that validates data tables \
                  based on configurable rules. It supports various data types and validation rules \
                  for numeric, string, and generic columns.\n\n\
                  Example usage:\n  \
                  dataguard --config validation.toml --output stdout"
)]
struct Args {
    /// Path to the TOML configuration file that defines validation rules
    #[arg(short, long, value_name = "FILE")]
    config: String,

    /// Output format for validation results
    #[arg(short, long, value_enum, default_value = "stdout")]
    output: OutputFormat,
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
    
    // Process validation based on output format
    match args.output {
        OutputFormat::Stdout => {
            for t in config.table {
                println!("Validation on: {}", t.name);
                construct_validator(&t)
                    .with_context(|| format!("Failed to validate table: '{}'", t.name))?;
            }
        }
        OutputFormat::Json => {
            // JSON output format - placeholder for future implementation
            for t in config.table {
                println!("Validation on: {}", t.name);
                construct_validator(&t)
                    .with_context(|| format!("Failed to validate table: '{}'", t.name))?;
            }
        }
    }
    
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {:?}", err);
        std::process::exit(1);
    }
}
