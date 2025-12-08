mod constructor;
mod errors;
mod parser;
use crate::constructor::construct_csv_table;
use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use dataguard_core::{RuleError, Validator};
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

    /// Enable debug mode with detailed error backtraces and stack traces
    #[arg(short, long)]
    debug: bool,
}

fn run(args: Args) -> Result<()> {
    let config_path = std::path::PathBuf::from(args.config);
    let config_str = std::fs::read_to_string(config_path.clone())
        .with_context(|| format!("Failed to read config file: {}", config_path.display()))?;
    let config: Config = toml::from_str(config_str.as_str())
        .with_context(|| format!("Failed to parse config file: {}", config_path.display()))?;
    if config.table.is_empty() {
        anyhow::bail!("Configuration file contains no table");
    }

    let mut validator = Validator::new();

    // Process validation based on output format
    match args.output {
        OutputFormat::Stdout => {
            for t in config.table {
                println!("Parsing: {}", t.name);
                let csv_table = construct_csv_table(&t)
                    .with_context(|| format!("Failed to parse table: '{}'", t.name))?;
                validator.add_table(t.name, csv_table);
            }
        }
        OutputFormat::Json => {
            // JSON output format - placeholder for future implementation
            for t in config.table {
                println!("Parsing: {}", t.name);
                let csv_table = construct_csv_table(&t)
                    .with_context(|| format!("Failed to parse table: '{}'", t.name))?;
                validator.add_table(t.name, csv_table);
            }
        }
    }

    match validator.validate_all() {
        Ok(()) => {
            // Validation succeeded
        }
        Err(RuleError::ValidationError(msg)) => {
            println!("Validation failed: {}", msg);
            // Continue or return Ok(())
        }
        Err(e) => {
            return Err(e).context("Validation failed");
        }
    }

    Ok(())
}

fn main() {
    let args = Args::parse();

    // Enable backtraces in debug mode
    if args.debug {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    if let Err(err) = run(args) {
        if std::env::var("RUST_BACKTRACE").is_ok() {
            eprintln!("Error: {:?}", err);
        } else {
            eprintln!("Error: {:#}", err);
            eprintln!("\nHint: Run with --debug flag for detailed stack traces");
        }
        std::process::exit(1);
    }
}
