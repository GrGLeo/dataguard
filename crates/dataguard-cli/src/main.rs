mod constructor;
mod errors;
mod parser;
use crate::runner::run;
use clap::{Parser, ValueEnum};
mod runner;

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

    /// Enable running validation automatically on file changes
    #[arg(short, long)]
    watch: bool
}



fn main() {
    let args = Args::parse();

    // Enable backtraces in debug mode
    if args.debug {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    match run(args) {
        Ok(all_passed) => {
            if !all_passed {
                std::process::exit(1)
            }
        }
        Err(err) => {
            if std::env::var("RUST_BACKTRACE").is_ok() {
                eprintln!("Error: {:?}", err);
            } else {
                eprintln!("Error: {:#}", err);
                eprintln!("\nHint: Run with --debug flag for detailed stack traces");
            }
            std::process::exit(2);
        }
    }
}
