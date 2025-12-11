use anyhow::{Context, Result};
use dataguard_core::Validator;
use dataguard_reports::StdOutFormatter;

use crate::{constructor::construct_csv_table, errors::ConfigError, parser::parse_config, Args, OutputFormat};

pub fn run(args: Args) -> Result<bool> {
    let mut validator = Validator::new();

    // Process validation based on output format
    match args.output {
        OutputFormat::Stdout => {
            let version = env!("CARGO_PKG_VERSION");
            let formatter = StdOutFormatter::new(version.to_string());
            formatter.print_loading_start();
            let config = parse_config(args.config)?;
            let n_table = config.table.len();
            for (i, t) in config.table.iter().enumerate() {
                formatter.print_loading_progress(i + 1, n_table, &t.name);
                let csv_table = construct_csv_table(t)
                    .with_context(|| format!("Failed to parse table: '{}'", t.name))?;
                validator.add_table(t.name.clone(), csv_table);
            }
            formatter.print_validation_start();
            let res = validator.validate_all()?;

            for r in &res {
                formatter.print_table_result(r);
            }
            let passed = res.iter().filter(|r| r.is_passed()).count();
            let failed = res.len() - passed;
            formatter.print_summary(passed, failed);

            Ok(failed == 0)
        }
        OutputFormat::Json => {
            // JSON output format - placeholder for future implementation
            let config = parse_config(args.config)?;
            for t in config.table {
                println!("Parsing: {}", t.name);
                let csv_table = construct_csv_table(&t)
                    .with_context(|| format!("Failed to parse table: '{}'", t.name))?;
                validator.add_table(t.name, csv_table);
            }
            Ok(true)
        }
    }
}

pub fn watch_run(args: Args) -> Result<bool> {
    let mut validator = Validator::new();

    // Process validation based on output format
    match args.output {
        OutputFormat::Stdout => {
            let version = env!("CARGO_PKG_VERSION");
            let formatter = StdOutFormatter::new(version.to_string());
            formatter.print_loading_start();
            let config = parse_config(args.config)?;
            let n_table = config.table.len();
            if n_table > 1 {
                return Err(ConfigError::TooMuchTable { n_table  });
            }
        },
        OutputFormat::Json => {
        }
    }
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::recommended_watcher(tx)?;
    Ok(true)
}
